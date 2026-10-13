# pipeline/nlp/news_nlp_pipeline.py
#
# News NLP Pipeline
#
# Replaces Reddit NLP as the source for features 25-27.
# Runs FinBERT sentiment and keyword extraction on news
# headlines collected by news_collector.py.
#
# Why news works as a Reddit replacement:
#   - News headlines are denser in financial signal per sentence
#   - No bots, no coordinated hype, no noise
#   - Covers institutional angle Reddit completely misses
#   - Faster: headlines are shorter than posts → faster FinBERT
#   - Available immediately (no API credentials needed)
#   - Already collected by news_collector.py
#
# Features produced (replaces features 25-27):
#   news_sentiment_score       — weighted FinBERT sentiment [-1, 1]
#   news_squeeze_keyword_score — squeeze language in headlines [0, 1]
#   news_catalyst_composite    — combined catalyst + sentiment [0, 1]
#
# Data flow:
#   MongoDB news_articles (raw headlines)
#     → FinBERT sentiment per headline
#     → KeyBERT/keyword scan per headline
#     → Aggregate to ticker-level features
#     → Write NLP results back to MongoDB
#     → Return feature dict for feature assembler
#
# Storage:
#   Reads:  MongoDB news_articles (raw)
#   Writes: MongoDB news_articles (nlp results back)
#           PostgreSQL collection_log

import os
import time
import pymongo
import psycopg2
from datetime import datetime, timedelta
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

from pipeline.nlp.sentiment import SentimentAnalyzer
from pipeline.nlp.keyword_extractor import KeywordExtractor


class NewsNLPPipeline:
    """
    NLP pipeline that operates on news headlines instead of
    Reddit posts. Produces the same 3 NLP features that
    the Reddit pipeline would have produced.

    Usage:
        pipeline = NewsNLPPipeline()

        # Process unprocessed news and get features
        features = pipeline.get_ticker_nlp_features('GME')

        # features contains:
        #   news_sentiment_score
        #   news_squeeze_keyword_score
        #   news_catalyst_composite
    """

    def __init__(self):
        # MongoDB for news articles
        mongo = pymongo.MongoClient(
            os.getenv('MONGO_URL', 'mongodb://localhost:27017'))
        self.db   = mongo[os.getenv('MONGO_DB', 'squeezradar')]
        self.news = self.db['news_articles']

        # PostgreSQL for logging
        self.pg = psycopg2.connect(os.getenv('POSTGRES_URL'))

        # NLP components (reused from existing pipeline)
        self.sentiment = SentimentAnalyzer()
        self.keywords  = KeywordExtractor()

    # ═════════════════════════════════════════════════════════
    # MAIN METHOD — called by feature assembler
    # ═════════════════════════════════════════════════════════

    def get_ticker_nlp_features(self,
                                 ticker: str,
                                 hours: int = 48) -> dict:
        """
        Returns NLP features for a ticker from recent news.
        This is the primary method called by feature_assembler.py.

        Args:
          ticker: stock symbol
          hours:  lookback window (default 48h — news is slower than Reddit)

        Returns dict with:
          news_sentiment_score       — FinBERT weighted sentiment [-1, 1]
          news_squeeze_keyword_score — squeeze language score [0, 1]
          news_catalyst_composite    — catalyst + sentiment combined [0, 1]
          news_article_count         — how many articles found
          news_nlp_available         — bool: was NLP run?
          top_headlines              — list of top 3 headlines
        """
        ticker  = ticker.upper().strip()
        start   = time.time()
        cutoff  = datetime.utcnow() - timedelta(hours=hours)

        # Step 1: Get unprocessed news articles
        unprocessed = self._get_unprocessed(ticker, cutoff)
        if unprocessed:
            self._process_articles(unprocessed)

        # Step 2: Get all processed articles in window
        articles = self._get_processed(ticker, cutoff)

        if not articles:
            return self._empty_features()

        # Step 3: Aggregate to ticker-level features
        features = self._aggregate(ticker, articles)

        duration = int((time.time()-start)*1000)
        self._log(ticker, 'success',
                  len(articles), None, duration)

        print(
            f'  ✓ {ticker} news NLP — '
            f'{len(articles)} articles | '
            f'sentiment: {features["news_sentiment_score"]:+.3f} | '
            f'urgency: {features["news_squeeze_keyword_score"]:.3f} | '
            f'catalyst: {features["news_catalyst_composite"]:.3f}'
        )

        return features

    def process_backlog(self,
                         batch_size: int = 30) -> dict:
        """
        Processes a batch of unprocessed news articles.
        Called by Celery worker every 15 minutes.

        Returns:
          processed    — number processed this run
          remaining    — estimated unprocessed remaining
        """
        start = time.time()

        unprocessed = list(
            self.news
            .find({'nlp_processed': {'$ne': True}})
            .sort('published_at', pymongo.DESCENDING)
            .limit(batch_size)
        )

        if not unprocessed:
            return {'processed': 0, 'remaining': 0}

        self._process_articles(unprocessed)

        remaining = self.news.count_documents(
            {'nlp_processed': {'$ne': True}})

        duration = int((time.time()-start)*1000)
        print(
            f'  ✓ News NLP backlog — '
            f'{len(unprocessed)} processed | '
            f'{remaining} remaining | '
            f'{duration}ms'
        )

        return {
            'processed':  len(unprocessed),
            'remaining':  remaining,
            'duration_ms': duration,
        }

    # ═════════════════════════════════════════════════════════
    # NLP PROCESSING
    # ═════════════════════════════════════════════════════════

    def _process_articles(self, articles: list):
        """
        Runs FinBERT + keyword extraction on a batch of articles.
        Writes results back to MongoDB.

        For news headlines:
          - Use headline as primary text (most signal-dense)
          - Add first 200 chars of body if available
          - FinBERT handles short texts well
        """
        # Build post-like dicts that sentiment analyzer expects
        # (reuses existing SentimentAnalyzer without modification)
        post_like = [
            {
                '_id':   str(article.get('_id')),
                'title': article.get('headline', ''),
                'body':  article.get('body', '')[:200],
                'score': 1,  # All news equally weighted
            }
            for article in articles
        ]

        # Run FinBERT sentiment
        analyzed = self.sentiment.analyze_posts(
            post_like, use_finbert=True)

        # Build sentiment map
        sentiment_map = {
            p['_id']: {
                'sentiment_label': p.get('sentiment_label'),
                'sentiment_score': p.get('sentiment_score', 0.0),
            }
            for p in analyzed
        }

        # Run keyword extraction per article
        for article in articles:
            art_id = str(article.get('_id'))
            text   = (article.get('headline', '') + ' '
                      + article.get('body', '')[:300])

            kw_result = self.keywords.extract_from_text(text)
            sent_data = sentiment_map.get(art_id, {})

            # Write back to MongoDB
            self.news.update_one(
                {'_id': article['_id']},
                {'$set': {
                    'sentiment_label':        sent_data.get(
                        'sentiment_label', 'neutral'),
                    'sentiment_score':        sent_data.get(
                        'sentiment_score', 0.0),
                    'keywords':               kw_result.get(
                        'keywords', []),
                    'keyword_urgency_score':  kw_result.get(
                        'keyword_urgency_score', 0.0),
                    'has_squeeze_language':   kw_result.get(
                        'has_squeeze_language', False),
                    'nlp_processed':          True,
                    'nlp_processed_at':       datetime.utcnow(),
                }}
            )

    # ═════════════════════════════════════════════════════════
    # AGGREGATION — article-level → ticker-level features
    # ═════════════════════════════════════════════════════════

    def _aggregate(self, ticker: str,
                    articles: list) -> dict:
        """
        Aggregates article-level NLP results into
        three ticker-level features for the ML vector.

        Feature 25 — news_sentiment_score:
          Weighted average of FinBERT sentiment scores.
          Recent articles weighted 2x older articles.
          Range: -1.0 to +1.0

        Feature 26 — news_squeeze_keyword_score:
          Average keyword urgency score across all articles.
          High = multiple articles discussing squeeze mechanics.
          Range: 0.0 to 1.0

        Feature 27 — news_catalyst_composite:
          Combined signal from:
            - catalyst_flags present in articles
            - Sentiment direction (bullish vs bearish)
            - Squeeze keyword presence
          Captures overall news "squeeze favorability".
          Range: 0.0 to 1.0
        """
        if not articles:
            return self._empty_features()

        now = datetime.utcnow()

        # ── Feature 25: news_sentiment_score ─────────────
        sentiment_scores = []
        weights          = []

        for article in articles:
            score = article.get('sentiment_score', 0.0) or 0.0

            # Recency weighting
            pub = article.get('published_at')
            if pub:
                age_hours = (now - pub.replace(
                    tzinfo=None)).total_seconds() / 3600
                # Recent articles weighted higher
                weight = max(2.0 - (age_hours / 48), 0.1)
            else:
                weight = 1.0

            sentiment_scores.append(score)
            weights.append(weight)

        total_weight = sum(weights) or 1.0
        news_sentiment = sum(
            s * w for s, w in zip(sentiment_scores, weights)
        ) / total_weight

        # ── Feature 26: news_squeeze_keyword_score ───────
        urgency_scores = [
            float(a.get('keyword_urgency_score', 0.0) or 0.0)
            for a in articles
        ]
        news_squeeze_keyword = (
            sum(urgency_scores) / len(urgency_scores)
            if urgency_scores else 0.0
        )

        # Count articles with squeeze language
        squeeze_article_count = sum(
            1 for a in articles
            if a.get('has_squeeze_language', False)
        )
        squeeze_pct = squeeze_article_count / len(articles)

        # Blend avg urgency and squeeze article percentage
        news_squeeze_keyword = (
            news_squeeze_keyword * 0.6 + squeeze_pct * 0.4)

        # ── Feature 27: news_catalyst_composite ──────────
        # Counts articles with catalyst flags
        catalyst_count = sum(
            1 for a in articles
            if a.get('has_catalyst', False)
               or len(a.get('catalyst_flags', [])) > 0
        )
        catalyst_pct = catalyst_count / len(articles)

        # Bullish catalyst bonus
        bullish_count = sum(
            1 for a in articles
            if (a.get('sentiment_label') == 'positive'
                and a.get('has_catalyst', False))
        )
        bullish_catalyst_pct = bullish_count / len(articles)

        # Composite: catalyst presence × sentiment direction
        # 0.0 = no catalysts, bearish
        # 1.0 = all articles are bullish catalysts
        raw_composite = (
            catalyst_pct * 0.4
            + bullish_catalyst_pct * 0.4
            + max(news_sentiment, 0) * 0.2
        )
        news_catalyst_composite = min(raw_composite, 1.0)

        # Top headlines for dashboard display
        top_headlines = [
            {
                'headline':  a.get('headline', '')[:100],
                'sentiment': a.get('sentiment_label', 'neutral'),
                'score':     a.get('sentiment_score', 0.0),
                'source':    a.get('source', ''),
                'has_squeeze': a.get('has_squeeze_language', False),
            }
            for a in sorted(
                articles,
                key=lambda x: abs(x.get('sentiment_score', 0.0)),
                reverse=True
            )[:3]
        ]

        return {
            # The three ML features
            'news_sentiment_score':       round(
                float(news_sentiment), 4),
            'news_squeeze_keyword_score': round(
                float(news_squeeze_keyword), 4),
            'news_catalyst_composite':    round(
                float(news_catalyst_composite), 4),

            # Supporting metadata for dashboard
            'news_article_count':         len(articles),
            'squeeze_article_count':      squeeze_article_count,
            'catalyst_count':             catalyst_count,
            'bullish_article_count':      sum(
                1 for a in articles
                if a.get('sentiment_label') == 'positive'),
            'bearish_article_count':      sum(
                1 for a in articles
                if a.get('sentiment_label') == 'negative'),
            'top_headlines':              top_headlines,
            'news_nlp_available':         True,
            'lookback_hours':             48,
            'computed_at':                datetime.utcnow(),
        }

    # ═════════════════════════════════════════════════════════
    # DATABASE READS
    # ═════════════════════════════════════════════════════════

    def _get_unprocessed(self, ticker: str,
                          cutoff: datetime) -> list:
        """Gets articles not yet through NLP pipeline."""
        return list(
            self.news.find({
                'ticker':      ticker,
                'published_at': {'$gte': cutoff},
                'nlp_processed': {'$ne': True},
            }).limit(50)
        )

    def _get_processed(self, ticker: str,
                        cutoff: datetime) -> list:
        """Gets articles that have been NLP-processed."""
        return list(
            self.news.find({
                'ticker':       ticker,
                'published_at': {'$gte': cutoff},
                'nlp_processed': True,
            }).sort('published_at', pymongo.DESCENDING)
            .limit(20)
        )

    # ═════════════════════════════════════════════════════════
    # HELPERS
    # ═════════════════════════════════════════════════════════

    @staticmethod
    def _empty_features() -> dict:
        """Returns zeroed features when no news available."""
        return {
            'news_sentiment_score':       0.0,
            'news_squeeze_keyword_score': 0.0,
            'news_catalyst_composite':    0.0,
            'news_article_count':         0,
            'squeeze_article_count':      0,
            'catalyst_count':             0,
            'bullish_article_count':      0,
            'bearish_article_count':      0,
            'top_headlines':              [],
            'news_nlp_available':         False,
        }

    def get_summary_text(self, ticker: str) -> str:
        """
        Returns human-readable news NLP summary.
        Used by SHAP explainer and dashboard.
        """
        features = self.get_ticker_nlp_features(ticker)

        if not features['news_nlp_available']:
            return f'No recent news found for {ticker}.'

        count     = features['news_article_count']
        sentiment = features['news_sentiment_score']
        urgency   = features['news_squeeze_keyword_score']
        catalysts = features['catalyst_count']

        sent_label = (
            'bullish' if sentiment > 0.2
            else 'bearish' if sentiment < -0.2
            else 'neutral'
        )

        parts = [f'{count} articles in last 48h']
        parts.append(f'news tone: {sent_label} '
                      f'({sentiment:+.2f})')

        if urgency > 0.3:
            parts.append(
                f'squeeze language detected '
                f'(urgency: {urgency:.2f})')

        if catalysts > 0:
            parts.append(f'{catalysts} catalyst events')

        return f'{ticker} news: ' + ', '.join(parts) + '.'

    def _log(self, ticker: str, status: str,
             rows: int, error: str, duration_ms: int):
        cur = self.pg.cursor()
        cur.execute("""
            INSERT INTO collection_log
                (ticker, source, status, rows_collected,
                 error_message, duration_ms)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (ticker, 'news_nlp', status,
              rows, error, duration_ms))
        self.pg.commit()

    def close(self):
        self.pg.close()


# ─────────────────────────────────────────────────────────────
if __name__ == '__main__':
    import json

    pipeline = NewsNLPPipeline()

    print('='*55)
    print('  News NLP Pipeline — Manual Test')
    print('='*55)

    test_tickers = ['GME', 'AAPL', 'TSLA']

    for ticker in test_tickers:
        print(f'\n── {ticker} ──')
        features = pipeline.get_ticker_nlp_features(
            ticker, hours=48)

        print(f'  news_sentiment_score:        '
              f'{features["news_sentiment_score"]:+.4f}')
        print(f'  news_squeeze_keyword_score:  '
              f'{features["news_squeeze_keyword_score"]:.4f}')
        print(f'  news_catalyst_composite:     '
              f'{features["news_catalyst_composite"]:.4f}')
        print(f'  news_article_count:          '
              f'{features["news_article_count"]}')
        print(f'  squeeze_articles:            '
              f'{features["squeeze_article_count"]}')
        print(f'  catalyst_count:              '
              f'{features["catalyst_count"]}')
        print(f'  bullish / bearish:           '
              f'{features["bullish_article_count"]} / '
              f'{features["bearish_article_count"]}')

        if features['top_headlines']:
            print(f'  top headlines:')
            for h in features['top_headlines']:
                print(f'    [{h["sentiment"]:8s} '
                      f'{h["score"]:+.2f}] '
                      f'{h["headline"][:60]}')

        print(f'\n  Summary: '
              f'{pipeline.get_summary_text(ticker)}')

    # Test backlog processing
    print('\n── Backlog processing ──')
    result = pipeline.process_backlog(batch_size=10)
    print(f'  processed: {result["processed"]}')
    print(f'  remaining: {result["remaining"]}')

    pipeline.close()
    print('\n✓ News NLP pipeline test complete')