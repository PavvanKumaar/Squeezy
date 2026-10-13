# pipeline/collectors/news_collector.py
#
# Features collected:
#   news_headlines        — list of recent headlines + metadata
#   catalyst_flags        — which catalyst types detected in headline
#   has_catalyst          — bool (any catalyst found)
#   news_catalyst_flag    — int 0/1 for feature vector
#
# Source:
#   Primary:   yfinance ticker.news (free, ~30min delay)
#   Secondary: Google News RSS (free, no API key)
#
# Storage:
#   MongoDB  news_articles — raw articles + NLP results written back
#   PostgreSQL collection_log
#
# NLP field lifecycle:
#   1. news_collector saves article with nlp_processed=False
#   2. news_nlp_pipeline.py picks up nlp_processed=False docs
#   3. Runs FinBERT + keywords, calls save_nlp_results()
#   4. save_nlp_results() sets nlp_processed=True + writes fields
#   5. feature_assembler reads processed docs for features 25-27
#
# CHANGES FROM ORIGINAL:
#   + nlp_processed=False on every new article (ensures pipeline picks it up)
#   + has_catalyst field computed at insert time from catalyst_flags
#   + save_nlp_results() method — NEW (called by news_nlp_pipeline)
#   + update_catalyst_flag() helper — updates PostgreSQL stock_events

import os
import re
import time
import hashlib
import requests
import pymongo
import psycopg2
import xml.etree.ElementTree as ET
import yfinance as yf
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────
# CATALYST DETECTION VOCABULARY
# ─────────────────────────────────────────────────────────────
BULLISH_CATALYSTS = [
    'short squeeze', 'gamma squeeze', 'short interest',
    'buyback', 'share repurchase', 'partnership',
    'fda approval', 'earnings beat', 'revenue beat',
    'activist investor', 'takeover', 'acquisition',
    'merger', 'raised guidance', 'upgraded', 'upgrade',
    'insider buying', 'raised price target',
    'beat expectations', 'strong earnings',
]

BEARISH_CATALYSTS = [
    'sec investigation', 'fraud', 'bankruptcy',
    'dilution', 'offering', 'secondary offering',
    'lawsuit', 'downgraded', 'downgrade',
    'missed earnings', 'guidance cut', 'lowered guidance',
    'delisted', 'short seller report', 'resignation',
    'accounting irregularity', 'restatement',
    'class action', 'failed trial', 'recall',
]


class NewsCollector:
    """
    Collects news headlines for a ticker from multiple free sources.
    Saves raw articles to MongoDB with NLP fields pre-initialised
    to False/None so the NLP pipeline can process them.

    The save_nlp_results() method is called by news_nlp_pipeline.py
    after NLP processing is complete.
    """

    def __init__(self):
        # MongoDB — raw articles + NLP results
        mongo = pymongo.MongoClient(
            os.getenv('MONGO_URL', 'mongodb://localhost:27017'))
        db = mongo[os.getenv('MONGO_DB', 'squeezradar')]
        self.collection = db['news_articles']
        self._ensure_mongo_indexes()

        # PostgreSQL — logging + catalyst events
        self.pg = psycopg2.connect(os.getenv('POSTGRES_URL'))

        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
        })

    # ═════════════════════════════════════════════════════════
    # PUBLIC — COLLECTION
    # ═════════════════════════════════════════════════════════

    def collect(self, ticker: str) -> dict:
        """
        Main entry point. Fetches headlines from all sources,
        saves to MongoDB, returns summary dict.

        Returns dict with:
          total_found     — articles found across all sources
          saved           — new articles saved to MongoDB
          has_catalyst    — bool: any catalyst in this batch
          catalyst_flags  — flat list of all detected catalyst strings
          articles        — list of article dicts
        """
        start  = time.time()
        ticker = ticker.upper().strip()

        all_articles = []

        # Source 1 — yfinance (fastest, most reliable)
        yf_articles = self._fetch_yfinance(ticker)
        all_articles.extend(yf_articles)

        # Source 2 — Google News RSS (more coverage)
        rss_articles = self._fetch_google_rss(ticker)
        all_articles.extend(rss_articles)

        # Deduplicate by URL hash
        seen    = set()
        unique  = []
        for a in all_articles:
            h = a.get('url_hash')
            if h and h not in seen:
                seen.add(h)
                unique.append(a)

        # Detect catalysts and set has_catalyst
        for article in unique:
            flags = self._detect_catalyst(
                article.get('headline', ''))
            article['catalyst_flags'] = flags
            article['has_catalyst']   = len(flags) > 0

        # Save to MongoDB
        saved = self._save(unique)

        # Write catalyst event to PostgreSQL if any found
        any_catalyst = any(a['has_catalyst'] for a in unique)
        if any_catalyst:
            self._write_catalyst_event(ticker, unique)

        duration = int((time.time()-start)*1000)
        self._log(ticker, 'success', saved, None, duration)

        result = {
            'ticker':       ticker,
            'total_found':  len(unique),
            'saved':        saved,
            'has_catalyst': any_catalyst,
            'catalyst_flags': list(set(
                flag
                for a in unique
                for flag in a.get('catalyst_flags', [])
            )),
            'articles':     unique,
            'collected_at': datetime.utcnow(),
            'error':        False,
        }

        print(
            f'  ✓ {ticker} news — '
            f'{len(unique)} articles | '
            f'{saved} new | '
            f'catalyst: {any_catalyst}'
        )
        return result

    # ═════════════════════════════════════════════════════════
    # PUBLIC — NLP WRITEBACK (called by news_nlp_pipeline.py)
    # ═════════════════════════════════════════════════════════

    def save_nlp_results(self, article_id,
                          nlp_data: dict) -> bool:
        """
        Writes NLP results back to a MongoDB article document.
        Called by news_nlp_pipeline.py after processing.

        Args:
          article_id: MongoDB _id of the article
          nlp_data:   dict with keys:
                        sentiment_label       — 'positive'/'negative'/'neutral'
                        sentiment_score       — float -1 to 1
                        keywords              — list of strings
                        keyword_urgency_score — float 0-1
                        has_squeeze_language  — bool

        Returns True on success, False on failure.
        """
        try:
            update = {
                'sentiment_label':       nlp_data.get(
                    'sentiment_label', 'neutral'),
                'sentiment_score':       nlp_data.get(
                    'sentiment_score', 0.0),
                'keywords':              nlp_data.get(
                    'keywords', []),
                'keyword_urgency_score': nlp_data.get(
                    'keyword_urgency_score', 0.0),
                'has_squeeze_language':  nlp_data.get(
                    'has_squeeze_language', False),
                'nlp_processed':         True,
                'nlp_processed_at':      datetime.utcnow(),
            }
            self.collection.update_one(
                {'_id': article_id},
                {'$set': update}
            )
            return True
        except Exception as e:
            print(f'  [news] save_nlp_results error: {e}')
            return False

    def save_nlp_results_batch(self,
                                results: list) -> int:
        """
        Bulk NLP writeback for a list of (article_id, nlp_data) tuples.
        More efficient than calling save_nlp_results() in a loop.

        Returns count of successfully updated documents.
        """
        updated = 0
        for article_id, nlp_data in results:
            if self.save_nlp_results(article_id, nlp_data):
                updated += 1
        return updated

    def get_unprocessed(self, ticker: str,
                         limit: int = 50,
                         hours: int = 48) -> list:
        """
        Returns articles not yet NLP-processed for a ticker.
        Called by news_nlp_pipeline.py to get work queue.
        """
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        return list(
            self.collection.find({
                'ticker':        ticker,
                'published_at':  {'$gte': cutoff},
                'nlp_processed': False,
            })
            .sort('published_at', pymongo.DESCENDING)
            .limit(limit)
        )

    def get_processed(self, ticker: str,
                       hours: int = 48,
                       limit: int = 20) -> list:
        """
        Returns NLP-processed articles for a ticker.
        Called by feature_assembler for features 25-27.
        """
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        return list(
            self.collection.find({
                'ticker':        ticker,
                'published_at':  {'$gte': cutoff},
                'nlp_processed': True,
            })
            .sort('published_at', pymongo.DESCENDING)
            .limit(limit)
        )

    def get_recent_for_ticker(self, ticker: str,
                               hours: int = 48) -> list:
        """
        Returns all recent articles for a ticker (processed or not).
        Used by dashboard news feed.
        """
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        return list(
            self.collection.find({
                'ticker':       ticker,
                'published_at': {'$gte': cutoff},
            })
            .sort('published_at', pymongo.DESCENDING)
            .limit(30)
        )

    # ═════════════════════════════════════════════════════════
    # FETCH METHODS
    # ═════════════════════════════════════════════════════════

    def _fetch_yfinance(self, ticker: str) -> list:
        """
        Fetches headlines from yfinance ticker.news.
        Free, no API key, ~30min delay.
        """
        try:
            session = requests.Session()
            session.headers.update({
                'User-Agent': (
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/120.0.0.0 Safari/537.36'
                )
            })
            stock    = yf.Ticker(ticker, session=session)
            raw_news = stock.news or []
            articles = []

            for item in raw_news[:15]:
                headline = item.get('title', '')
                url      = item.get('link', '')
                if not headline:
                    continue

                articles.append(
                    self._make_doc(
                        ticker=ticker,
                        headline=headline,
                        url=url,
                        publisher=item.get('publisher', ''),
                        published_at=datetime.fromtimestamp(
                            item.get('providerPublishTime', 0),
                            tz=timezone.utc
                        ),
                        source='yfinance',
                    )
                )
            return articles

        except Exception as e:
            print(f'  [news] yfinance fetch error: {e}')
            return []

    def _fetch_google_rss(self, ticker: str) -> list:
        """
        Fetches headlines from Google News RSS feed.
        Free, no API key, reasonably current.
        """
        try:
            url = (
                f'https://news.google.com/rss/search'
                f'?q={ticker}+stock&hl=en-US&gl=US&ceid=US:en'
            )
            resp = self.session.get(url, timeout=10)
            if resp.status_code != 200:
                return []

            root  = ET.fromstring(resp.content)
            items = root.findall('.//item')
            articles = []

            for item in items[:10]:
                title_el  = item.find('title')
                link_el   = item.find('link')
                pub_el    = item.find('pubDate')

                headline = title_el.text if title_el is not None else ''
                url      = link_el.text  if link_el is not None  else ''
                if not headline:
                    continue

                # Parse pubDate if present
                published_at = datetime.utcnow().replace(
                    tzinfo=timezone.utc)
                if pub_el is not None and pub_el.text:
                    try:
                        from email.utils import parsedate_to_datetime
                        published_at = parsedate_to_datetime(
                            pub_el.text)
                    except Exception:
                        pass

                articles.append(
                    self._make_doc(
                        ticker=ticker,
                        headline=headline,
                        url=url,
                        publisher='google_news',
                        published_at=published_at,
                        source='google_rss',
                    )
                )
            return articles

        except Exception as e:
            print(f'  [news] Google RSS error: {e}')
            return []

    # ═════════════════════════════════════════════════════════
    # DOCUMENT CONSTRUCTION
    # ═════════════════════════════════════════════════════════

    def _make_doc(self, ticker: str, headline: str,
                   url: str, publisher: str,
                   published_at: datetime,
                   source: str) -> dict:
        """
        Builds a MongoDB document for one news article.
        All NLP fields pre-set to False/None/empty so the
        NLP pipeline can query nlp_processed=False reliably.
        """
        return {
            'ticker':       ticker,
            'source':       source,
            'headline':     headline,
            'url':          url,
            'url_hash':     hashlib.md5(url.encode()).hexdigest(),
            'publisher':    publisher,
            'published_at': published_at,
            'collected_at': datetime.utcnow(),

            # Catalyst fields — set at collection time
            'catalyst_flags': [],    # populated in collect()
            'has_catalyst':   False, # populated in collect()

            # NLP fields — all False/None until pipeline runs
            # news_nlp_pipeline.py queries nlp_processed=False
            # and writes results back via save_nlp_results()
            'nlp_processed':         False,
            'nlp_processed_at':      None,
            'sentiment_label':       None,
            'sentiment_score':       None,
            'keywords':              [],
            'keyword_urgency_score': None,
            'has_squeeze_language':  False,
        }

    # ═════════════════════════════════════════════════════════
    # CATALYST DETECTION
    # ═════════════════════════════════════════════════════════

    def _detect_catalyst(self, headline: str) -> list:
        """
        Scans headline for squeeze-relevant catalyst keywords.
        Returns list of matched catalyst strings.
        """
        lower  = headline.lower()
        flags  = []
        for kw in BULLISH_CATALYSTS:
            if kw in lower:
                flags.append(f'bullish:{kw.replace(" ", "_")}')
        for kw in BEARISH_CATALYSTS:
            if kw in lower:
                flags.append(f'bearish:{kw.replace(" ", "_")}')
        return flags

    # ═════════════════════════════════════════════════════════
    # DATABASE WRITES
    # ═════════════════════════════════════════════════════════

    def _save(self, articles: list) -> int:
        """Bulk inserts articles into MongoDB, skipping duplicates."""
        if not articles:
            return 0
        try:
            result = self.collection.insert_many(
                articles, ordered=False)
            return len(result.inserted_ids)
        except pymongo.errors.BulkWriteError as e:
            return e.details.get('nInserted', 0)
        except Exception as e:
            print(f'  [news] MongoDB save error: {e}')
            return 0

    def _write_catalyst_event(self, ticker: str,
                               articles: list):
        """
        Writes a catalyst event to PostgreSQL stock_events
        when a catalyst is detected in news headlines.
        The feature assembler checks this table for
        the news_catalyst_flag feature (index 23).
        """
        import json
        cur = self.pg.cursor()
        try:
            catalyst_headlines = [
                {
                    'headline': a['headline'][:200],
                    'flags':    a['catalyst_flags'],
                    'source':   a['source'],
                }
                for a in articles
                if a.get('has_catalyst')
            ]
            if not catalyst_headlines:
                return

            cur.execute("""
                INSERT INTO stock_events
                    (ticker, event_type, event_data, occurred_at)
                VALUES (%s, %s, %s, NOW())
            """, (
                ticker,
                'catalyst',
                json.dumps({
                    'article_count': len(catalyst_headlines),
                    'headlines':     catalyst_headlines[:5],
                }),
            ))
            self.pg.commit()
        except Exception as e:
            self.pg.rollback()
            print(f'  [news] catalyst event write error: {e}')

    def _log(self, ticker: str, status: str,
             rows: int, error: str, duration_ms: int):
        cur = self.pg.cursor()
        cur.execute("""
            INSERT INTO collection_log
                (ticker, source, status, rows_collected,
                 error_message, duration_ms)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (ticker, 'news', status, rows, error, duration_ms))
        self.pg.commit()

    def _ensure_mongo_indexes(self):
        """Creates indexes if they don't exist."""
        try:
            self.collection.create_index(
                [('ticker', 1), ('published_at', -1)])
            self.collection.create_index(
                [('url_hash', 1)], unique=True,
                sparse=True)
            self.collection.create_index(
                [('nlp_processed', 1)])
            self.collection.create_index(
                [('ticker', 1), ('nlp_processed', 1),
                 ('published_at', -1)])
            # TTL: auto-expire articles after 30 days
            self.collection.create_index(
                [('collected_at', 1)],
                expireAfterSeconds=2592000)
        except Exception:
            pass

    def close(self):
        self.pg.close()


# ─────────────────────────────────────────────────────────────
if __name__ == '__main__':
    collector = NewsCollector()

    print('='*55)
    print('  News Collector — Updated Test')
    print('='*55)

    # Test 1: collect articles
    print('\n[1] Collecting news for GME...')
    result = collector.collect('GME')
    print(f'  total_found:   {result["total_found"]}')
    print(f'  saved:         {result["saved"]}')
    print(f'  has_catalyst:  {result["has_catalyst"]}')
    print(f'  catalyst_flags:{result["catalyst_flags"][:3]}')

    if result['articles']:
        a = result['articles'][0]
        print(f'\n  Sample article:')
        print(f'    headline:       {a["headline"][:70]}')
        print(f'    source:         {a["source"]}')
        print(f'    has_catalyst:   {a["has_catalyst"]}')
        print(f'    nlp_processed:  {a["nlp_processed"]}')
        print(f'    sentiment_label:{a["sentiment_label"]}')

    # Test 2: get_unprocessed
    print('\n[2] Unprocessed articles (GME):')
    unprocessed = collector.get_unprocessed('GME', limit=5)
    print(f'  count: {len(unprocessed)}')

    # Test 3: simulate NLP writeback
    print('\n[3] Simulating NLP writeback...')
    if unprocessed:
        article_id = unprocessed[0]['_id']
        success = collector.save_nlp_results(
            article_id,
            {
                'sentiment_label':       'positive',
                'sentiment_score':       0.82,
                'keywords':              ['short squeeze', 'gamma'],
                'keyword_urgency_score': 0.74,
                'has_squeeze_language':  True,
            }
        )
        print(f'  save_nlp_results: {success}')

        # Verify writeback
        updated = collector.collection.find_one(
            {'_id': article_id})
        if updated:
            print(f'  nlp_processed:    {updated["nlp_processed"]}')
            print(f'  sentiment_label:  {updated["sentiment_label"]}')
            print(f'  sentiment_score:  {updated["sentiment_score"]}')
            print(f'  has_squeeze:      {updated["has_squeeze_language"]}')

    # Test 4: get_processed
    print('\n[4] Processed articles (GME):')
    processed = collector.get_processed('GME', limit=5)
    print(f'  count: {len(processed)}')

    collector.close()
    print('\n✓ News collector updated test complete')