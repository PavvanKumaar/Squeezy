# pipeline/nlp/sentiment.py
#
# Sentiment analysis for Reddit posts using FinBERT.
#
# Why FinBERT over VADER?
#   VADER: "This stock is going to the moon 🚀🚀🚀" → neutral
#   FinBERT: same text → strongly positive
#   FinBERT is trained on financial text and understands
#   domain-specific language that general NLP models miss.
#
# Model: ProsusAI/finbert (HuggingFace)
# Input: list of post dicts from MongoDB
# Output: same list with sentiment_label + sentiment_score added
#
# Processing:
#   - Runs FinBERT in batches of 16 (GPU memory efficient)
#   - Falls back to VADER if FinBERT model unavailable
#   - Aggregates post-level sentiment → ticker-level score
#   - Weights by upvote score (high-voted posts matter more)

import os
import time
import numpy as np
from datetime import datetime
from typing import Optional

# Lazy imports — only load heavy models when needed
_finbert_pipeline = None
_vader_analyzer   = None


def _get_finbert():
    """Lazy-loads FinBERT model on first call."""
    global _finbert_pipeline
    if _finbert_pipeline is None:
        try:
            from transformers import pipeline
            import torch
            device = 0 if torch.cuda.is_available() else -1
            print(f'  [NLP] Loading FinBERT '
                  f'(device: {"GPU" if device==0 else "CPU"})...')
            _finbert_pipeline = pipeline(
                'sentiment-analysis',
                model='ProsusAI/finbert',
                device=device,
                truncation=True,
                max_length=512,
            )
            print('  [NLP] FinBERT loaded ✓')
        except Exception as e:
            print(f'  [NLP] FinBERT load failed: {e}')
            print('  [NLP] Falling back to VADER')
    return _finbert_pipeline


def _get_vader():
    """Lazy-loads VADER on first call."""
    global _vader_analyzer
    if _vader_analyzer is None:
        from vaderSentiment.vaderSentiment import (
            SentimentIntensityAnalyzer)
        _vader_analyzer = SentimentIntensityAnalyzer()
    return _vader_analyzer


class SentimentAnalyzer:
    """
    Analyzes sentiment of Reddit posts using FinBERT.

    Usage:
      analyzer = SentimentAnalyzer()
      posts    = reddit_collector.get_recent_posts('GME')
      posts    = analyzer.analyze_posts(posts)
      score    = analyzer.aggregate_sentiment(posts)
    """

    LABEL_MAP = {
        # FinBERT outputs these labels
        'positive': 1.0,
        'negative': -1.0,
        'neutral':  0.0,
    }

    def analyze_posts(self, posts: list,
                       use_finbert: bool = True) -> list:
        """
        Runs sentiment analysis on a list of post dicts.
        Adds sentiment_label and sentiment_score to each post.

        Args:
          posts:       list of post dicts (from MongoDB)
          use_finbert: True = FinBERT, False = VADER (faster)

        Returns: same list with sentiment fields populated
        """
        if not posts:
            return posts

        # Build texts for analysis
        # Combine title + first 300 chars of body
        texts = [
            self._build_text(p) for p in posts
        ]

        if use_finbert:
            labeled = self._analyze_finbert(texts)
        else:
            labeled = self._analyze_vader(texts)

        # Write results back to posts
        for post, result in zip(posts, labeled):
            post['sentiment_label'] = result['label']
            post['sentiment_score'] = result['score']

        return posts

    def aggregate_sentiment(self, posts: list) -> dict:
        """
        Aggregates post-level sentiment into ticker-level metrics.
        Weights each post by its upvote score.
        Posts with more upvotes represent stronger community consensus.

        Returns dict with:
          sentiment_score    — weighted average [-1, 1]
          bullish_pct        — % of posts bullish
          bearish_pct        — % of posts bearish
          neutral_pct        — % of posts neutral
          post_count         — total posts analyzed
          high_conviction    — bool: strong consensus (>70% one way)
        """
        if not posts:
            return {
                'sentiment_score':  0.0,
                'bullish_pct':      0.0,
                'bearish_pct':      0.0,
                'neutral_pct':      0.0,
                'post_count':       0,
                'high_conviction':  False,
            }

        # Only include posts that have been sentiment-analyzed
        analyzed = [p for p in posts
                    if p.get('sentiment_score') is not None]
        if not analyzed:
            return {
                'sentiment_score': 0.0,
                'post_count':      len(posts),
                'analyzed_count':  0,
            }

        # Weighted average by upvote score
        # Minimum weight of 1 (avoid zero division)
        total_weight = sum(
            max(p.get('score', 1), 1) for p in analyzed)
        weighted_sentiment = sum(
            p['sentiment_score'] * max(p.get('score', 1), 1)
            for p in analyzed
        ) / total_weight

        # Count by label
        bullish = sum(1 for p in analyzed
                      if p.get('sentiment_label') == 'positive')
        bearish = sum(1 for p in analyzed
                      if p.get('sentiment_label') == 'negative')
        neutral = len(analyzed) - bullish - bearish

        bullish_pct = bullish / len(analyzed)
        bearish_pct = bearish / len(analyzed)
        neutral_pct = neutral / len(analyzed)

        return {
            'sentiment_score':  round(weighted_sentiment, 4),
            'bullish_pct':      round(bullish_pct, 4),
            'bearish_pct':      round(bearish_pct, 4),
            'neutral_pct':      round(neutral_pct, 4),
            'post_count':       len(analyzed),
            'high_conviction':  (bullish_pct > 0.70
                                  or bearish_pct > 0.70),
            'computed_at':      datetime.utcnow(),
        }

    # ═════════════════════════════════════════════════════════
    # ANALYSIS BACKENDS
    # ═════════════════════════════════════════════════════════

    def _analyze_finbert(self, texts: list) -> list:
        """
        Runs FinBERT on a list of texts in batches of 16.
        Falls back to VADER if model unavailable.
        """
        model = _get_finbert()
        if model is None:
            return self._analyze_vader(texts)

        results = []
        batch_size = 16

        try:
            for i in range(0, len(texts), batch_size):
                batch = texts[i:i+batch_size]
                # Truncate long texts to prevent OOM
                batch = [t[:512] for t in batch]

                outputs = model(batch,
                                truncation=True,
                                max_length=512)

                for output in outputs:
                    label = output['label'].lower()
                    raw_score = output['score']
                    # Convert to signed score
                    signed = raw_score * self.LABEL_MAP.get(
                        label, 0.0)
                    results.append({
                        'label': label,
                        'score': round(signed, 4),
                    })

        except Exception as e:
            print(f'  [NLP] FinBERT batch error: {e}')
            print('  [NLP] Falling back to VADER for this batch')
            return self._analyze_vader(texts)

        return results

    def _analyze_vader(self, texts: list) -> list:
        """
        VADER sentiment — fast, no GPU needed.
        Less accurate than FinBERT for financial text
        but good fallback / useful for speed comparisons.
        """
        vader  = _get_vader()
        results = []

        for text in texts:
            scores = vader.polarity_scores(text)
            compound = scores['compound']

            if compound >= 0.05:
                label = 'positive'
            elif compound <= -0.05:
                label = 'negative'
            else:
                label = 'neutral'

            results.append({
                'label': label,
                'score': round(compound, 4),
            })

        return results

    # ═════════════════════════════════════════════════════════
    # HELPERS
    # ═════════════════════════════════════════════════════════

    @staticmethod
    def _build_text(post: dict) -> str:
        """
        Builds the text to analyze from a post dict.
        Title is most signal-dense; body adds context.
        """
        title = post.get('title', '')
        body  = post.get('body', '')[:300]
        return f'{title} {body}'.strip()


# ─────────────────────────────────────────────────────────────
if __name__ == '__main__':
    print('='*55)
    print('  Sentiment Analyzer — Manual Test')
    print('='*55)

    analyzer = SentimentAnalyzer()

    # Test with known examples
    test_posts = [
        {
            '_id':          'test1',
            'title':        'GME SHORT SQUEEZE INCOMING 🚀🚀🚀 '
                            'SHORT INTEREST AT 40% LETS GO',
            'body':         'Borrow rate spiking, days to cover '
                            'through the roof. This is it boys.',
            'score':        5420,
            'upvote_ratio': 0.94,
        },
        {
            '_id':          'test2',
            'title':        'GME is a obvious pump and dump, '
                            'avoid this garbage stock',
            'body':         'No fundamentals, just retail bagholders '
                            'hoping for a miracle. Sell now.',
            'score':        120,
            'upvote_ratio': 0.45,
        },
        {
            '_id':          'test3',
            'title':        'Thoughts on GME Q4 earnings?',
            'body':         'Revenue was in line, nothing spectacular. '
                            'Watching to see what happens.',
            'score':        340,
            'upvote_ratio': 0.78,
        },
        {
            '_id':          'test4',
            'title':        'GME gamma squeeze setup — '
                            'options chain looks perfect',
            'body':         'Max pain at $25, tons of call OI above. '
                            'Market makers will have to hedge.',
            'score':        2100,
            'upvote_ratio': 0.88,
        },
    ]

    print('\n[1] FinBERT sentiment analysis...')
    start = time.time()
    analyzed = analyzer.analyze_posts(test_posts, use_finbert=True)
    elapsed  = time.time() - start

    for p in analyzed:
        print(f'\n  [{p["sentiment_label"]:8s} '
              f'{p["sentiment_score"]:+.3f}] '
              f'(score:{p["score"]}) '
              f'{p["title"][:60]}...')

    print(f'\n  Processed {len(analyzed)} posts in {elapsed:.2f}s')

    print('\n[2] Aggregate sentiment...')
    agg = analyzer.aggregate_sentiment(analyzed)
    print(f'  sentiment_score = {agg["sentiment_score"]:+.4f}')
    print(f'  bullish_pct     = {agg["bullish_pct"]:.1%}')
    print(f'  bearish_pct     = {agg["bearish_pct"]:.1%}')
    print(f'  neutral_pct     = {agg["neutral_pct"]:.1%}')
    print(f'  high_conviction = {agg["high_conviction"]}')
    print(f'  post_count      = {agg["post_count"]}')

    print('\n[3] VADER fallback...')
    vader_analyzed = analyzer.analyze_posts(
        test_posts.copy(), use_finbert=False)
    for p in vader_analyzed:
        print(f'  [{p["sentiment_label"]:8s} '
              f'{p["sentiment_score"]:+.3f}] '
              f'{p["title"][:50]}...')

    print('\n✓ Sentiment analyzer test complete')