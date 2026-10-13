# pipeline/nlp/keyword_extractor.py
#
# Extracts squeeze-relevant keywords from Reddit posts.
# Computes keyword_urgency_score — how much squeeze language
# is present in the conversation around a ticker.
#
# Two methods:
#   1. Rule-based keyword matching (fast, deterministic)
#      → Used for real-time urgency scoring
#   2. KeyBERT semantic extraction (slower, richer)
#      → Used for topic discovery and narrative shift detection
#
# Why keyword_urgency_score matters:
#   Language shift from "undervalued" → "squeeze" → "moon"
#   is itself a measurable signal. The transition predicts
#   which lifecycle phase the stock is in.

import re
import time
from datetime import datetime
from collections import Counter
from typing import Optional

# ─────────────────────────────────────────────────────────────
# SQUEEZE VOCABULARY — grouped by signal strength
# ─────────────────────────────────────────────────────────────

HIGH_URGENCY_KEYWORDS = [
    # Direct squeeze language (strongest signals)
    'short squeeze', 'gamma squeeze', 'short interest',
    'days to cover', 'naked shorts', 'naked shorting',
    'hard to borrow', 'htb', 'fail to deliver', 'ftd',
    'forced covering', 'margin call', 'short ladder',
    'borrow rate', 'cost to borrow', 'shares available',
    'short float', 'covering', 'squeeze play',
    'max pain', 'options expiry', 'gamma exposure',
    'dark pool', 'payment for order flow', 'pfof',
    'citadel', 'market maker hedge',
]

MEDIUM_URGENCY_KEYWORDS = [
    # Options speculation language
    'calls', 'puts', 'yolo', 'options chain',
    'theta gang', 'iv crush', 'implied volatility',
    'open interest', 'call sweep', 'unusual options',
    # Retail coordination language
    'diamond hands', 'apes together', 'hold the line',
    'dont sell', 'hodl', 'to the moon', 'tendies',
    'squeeze incoming', 'this is the way',
    # Price target language
    'price target', 'moon', 'squeeze target',
    '10x', '100x', 'short term catalyst',
]

LOW_URGENCY_KEYWORDS = [
    # General bullish retail sentiment
    'buy the dip', 'oversold', 'undervalued',
    'undervalued gem', 'hidden gem', 'strong buy',
    'bullish', 'breakout', 'momentum', 'volume spike',
    'relative strength', 'chart looks good',
    'earnings beat', 'catalyst', 'news',
    'upgrade', 'analyst', 'institutional',
]

# All keywords combined with weights
KEYWORD_WEIGHTS = {}
for kw in HIGH_URGENCY_KEYWORDS:
    KEYWORD_WEIGHTS[kw] = 3.0
for kw in MEDIUM_URGENCY_KEYWORDS:
    KEYWORD_WEIGHTS[kw] = 2.0
for kw in LOW_URGENCY_KEYWORDS:
    KEYWORD_WEIGHTS[kw] = 1.0

# Lazy-loaded KeyBERT model
_keybert_model = None


def _get_keybert():
    global _keybert_model
    if _keybert_model is None:
        try:
            from keybert import KeyBERT
            _keybert_model = KeyBERT()
            print('  [NLP] KeyBERT loaded ✓')
        except Exception as e:
            print(f'  [NLP] KeyBERT load failed: {e}')
    return _keybert_model


class KeywordExtractor:
    """
    Extracts squeeze-relevant keywords from Reddit posts
    and computes an urgency score for the ML feature vector.
    """

    def extract_from_posts(self, posts: list) -> dict:
        """
        Extracts keywords and urgency score from a list of posts.

        Args:
          posts: list of post dicts (title + body)

        Returns dict with:
          top_keywords          — list of top keyword strings
          keyword_urgency_score — 0.0 to 1.0 (1.0 = max squeeze talk)
          matched_squeeze_terms — which high-urgency terms were found
          keyword_counts        — frequency of each keyword
          squeeze_language_pct  — % of posts using squeeze language
        """
        if not posts:
            return self._empty_result()

        # Combine all text
        all_text = ' '.join(
            (p.get('title', '') + ' '
             + p.get('body', ''))
            for p in posts
        ).lower()

        # Rule-based extraction (always runs — fast)
        rule_result = self._rule_based_extract(all_text)

        # Per-post squeeze language detection
        squeeze_posts = sum(
            1 for p in posts
            if self._has_squeeze_language(
                p.get('title', '') + ' '
                + p.get('body', ''))
        )
        squeeze_language_pct = squeeze_posts / len(posts)

        # KeyBERT extraction (runs if model available)
        keybert_keywords = []
        if len(all_text) > 100:
            keybert_keywords = self._keybert_extract(all_text)

        # Merge keyword lists
        all_keywords = list(set(
            rule_result['matched_terms'] + keybert_keywords
        ))

        return {
            'top_keywords':            all_keywords[:20],
            'keyword_urgency_score':   rule_result['urgency_score'],
            'matched_squeeze_terms':   rule_result['matched_terms'],
            'high_urgency_terms':      rule_result['high_urgency'],
            'keyword_counts':          rule_result['counts'],
            'squeeze_language_pct':    round(
                squeeze_language_pct, 4),
            'post_count_analyzed':     len(posts),
            'keybert_keywords':        keybert_keywords,
            'computed_at':             datetime.utcnow(),
        }

    def extract_from_text(self, text: str) -> dict:
        """
        Extracts keywords from a single text string.
        Used for per-post keyword annotation.
        """
        text_lower = text.lower()
        rule_result = self._rule_based_extract(text_lower)
        return {
            'keywords':              rule_result['matched_terms'],
            'keyword_urgency_score': rule_result['urgency_score'],
            'has_squeeze_language':  self._has_squeeze_language(text),
        }

    def compute_narrative_shift(self,
                                 ticker: str,
                                 recent_posts: list,
                                 older_posts: list) -> dict:
        """
        Detects if the narrative around a ticker is shifting
        from fundamental to squeeze language.

        Args:
          recent_posts: last 6 hours
          older_posts:  6-24 hours ago

        Returns:
          shift_detected  — bool
          shift_direction — 'toward_squeeze' / 'away_from_squeeze'
          urgency_delta   — change in urgency score
        """
        if not recent_posts or not older_posts:
            return {
                'shift_detected':  False,
                'urgency_delta':   0.0,
                'shift_direction': 'insufficient_data',
            }

        recent_result = self.extract_from_posts(recent_posts)
        older_result  = self.extract_from_posts(older_posts)

        recent_urgency = recent_result['keyword_urgency_score']
        older_urgency  = older_result['keyword_urgency_score']
        delta          = recent_urgency - older_urgency

        return {
            'shift_detected':   abs(delta) > 0.15,
            'urgency_delta':    round(delta, 4),
            'recent_urgency':   recent_urgency,
            'older_urgency':    older_urgency,
            'shift_direction': (
                'toward_squeeze' if delta > 0
                else 'away_from_squeeze'
            ),
        }

    # ═════════════════════════════════════════════════════════
    # EXTRACTION BACKENDS
    # ═════════════════════════════════════════════════════════

    def _rule_based_extract(self, text: str) -> dict:
        """
        Fast rule-based keyword matching.
        Scans text for all known squeeze vocabulary.
        Computes weighted urgency score.
        """
        text_lower  = text.lower()
        matched     = []
        high_urgency = []
        counts      = Counter()
        total_weight = 0.0
        max_possible = (
            len(HIGH_URGENCY_KEYWORDS) * 3.0
            + len(MEDIUM_URGENCY_KEYWORDS) * 2.0
            + len(LOW_URGENCY_KEYWORDS) * 1.0
        )

        for keyword, weight in KEYWORD_WEIGHTS.items():
            # Count occurrences
            count = text_lower.count(keyword)
            if count > 0:
                matched.append(keyword)
                counts[keyword] = count
                total_weight   += weight * min(count, 3)
                # Cap at 3 occurrences per keyword
                if keyword in HIGH_URGENCY_KEYWORDS:
                    high_urgency.append(keyword)

        # Normalize to 0-1
        urgency_score = min(
            total_weight / max_possible * 5, 1.0
        ) if max_possible > 0 else 0.0
        # Multiply by 5 to make the scale more sensitive
        # (most texts will hit <20% of keywords)

        return {
            'matched_terms':  matched,
            'high_urgency':   high_urgency,
            'counts':         dict(counts),
            'urgency_score':  round(urgency_score, 4),
        }

    def _keybert_extract(self, text: str,
                          n_keywords: int = 15) -> list:
        """
        KeyBERT semantic keyword extraction.
        Finds contextually relevant phrases beyond exact matches.
        """
        model = _get_keybert()
        if model is None:
            return []

        try:
            keywords = model.extract_keywords(
                text[:5000],  # Limit text length
                keyphrase_ngram_range=(1, 3),
                stop_words='english',
                top_n=n_keywords,
                diversity=0.5,
                # diversity=0.5 balances relevance vs variety
            )
            return [kw for kw, score in keywords
                    if score > 0.2]
        except Exception as e:
            print(f'  [NLP] KeyBERT extraction error: {e}')
            return []

    def _has_squeeze_language(self, text: str) -> bool:
        """
        Returns True if a single post contains any
        high or medium urgency squeeze vocabulary.
        """
        text_lower = text.lower()
        for kw in HIGH_URGENCY_KEYWORDS + MEDIUM_URGENCY_KEYWORDS:
            if kw in text_lower:
                return True
        return False

    @staticmethod
    def _empty_result() -> dict:
        return {
            'top_keywords':           [],
            'keyword_urgency_score':  0.0,
            'matched_squeeze_terms':  [],
            'high_urgency_terms':     [],
            'keyword_counts':         {},
            'squeeze_language_pct':   0.0,
            'post_count_analyzed':    0,
            'keybert_keywords':       [],
        }


# ─────────────────────────────────────────────────────────────
if __name__ == '__main__':
    print('='*55)
    print('  Keyword Extractor — Manual Test')
    print('='*55)

    extractor = KeywordExtractor()

    test_posts = [
        {
            'title': 'GME SHORT SQUEEZE — short interest 40%, '
                     'borrow rate spiking to 80%, days to cover 5+',
            'body':  'Fail to deliver numbers through the roof. '
                     'Market makers need to hedge. Gamma squeeze '
                     'incoming. Diamond hands apes.',
        },
        {
            'title': 'GME Q4 earnings analysis — fundamentals look ok',
            'body':  'Revenue was in line with expectations. '
                     'Management seems to be executing. '
                     'Long term hold.',
        },
        {
            'title': 'GME YOLO — bought 500 calls, this is the way',
            'body':  'Max pain at $25. Implied volatility crushed. '
                     'Short ladder attacks won\'t stop us. Moon.',
        },
    ]

    print('\n[1] extract_from_posts()')
    result = extractor.extract_from_posts(test_posts)
    print(f'  urgency_score:        {result["keyword_urgency_score"]}')
    print(f'  squeeze_language_pct: {result["squeeze_language_pct"]:.1%}')
    print(f'  high_urgency_terms:   {result["high_urgency_terms"]}')
    print(f'  top_keywords:         {result["top_keywords"][:8]}')

    print('\n[2] Single text extraction')
    text = 'Short squeeze incoming! Borrow rate at 80%, naked shorts exposed.'
    r = extractor.extract_from_text(text)
    print(f'  urgency_score: {r["keyword_urgency_score"]}')
    print(f'  keywords:      {r["keywords"]}')
    print(f'  has_squeeze:   {r["has_squeeze_language"]}')

    print('\n[3] Narrative shift detection')
    recent = test_posts[:2]
    older  = test_posts[1:]
    shift  = extractor.compute_narrative_shift('GME', recent, older)
    print(f'  shift_detected:  {shift["shift_detected"]}')
    print(f'  urgency_delta:   {shift["urgency_delta"]:+.4f}')
    print(f'  direction:       {shift["shift_direction"]}')

    print('\n✓ Keyword extractor test complete')