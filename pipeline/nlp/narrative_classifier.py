# pipeline/nlp/narrative_classifier.py
#
# Classifies the dominant narrative around a ticker from Reddit posts.
#
# Five narrative categories:
#   squeeze_play      — community primarily discussing squeeze mechanics
#   meme_hype         — emoji/moon culture, community-driven momentum
#   fundamental_buy   — earnings, revenue, product, valuation discussion
#   momentum_trade    — technical analysis, chart patterns, breakouts
#   fear_uncertainty  — negative sentiment, fraud concerns, sell signals
#
# Why narrative classification matters:
#   Two stocks can both have 500 Reddit mentions.
#   One is being discussed as a squeeze play (high signal).
#   One is being discussed because of an SEC investigation (bearish).
#   Raw mention count cannot distinguish these.
#   Narrative classification can.
#
# The narrative_squeeze_prob feature feeds directly into
# the ML lifecycle classifier as a 0-1 continuous feature.

import re
from datetime import datetime
from collections import defaultdict
from typing import Optional

# ─────────────────────────────────────────────────────────────
# NARRATIVE PATTERN DEFINITIONS
# Each narrative has a set of keyword patterns.
# Patterns are matched against the full text of posts.
# ─────────────────────────────────────────────────────────────

NARRATIVE_PATTERNS = {

    'squeeze_play': {
        'weight': 1.0,
        'patterns': [
            'short squeeze', 'gamma squeeze', 'short interest',
            'days to cover', 'borrow rate', 'hard to borrow',
            'fail to deliver', 'ftd', 'naked short', 'covering',
            'forced cover', 'margin call', 'squeeze play',
            'options expiry', 'max pain', 'gamma exposure',
            'dark pool', 'short ladder', 'short float',
            'shares available', 'cost to borrow',
        ],
    },

    'meme_hype': {
        'weight': 0.9,
        'patterns': [
            'to the moon', 'diamond hands', 'apes together',
            'tendies', 'yolo', 'hodl', 'this is the way',
            'let\'s go', 'hold the line', 'we like the stock',
            'retard', 'regard', 'stonks', 'degen',
            'rocket', '🚀', '💎', '🙌', '🦍', 'wsb',
            'wallstreetbets', 'squeeze incoming',
        ],
    },

    'fundamental_buy': {
        'weight': 0.8,
        'patterns': [
            'earnings', 'revenue', 'profit', 'guidance',
            'pe ratio', 'valuation', 'undervalued', 'fair value',
            'balance sheet', 'cash flow', 'ebitda', 'margins',
            'fundamentals', 'management', 'ceo', 'product',
            'market share', 'competitive', 'moat', 'dcf',
            'price target', 'analyst', 'upgrade', 'downgrade',
        ],
    },

    'momentum_trade': {
        'weight': 0.7,
        'patterns': [
            'breakout', 'resistance', 'support', 'chart',
            'technical', 'rsi', 'macd', 'volume spike',
            'moving average', 'trend', 'bullish flag',
            'cup and handle', 'ascending triangle', 'wedge',
            'momentum', 'relative strength', '52 week high',
            'all time high', 'ath', 'consolidation',
            'price action', 'level two',
        ],
    },

    'fear_uncertainty': {
        'weight': 0.6,
        'patterns': [
            'sec investigation', 'fraud', 'scam', 'pump and dump',
            'bankruptcy', 'delisted', 'avoid', 'sell everything',
            'bag holder', 'bagholder', 'bubble', 'overvalued',
            'dont buy', 'red flags', 'stay away', 'warning',
            'dilution', 'offering', 'lawsuit', 'short seller report',
            'resign', 'accounting irregularity', 'restatement',
        ],
    },
}


class NarrativeClassifier:
    """
    Classifies the dominant narrative around a ticker.
    Uses rule-based pattern matching (fast and interpretable).

    Can be upgraded to ML-based classification once
    enough labeled examples are collected.
    """

    def classify(self, posts: list,
                 keywords: list = None) -> dict:
        """
        Classifies the dominant narrative from a list of posts.

        Args:
          posts:    list of post dicts (title + body)
          keywords: optional pre-extracted keywords from KeyBERT

        Returns dict with:
          dominant_narrative    — category with highest score
          narrative_scores      — score for each category (0-1)
          narrative_squeeze_prob — probability of squeeze narrative (0-1)
          narrative_shift_detected — bool: is language changing?
          confidence            — how clear the dominant narrative is
          supporting_evidence   — which patterns triggered
        """
        if not posts:
            return self._empty_result()

        # Combine all text
        all_text = ' '.join(
            p.get('title', '') + ' ' + p.get('body', '')
            for p in posts
        ).lower()

        # Score each narrative
        raw_scores    = {}
        evidence      = defaultdict(list)

        for narrative, config in NARRATIVE_PATTERNS.items():
            score  = 0.0
            weight = config['weight']

            for pattern in config['patterns']:
                count = all_text.count(pattern)
                if count > 0:
                    score += count * weight
                    evidence[narrative].append(
                        f'{pattern}({count})')

            raw_scores[narrative] = score

        # Normalize scores to 0-1
        total = sum(raw_scores.values()) or 1.0
        norm_scores = {
            k: round(v / total, 4)
            for k, v in raw_scores.items()
        }

        # Identify dominant narrative
        dominant = max(norm_scores, key=norm_scores.get)
        dom_score = norm_scores[dominant]

        # squeeze_prob — how squeeze-focused is the conversation?
        squeeze_score = norm_scores.get('squeeze_play', 0)
        meme_score    = norm_scores.get('meme_hype', 0)
        # Meme hype amplifies squeeze probability
        squeeze_prob  = min(
            squeeze_score + meme_score * 0.3, 1.0)

        # Confidence — how clear is the dominant narrative?
        # If one narrative has > 50% of score → high confidence
        if dom_score > 0.50:
            confidence = 'high'
        elif dom_score > 0.35:
            confidence = 'medium'
        else:
            confidence = 'low'

        # Narrative shift — are squeeze keywords being used more?
        shift = self._detect_shift(posts)

        return {
            'dominant_narrative':     dominant,
            'narrative_scores':       norm_scores,
            'narrative_squeeze_prob': round(squeeze_prob, 4),
            'narrative_shift_detected': shift['detected'],
            'shift_direction':        shift['direction'],
            'confidence':             confidence,
            'supporting_evidence': {
                k: v[:5]  # top 5 matching patterns
                for k, v in evidence.items()
                if v
            },
            'post_count':             len(posts),
            'computed_at':            datetime.utcnow(),
        }

    def classify_single_post(self, post: dict) -> dict:
        """
        Classifies a single post. Used for per-post annotation
        in the NLP pipeline.
        """
        text = (post.get('title', '') + ' '
                + post.get('body', '')).lower()

        scores = {}
        for narrative, config in NARRATIVE_PATTERNS.items():
            score = sum(
                text.count(p) * config['weight']
                for p in config['patterns']
            )
            scores[narrative] = score

        total    = sum(scores.values()) or 1.0
        dominant = max(scores, key=scores.get)
        norm     = {k: round(v/total, 4) for k, v in scores.items()}

        return {
            'narrative_category':  dominant,
            'narrative_scores':    norm,
            'narrative_squeeze_prob': min(
                norm.get('squeeze_play', 0)
                + norm.get('meme_hype', 0) * 0.3,
                1.0
            ),
        }

    def _detect_shift(self, posts: list) -> dict:
        """
        Detects if narrative is shifting by comparing
        first half vs second half of posts (sorted by time).
        """
        if len(posts) < 6:
            return {'detected': False, 'direction': 'insufficient_data'}

        # Sort by creation time
        try:
            sorted_posts = sorted(
                posts,
                key=lambda p: p.get('created_utc', datetime.min)
            )
        except Exception:
            return {'detected': False, 'direction': 'sort_error'}

        mid        = len(sorted_posts) // 2
        older_half = sorted_posts[:mid]
        newer_half = sorted_posts[mid:]

        older_text = ' '.join(
            p.get('title', '') + ' ' + p.get('body', '')
            for p in older_half
        ).lower()

        newer_text = ' '.join(
            p.get('title', '') + ' ' + p.get('body', '')
            for p in newer_half
        ).lower()

        older_squeeze = sum(
            older_text.count(p)
            for p in NARRATIVE_PATTERNS['squeeze_play']['patterns']
        )
        newer_squeeze = sum(
            newer_text.count(p)
            for p in NARRATIVE_PATTERNS['squeeze_play']['patterns']
        )

        # Normalize by post count
        older_rate = older_squeeze / len(older_half)
        newer_rate = newer_squeeze / len(newer_half)
        delta      = newer_rate - older_rate

        return {
            'detected':  abs(delta) > 0.5,
            'direction': (
                'toward_squeeze' if delta > 0
                else 'away_from_squeeze'
            ),
            'delta':     round(delta, 4),
        }

    @staticmethod
    def _empty_result() -> dict:
        return {
            'dominant_narrative':       'unknown',
            'narrative_scores':         {
                k: 0.0 for k in NARRATIVE_PATTERNS
            },
            'narrative_squeeze_prob':   0.0,
            'narrative_shift_detected': False,
            'shift_direction':          'insufficient_data',
            'confidence':               'low',
            'supporting_evidence':      {},
            'post_count':               0,
        }


# ─────────────────────────────────────────────────────────────
if __name__ == '__main__':
    print('='*55)
    print('  Narrative Classifier — Manual Test')
    print('='*55)

    clf = NarrativeClassifier()

    # Test case 1: squeeze play narrative
    squeeze_posts = [
        {
            'title': 'Short interest at 40%, borrow rate 80%, '
                     'days to cover 5+ — SQUEEZE INCOMING',
            'body':  'Fail to deliver numbers through the roof. '
                     'Gamma squeeze setup perfect. Max pain $25. '
                     'Market makers forced to hedge.',
            'score': 5000,
            'created_utc': datetime(2026, 1, 1, 10, 0),
        },
        {
            'title': 'The short squeeze thesis for GME '
                     '— everything aligns',
            'body':  'Naked shorts, hard to borrow, options expiry '
                     'this Friday. Cost to borrow spiking.',
            'score': 2100,
            'created_utc': datetime(2026, 1, 1, 11, 0),
        },
        {
            'title': 'GME gamma squeeze — calls being swept',
            'body':  'Short ladder attack failed. Dark pool volume '
                     'suspicious. Covering imminent.',
            'score': 3400,
            'created_utc': datetime(2026, 1, 1, 12, 0),
        },
    ]

    print('\n[1] Squeeze play narrative:')
    r1 = clf.classify(squeeze_posts)
    print(f'  dominant:      {r1["dominant_narrative"]}')
    print(f'  squeeze_prob:  {r1["narrative_squeeze_prob"]}')
    print(f'  confidence:    {r1["confidence"]}')
    print(f'  scores:        {r1["narrative_scores"]}')
    print(f'  shift:         {r1["narrative_shift_detected"]}')

    # Test case 2: fundamental narrative
    fundamental_posts = [
        {
            'title': 'GME Q4 earnings beat — revenue up 15%',
            'body':  'Management executing well. PE ratio reasonable. '
                     'Undervalued at current levels. Cash flow positive.',
            'score': 800,
            'created_utc': datetime(2026, 1, 1, 10, 0),
        },
        {
            'title': 'GME valuation analysis — fair value $35',
            'body':  'DCF model shows intrinsic value above current '
                     'price. Strong balance sheet, no debt.',
            'score': 600,
            'created_utc': datetime(2026, 1, 1, 11, 0),
        },
    ]

    print('\n[2] Fundamental narrative:')
    r2 = clf.classify(fundamental_posts)
    print(f'  dominant:      {r2["dominant_narrative"]}')
    print(f'  squeeze_prob:  {r2["narrative_squeeze_prob"]}')
    print(f'  confidence:    {r2["confidence"]}')

    # Test case 3: single post classification
    print('\n[3] Single post classification:')
    post = {
        'title': 'Short squeeze incoming — massive FTD, '
                 'borrow rate 90%',
        'body':  'Diamond hands, to the moon 🚀',
    }
    r3 = clf.classify_single_post(post)
    print(f'  category:      {r3["narrative_category"]}')
    print(f'  squeeze_prob:  {r3["narrative_squeeze_prob"]}')

    print('\n✓ Narrative classifier test complete')