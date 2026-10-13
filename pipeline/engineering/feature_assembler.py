# pipeline/engineering/feature_assembler.py  (v2 — news NLP)
#
# Changes from v1:
#   Features 25-27 now use news NLP instead of Reddit placeholders
#   25: news_sentiment_score        (was: sentiment_finbert → 0.0)
#   26: news_squeeze_keyword_score  (was: keyword_urgency_score → 0.0)
#   27: news_catalyst_composite     (was: narrative_squeeze_prob → 0.0)
#
# Everything else identical to v1.
# Replace feature_assembler.py with this file.

import os
import time
import numpy as np
import psycopg2
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

from pipeline.engineering.market_features import MarketFeatureEngine
from pipeline.engineering.options_features import OptionsFeatureEngine
from pipeline.engineering.si_proxy import SIProxyEstimator
from pipeline.nlp.news_nlp_pipeline import NewsNLPPipeline

# ─────────────────────────────────────────────────────────────
# FEATURE ORDER — DO NOT CHANGE INDEX POSITIONS
# ─────────────────────────────────────────────────────────────
FEATURE_ORDER = [
    # Market / Short Interest (0-5)
    'short_interest_pct_norm',
    'days_to_cover_norm',
    'borrow_rate_norm',
    'si_staleness_penalty',
    'borrow_rate_delta_norm',
    'short_volume_ratio',

    # Price / Volume (6-11)
    'volume_ratio_30d_norm',
    'float_rotation_norm',
    'price_change_1d',
    'price_change_5d',
    'rsi_14_norm',
    'distance_from_52w_high_norm',

    # Volatility / Technical (12-13)
    'atr_norm',
    'volatility_10d_norm',

    # Options (14-18)
    'call_put_ratio_norm',
    'iv_rank_norm',
    'max_pain_distance_norm',
    'options_available',
    'unusual_sweep_flag',

    # Social — StockTwits + Trends + SEC (19-24)
    'stocktwits_bull_ratio',
    'stocktwits_message_count_norm',
    'google_trend_score_norm',
    'trend_spike_flag',
    'news_catalyst_flag',
    'has_activist_filing',

    # News NLP (25-27) — replaces Reddit placeholders
    'news_sentiment_score',        # 25 FinBERT on headlines [-1,1]
    'news_squeeze_keyword_score',  # 26 squeeze language [0,1]
    'news_catalyst_composite',     # 27 catalyst + sentiment [0,1]
]

FEATURE_COUNT = len(FEATURE_ORDER)  # 28

NORM_RANGES = {
    'short_interest_pct':       (0.0,   100.0),
    'days_to_cover':            (0.0,    20.0),
    'borrow_rate':              (0.0,   300.0),
    'borrow_rate_delta':       (-100.0, 100.0),
    'volume_ratio_30d':         (0.0,    20.0),
    'float_rotation':           (0.0,     5.0),
    'rsi_14':                   (0.0,   100.0),
    'distance_from_52w_high':   (0.0,     1.0),
    'volatility_10d':           (0.0,     0.5),
    'call_put_ratio':           (0.0,    10.0),
    'iv_rank':                  (0.0,   100.0),
    'max_pain_distance':       (-1.0,    1.0),
    'stocktwits_message_count': (0,     500),
    'google_trend_score':       (0.0,   100.0),
    'news_sentiment_score':    (-1.0,    1.0),
}


def normalize(value: float, feature_name: str) -> float:
    if value is None or (isinstance(value, float)
                          and np.isnan(value)):
        return 0.0
    if feature_name not in NORM_RANGES:
        return float(np.clip(value, -1.0, 1.0))
    lo, hi = NORM_RANGES[feature_name]
    clipped = float(np.clip(value, lo, hi))
    if hi == lo:
        return 0.0
    if lo < 0:
        return round(clipped / max(abs(lo), abs(hi)), 6)
    return round((clipped - lo) / (hi - lo), 6)


class FeatureAssembler:
    """
    Assembles the complete 28-feature vector for ML scoring.
    v2: Uses news NLP features (25-27) instead of Reddit placeholders.
    """

    def __init__(self):
        self.market_engine  = MarketFeatureEngine()
        self.options_engine = OptionsFeatureEngine()
        self.si_proxy       = SIProxyEstimator()
        self.news_nlp       = NewsNLPPipeline()
        self.pg             = psycopg2.connect(
            os.getenv('POSTGRES_URL'))

    # ═════════════════════════════════════════════════════════
    # MAIN METHOD
    # ═════════════════════════════════════════════════════════

    def assemble(self, ticker: str) -> dict:
        """
        Assembles the complete 28-feature dict for one ticker.
        Returns dict with raw, normalized, and vector fields.
        """
        ticker = ticker.upper().strip()
        start  = time.time()

        # 1. Market features
        market  = self.market_engine.compute(ticker)

        # 2. SI proxy adjustment
        si_est  = self.si_proxy.estimate(ticker)
        if si_est.get('si_stale_days', 0) > 7:
            market['short_interest_pct'] = si_est.get(
                'si_effective_pct',
                market.get('short_interest_pct', 0.0))

        # 3. Options features
        options = self.options_engine.compute(ticker)

        # 4. Social features
        social  = self._get_social_features(ticker)

        # 5. News NLP features (replaces Reddit)
        nlp     = self.news_nlp.get_ticker_nlp_features(
            ticker, hours=48)

        # 6. Build raw dict
        raw     = self._build_raw_dict(
            market, options, si_est, social, nlp)

        # 7. Normalize
        normalized = self._normalize_all(raw)

        # 8. Build ordered vector
        feature_vector = [
            normalized.get(f, 0.0) for f in FEATURE_ORDER]

        # 9. Quality assessment
        binary_ok = {
            'options_available', 'unusual_sweep_flag',
            'trend_spike_flag', 'news_catalyst_flag',
            'has_activist_filing',
        }
        missing = [
            f for f in FEATURE_ORDER
            if normalized.get(f, 0.0) == 0.0
            and f not in binary_ok
        ]
        quality = round(
            (FEATURE_COUNT - len(missing))
            / FEATURE_COUNT * 100, 1)

        return {
            'ticker':               ticker,
            'raw':                  raw,
            'normalized':           normalized,
            'feature_vector':       feature_vector,
            'quality_score':        quality,
            'missing_features':     missing,
            'si_proxy_direction':   si_est.get('si_proxy_direction'),
            'si_proxy_confidence':  si_est.get('si_proxy_confidence'),
            'news_nlp_available':   nlp.get('news_nlp_available', False),
            'news_article_count':   nlp.get('news_article_count', 0),
            'assembled_at':         datetime.utcnow(),
            'assembly_duration_ms': int((time.time()-start)*1000),
        }

    def to_vector(self, assembled: dict) -> np.ndarray:
        """Converts assembled dict to numpy array shape (28,)."""
        vector = np.array(
            assembled['feature_vector'], dtype=np.float32)
        vector = np.nan_to_num(vector, nan=0.0,
                                posinf=1.0, neginf=-1.0)
        vector = np.clip(vector, -1.0, 1.0)
        assert len(vector) == FEATURE_COUNT
        return vector

    def validate_vector(self, vector: np.ndarray) -> bool:
        """Validates feature vector before ML scoring."""
        valid = True
        if len(vector) != FEATURE_COUNT:
            print(f'  Wrong length: {len(vector)}')
            valid = False
        if np.any(np.isnan(vector)):
            nans = np.where(np.isnan(vector))[0]
            for i in nans:
                print(f'  NaN at [{i}]: {FEATURE_ORDER[i]}')
            valid = False
        return valid

    def get_feature_names(self) -> list:
        return FEATURE_ORDER.copy()

    # ═════════════════════════════════════════════════════════
    # SOCIAL FEATURES
    # ═════════════════════════════════════════════════════════

    def _get_social_features(self, ticker: str) -> dict:
        """Reads StockTwits, Trends, News, SEC from PostgreSQL."""
        cur    = self.pg.cursor()
        result = {
            'stocktwits_bull_ratio':    0.0,
            'stocktwits_message_count': 0,
            'google_trend_score':       0.0,
            'trend_spike_flag':         0,
            'news_catalyst_flag':       0,
            'has_activist_filing':      0,
        }

        try:
            cur.execute("""
                SELECT bull_ratio, message_count
                FROM   stocktwits_snapshots
                WHERE  ticker = %s
                ORDER  BY collected_at DESC LIMIT 1
            """, (ticker,))
            row = cur.fetchone()
            if row:
                result['stocktwits_bull_ratio']    = float(row[0] or 0)
                result['stocktwits_message_count'] = int(row[1] or 0)
        except Exception:
            pass

        try:
            cur.execute("""
                SELECT trend_score, trend_spike
                FROM   trends_snapshots
                WHERE  ticker = %s
                ORDER  BY collected_at DESC LIMIT 1
            """, (ticker,))
            row = cur.fetchone()
            if row:
                result['google_trend_score'] = float(row[0] or 0)
                result['trend_spike_flag']   = 1 if row[1] else 0
        except Exception:
            pass

        try:
            cur.execute("""
                SELECT COUNT(*) FROM stock_events
                WHERE ticker = %s
                  AND event_type = 'catalyst'
                  AND occurred_at > NOW() - INTERVAL '48 hours'
            """, (ticker,))
            if cur.fetchone()[0] > 0:
                result['news_catalyst_flag'] = 1
        except Exception:
            pass

        try:
            cur.execute("""
                SELECT COUNT(*) FROM stock_events
                WHERE ticker = %s
                  AND event_type = 'activist_filing'
                  AND occurred_at > NOW() - INTERVAL '30 days'
            """, (ticker,))
            if cur.fetchone()[0] > 0:
                result['has_activist_filing'] = 1
        except Exception:
            pass

        return result

    # ═════════════════════════════════════════════════════════
    # RAW DICT BUILD
    # ═════════════════════════════════════════════════════════

    def _build_raw_dict(self, market: dict, options: dict,
                         si_est: dict, social: dict,
                         nlp: dict) -> dict:
        raw = {}

        # Market
        raw['short_interest_pct']     = market.get('short_interest_pct', 0.0)
        raw['days_to_cover']          = market.get('days_to_cover', 0.0)
        raw['borrow_rate']            = market.get('borrow_rate', 0.0)
        raw['si_staleness_penalty']   = si_est.get('staleness_penalty', 0.0)
        raw['borrow_rate_delta']      = market.get('borrow_rate_delta', 0.0)
        raw['short_volume_ratio']     = market.get('short_volume_ratio', 0.0)
        raw['volume_ratio_30d']       = market.get('volume_ratio_30d', 0.0)
        raw['float_rotation']         = market.get('float_rotation', 0.0)
        raw['price_change_1d']        = market.get('price_change_1d', 0.0)
        raw['price_change_5d']        = market.get('price_change_5d', 0.0)
        raw['rsi_14']                 = market.get('rsi_14', 50.0)
        raw['distance_from_52w_high'] = market.get('distance_from_52w_high', 0.0)
        atr   = market.get('atr_14', 0.0) or 0.0
        price = market.get('price_current', 1.0) or 1.0
        raw['atr_norm']               = atr / price if price > 0 else 0.0
        raw['volatility_10d']         = market.get('volatility_10d', 0.0)

        # Options
        raw['call_put_ratio']         = options.get('call_put_ratio', 0.0)
        raw['iv_rank']                = options.get('iv_rank', 0.0)
        raw['max_pain_distance']      = options.get('max_pain_distance', 0.0)
        raw['options_available']      = 1.0 if options.get('options_available') else 0.0
        raw['unusual_sweep_flag']     = 1.0 if options.get('unusual_sweep_flag') else 0.0

        # Social
        raw['stocktwits_bull_ratio']     = social.get('stocktwits_bull_ratio', 0.0)
        raw['stocktwits_message_count']  = social.get('stocktwits_message_count', 0)
        raw['google_trend_score']        = social.get('google_trend_score', 0.0)
        raw['trend_spike_flag']          = social.get('trend_spike_flag', 0)
        raw['news_catalyst_flag']        = social.get('news_catalyst_flag', 0)
        raw['has_activist_filing']       = social.get('has_activist_filing', 0)

        # News NLP (features 25-27)
        raw['news_sentiment_score']       = nlp.get('news_sentiment_score', 0.0)
        raw['news_squeeze_keyword_score'] = nlp.get('news_squeeze_keyword_score', 0.0)
        raw['news_catalyst_composite']    = nlp.get('news_catalyst_composite', 0.0)

        return raw

    # ═════════════════════════════════════════════════════════
    # NORMALIZATION
    # ═════════════════════════════════════════════════════════

    def _normalize_all(self, raw: dict) -> dict:
        n = {}

        n['short_interest_pct_norm']    = normalize(raw['short_interest_pct'], 'short_interest_pct')
        n['days_to_cover_norm']         = normalize(raw['days_to_cover'], 'days_to_cover')
        n['borrow_rate_norm']           = normalize(raw['borrow_rate'], 'borrow_rate')
        n['si_staleness_penalty']       = float(np.clip(raw.get('si_staleness_penalty', 0.0), 0.0, 1.0))
        n['borrow_rate_delta_norm']     = normalize(raw['borrow_rate_delta'], 'borrow_rate_delta')
        n['short_volume_ratio']         = float(np.clip(raw.get('short_volume_ratio', 0.0), 0.0, 1.0))
        n['volume_ratio_30d_norm']      = normalize(raw['volume_ratio_30d'], 'volume_ratio_30d')
        n['float_rotation_norm']        = normalize(raw['float_rotation'], 'float_rotation')
        n['price_change_1d']            = float(np.clip(raw.get('price_change_1d', 0.0), -1.0, 1.0))
        n['price_change_5d']            = float(np.clip(raw.get('price_change_5d', 0.0), -1.0, 1.0))
        n['rsi_14_norm']                = normalize(raw['rsi_14'], 'rsi_14')
        n['distance_from_52w_high_norm'] = normalize(raw['distance_from_52w_high'], 'distance_from_52w_high')
        n['atr_norm']                   = float(np.clip(raw.get('atr_norm', 0.0), 0.0, 1.0))
        n['volatility_10d_norm']        = normalize(raw['volatility_10d'], 'volatility_10d')
        n['call_put_ratio_norm']        = normalize(raw['call_put_ratio'], 'call_put_ratio')
        n['iv_rank_norm']               = normalize(raw['iv_rank'], 'iv_rank')
        n['max_pain_distance_norm']     = float(np.clip(raw.get('max_pain_distance', 0.0), -1.0, 1.0))
        n['options_available']          = float(raw.get('options_available', 0.0))
        n['unusual_sweep_flag']         = float(raw.get('unusual_sweep_flag', 0.0))
        n['stocktwits_bull_ratio']      = float(np.clip(raw.get('stocktwits_bull_ratio', 0.0), 0.0, 1.0))
        n['stocktwits_message_count_norm'] = normalize(raw.get('stocktwits_message_count', 0), 'stocktwits_message_count')
        n['google_trend_score_norm']    = normalize(raw.get('google_trend_score', 0.0), 'google_trend_score')
        n['trend_spike_flag']           = float(raw.get('trend_spike_flag', 0))
        n['news_catalyst_flag']         = float(raw.get('news_catalyst_flag', 0))
        n['has_activist_filing']        = float(raw.get('has_activist_filing', 0))

        # News NLP (25-27) — real values now, not zeros
        n['news_sentiment_score']       = normalize(
            raw.get('news_sentiment_score', 0.0), 'news_sentiment_score')
        n['news_squeeze_keyword_score'] = float(np.clip(
            raw.get('news_squeeze_keyword_score', 0.0), 0.0, 1.0))
        n['news_catalyst_composite']    = float(np.clip(
            raw.get('news_catalyst_composite', 0.0), 0.0, 1.0))

        return n

    def close(self):
        self.market_engine.close()
        self.options_engine.close()
        self.si_proxy.close()
        self.news_nlp.close()
        self.pg.close()


# ─────────────────────────────────────────────────────────────
if __name__ == '__main__':
    assembler = FeatureAssembler()

    print('='*55)
    print('  Feature Assembler v2 (News NLP) — Test')
    print('='*55)

    for ticker in ['GME', 'AAPL']:
        print(f'\n── {ticker} ──')
        result = assembler.assemble(ticker)

        print(f'  quality_score:        {result["quality_score"]}%')
        print(f'  news_nlp_available:   {result["news_nlp_available"]}')
        print(f'  news_article_count:   {result["news_article_count"]}')
        print(f'  si_proxy_direction:   {result["si_proxy_direction"]}')
        print(f'  assembly_duration_ms: {result["assembly_duration_ms"]}ms')

        vector = assembler.to_vector(result)
        valid  = assembler.validate_vector(vector)
        print(f'  vector valid: {valid} (shape={vector.shape})')

        print(f'\n  News NLP features (25-27):')
        for i in [25, 26, 27]:
            print(f'    [{i}] {FEATURE_ORDER[i]:35s} = '
                  f'{vector[i]:.4f}')

    assembler.close()
    print('\n✓ Feature assembler v2 test complete')