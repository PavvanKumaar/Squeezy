# pipeline/scoring/store.py
#
# Keeps the FULL grouped payload per run in a NEW table (scoring_payloads), so
# no existing table has to change. stock_scores (written by save_score) keeps
# only the fixed columns; this table is where per-feature panels live for
# history and for the API/dashboard later.

import json


class PayloadStore:
    DDL = """
        CREATE TABLE IF NOT EXISTS scoring_payloads (
            time          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            ticker        TEXT        NOT NULL,
            squeeze_score DOUBLE PRECISION,
            phase_label   TEXT,
            payload       JSONB       NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_scoring_payloads_ticker_time
            ON scoring_payloads (ticker, time DESC);
    """

    def __init__(self, pg):
        self.pg = pg
        self.ready = False
        self.ensure()

    def ensure(self):
        try:
            cur = self.pg.cursor()
            cur.execute(self.DDL)
            self.pg.commit()
            self.ready = True
        except Exception as e:
            self.pg.rollback()
            print(f'  [store] could not create scoring_payloads: {e}')

    def save(self, ticker, payload):
        if not self.ready:
            return False
        try:
            cur = self.pg.cursor()
            cur.execute("""
                INSERT INTO scoring_payloads
                    (time, ticker, squeeze_score, phase_label, payload)
                VALUES (NOW(), %s, %s, %s, %s)
            """, (ticker, payload.get('squeeze_score'),
                  payload.get('phase_label'), json.dumps(payload, default=str)))
            self.pg.commit()
            return True
        except Exception as e:
            self.pg.rollback()
            print(f'  [store] write failed for {ticker}: {e}')
            return False

    def latest(self, ticker):
        cur = self.pg.cursor()
        try:
            cur.execute("""SELECT payload FROM scoring_payloads
                           WHERE ticker = %s ORDER BY time DESC LIMIT 1""", (ticker,))
            row = cur.fetchone()
        except Exception:
            self.pg.rollback()
            return None
        if not row:
            return None
        return row[0] if isinstance(row[0], dict) else json.loads(row[0])
