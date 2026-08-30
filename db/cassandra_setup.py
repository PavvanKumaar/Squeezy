# db/cassandra_setup.py
# Run once after Cassandra starts

from cassandra.cluster import Cluster
import os

def setup_cassandra():
    cluster = Cluster([os.getenv('CASSANDRA_HOST', 'localhost')])
    session = cluster.connect()

    # Create keyspace
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS squeezradar
        WITH replication = {
            'class': 'SimpleStrategy',
            'replication_factor': 1
        }
    """)
    session.set_keyspace('squeezradar')
    print("✓ Cassandra keyspace created")

    # Market tick data (high write volume)
    session.execute("""
        CREATE TABLE IF NOT EXISTS market_ticks (
            ticker      TEXT,
            date        DATE,
            ts          TIMESTAMP,
            open        DOUBLE,
            high        DOUBLE,
            low         DOUBLE,
            close       DOUBLE,
            volume      BIGINT,
            interval    TEXT,
            PRIMARY KEY ((ticker, date), ts)
        ) WITH CLUSTERING ORDER BY (ts DESC)
          AND default_time_to_live = 7776000
    """)
    print("✓ market_ticks table created")

    # Borrow rate history (updates 3x/day)
    session.execute("""
        CREATE TABLE IF NOT EXISTS borrow_rate_history (
            ticker          TEXT,
            date            DATE,
            ts              TIMESTAMP,
            borrow_rate     DOUBLE,
            available_shares BIGINT,
            source          TEXT,
            PRIMARY KEY ((ticker, date), ts)
        ) WITH CLUSTERING ORDER BY (ts DESC)
    """)
    print("✓ borrow_rate_history table created")

    # Short volume daily
    session.execute("""
        CREATE TABLE IF NOT EXISTS short_volume_daily (
            ticker          TEXT,
            date            DATE,
            short_volume    BIGINT,
            total_volume    BIGINT,
            short_ratio     DOUBLE,
            source          TEXT,
            PRIMARY KEY (ticker, date)
        ) WITH CLUSTERING ORDER BY (date DESC)
    """)
    print("✓ short_volume_daily table created")

    print("\n✓ Cassandra setup complete")
    cluster.shutdown()

if __name__ == '__main__':
    setup_cassandra()