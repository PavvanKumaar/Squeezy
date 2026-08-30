# tests/test_connections.py
# Run this every morning before starting work
# All 5 must pass before you write any code

import os
import sys
sys.path.append('.')

from dotenv import load_dotenv
load_dotenv()

def test_postgres():
    import psycopg2
    try:
        conn = psycopg2.connect(os.getenv('POSTGRES_URL'))
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM stocks")
        count = cur.fetchone()[0]
        conn.close()
        print(f"  ✓ PostgreSQL — connected, {count} stocks")
        return True
    except Exception as e:
        print(f"  ✗ PostgreSQL — FAILED: {e}")
        return False

def test_mongo():
    import pymongo
    try:
        client = pymongo.MongoClient(
            os.getenv('MONGO_URL'), serverSelectionTimeoutMS=3000)
        client.server_info()
        db = client[os.getenv('MONGO_DB', 'squeezradar')]
        count = db['reddit_posts'].count_documents({})
        client.close()
        print(f"  ✓ MongoDB — connected, {count} Reddit posts")
        return True
    except Exception as e:
        print(f"  ✗ MongoDB — FAILED: {e}")
        return False

def test_redis():
    import redis
    try:
        r = redis.Redis.from_url(
            os.getenv('REDIS_URL', 'redis://localhost:6379'))
        r.ping()
        r.set('squeezradar_test', '1', ex=10)
        val = r.get('squeezradar_test')
        print(f"  ✓ Redis — connected, ping ok")
        return True
    except Exception as e:
        print(f"  ✗ Redis — FAILED: {e}")
        return False

def test_cassandra():
    from cassandra.cluster import Cluster
    try:
        cluster = Cluster([os.getenv(
            'CASSANDRA_HOST', 'localhost')])
        session = cluster.connect('squeezradar')
        rows = session.execute(
            "SELECT table_name FROM system_schema.tables "
            "WHERE keyspace_name = 'squeezradar'"
        )
        tables = [r.table_name for r in rows]
        cluster.shutdown()
        print(f"  ✓ Cassandra — connected, "
              f"tables: {', '.join(tables)}")
        return True
    except Exception as e:
        print(f"  ✗ Cassandra — FAILED: {e}")
        return False

def test_elasticsearch():
    from elasticsearch import Elasticsearch
    try:
        es = Elasticsearch(
            os.getenv('ELASTIC_URL', 'http://localhost:9200'))
        info = es.info()
        indices = list(es.indices.get_alias().keys())
        print(f"  ✓ Elasticsearch — connected, "
              f"indices: {indices}")
        return True
    except Exception as e:
        print(f"  ✗ Elasticsearch — FAILED: {e}")
        return False

if __name__ == '__main__':
    print("\n" + "="*50)
    print("  SqueezeRadar — Connection Tests")
    print("="*50 + "\n")

    results = {
        'PostgreSQL':     test_postgres(),
        'MongoDB':        test_mongo(),
        'Redis':          test_redis(),
        'Cassandra':      test_cassandra(),
        'Elasticsearch':  test_elasticsearch(),
    }

    print("\n" + "="*50)
    passed = sum(results.values())
    total = len(results)
    print(f"  {passed}/{total} connections passing")

    if passed == total:
        print("  ✓ ALL SYSTEMS GO — start coding")
    else:
        failed = [k for k, v in results.items() if not v]
        print(f"  ✗ Fix these first: {', '.join(failed)}")
    print("="*50 + "\n")