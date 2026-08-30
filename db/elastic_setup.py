# db/elastic_setup.py
# Run once after Elasticsearch starts

from elasticsearch import Elasticsearch
import os

def setup_elasticsearch():
    es = Elasticsearch(
        os.getenv('ELASTIC_URL', 'http://localhost:9200'))

    # Reddit posts index
    if not es.indices.exists(index='reddit_posts'):
        es.indices.create(
            index='reddit_posts',
            body={
                "mappings": {
                    "properties": {
                        "ticker":           {"type": "keyword"},
                        "subreddit":        {"type": "keyword"},
                        "title": {
                            "type": "text",
                            "analyzer": "english"
                        },
                        "body": {
                            "type": "text",
                            "analyzer": "english"
                        },
                        "sentiment_label":  {"type": "keyword"},
                        "sentiment_score":  {"type": "float"},
                        "keywords":         {"type": "keyword"},
                        "topic_cluster":    {"type": "keyword"},
                        "score":            {"type": "integer"},
                        "created_utc":      {"type": "date"}
                    }
                },
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                }
            }
        )
        print("✓ reddit_posts index created")

    # News articles index
    if not es.indices.exists(index='news_articles'):
        es.indices.create(
            index='news_articles',
            body={
                "mappings": {
                    "properties": {
                        "ticker":           {"type": "keyword"},
                        "source":           {"type": "keyword"},
                        "headline": {
                            "type": "text",
                            "analyzer": "english"
                        },
                        "catalyst_flags":   {"type": "keyword"},
                        "sentiment_score":  {"type": "float"},
                        "published_at":     {"type": "date"}
                    }
                }
            }
        )
        print("✓ news_articles index created")

    print("\n✓ Elasticsearch setup complete")

if __name__ == '__main__':
    setup_elasticsearch()