# db/mongo_indexes.py
# Run once after MongoDB starts to create indexes

import pymongo
import os
from dotenv import load_dotenv

load_dotenv()

def setup_mongo_indexes():
    client = pymongo.MongoClient(
        os.getenv('MONGO_URL', 'mongodb://localhost:27017'))
    db = client[os.getenv('MONGO_DB', 'squeezradar')]

    # reddit_posts collection
    posts = db['reddit_posts']
    posts.create_index([('ticker', 1), ('created_utc', -1)])
    posts.create_index([('subreddit', 1), ('created_utc', -1)])
    posts.create_index([('nlp_processed', 1)])
    posts.create_index(
        [('created_utc', 1)],
        expireAfterSeconds=7776000  # 90 day TTL
    )
    print("✓ reddit_posts indexes created")

    # news_articles collection
    news = db['news_articles']
    news.create_index([('ticker', 1), ('published_at', -1)])
    news.create_index([('catalyst_flags', 1)])
    news.create_index(
        [('published_at', 1)],
        expireAfterSeconds=2592000  # 30 day TTL
    )
    print("✓ news_articles indexes created")

    # stocktwits collection
    twits = db['stocktwits']
    twits.create_index([('ticker', 1), ('collected_at', -1)])
    print("✓ stocktwits indexes created")

    print("\n✓ All MongoDB indexes ready")
    client.close()

if __name__ == '__main__':
    setup_mongo_indexes()