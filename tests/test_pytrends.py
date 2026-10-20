import sys
sys.path.append('.')

import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()

from pipeline.collectors.pytrends_collector import PyTrendsCollector

collector = PyTrendsCollector()

    # Test one at a time — pytrends rate limits fast
result = collector.collect('GME')
print(f"\nResult: {result}")