from pipeline.collectors.news_collector import NewsCollector

collector = NewsCollector()
result = collector.collect('GME')
print(f"\nTotal articles: {result['total_found']}")
print(f"Catalyst detected: {result['has_catalyst']}")
print(f"Catalyst flags: {result['catalyst_flags']}")


