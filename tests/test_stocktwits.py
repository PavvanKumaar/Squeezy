from pipeline.collectors.stocktwits_collector import StockTwitsCollector

collector = StockTwitsCollector()

print("Testing single ticker:")
result = collector.collect('GME')
print(f"result : {result}")

print("\nTesting trending tickers:")
trending = collector.get_trending()
print(f"  Trending now: {trending[:10]}")