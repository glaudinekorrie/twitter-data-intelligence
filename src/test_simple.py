print("Testing from INSIDE src folder...")

try:
    # Direct import (no src prefix needed)
    from transform.sentiment_analyzer import SentimentAnalyzer
    print("✅ Import successful!")
    
    analyzer = SentimentAnalyzer()
    result = analyzer.analyze_sentiment("Testing from src folder")
    
    print(f"✅ Analysis works!")
    print(f"Sentiment: {result['sentiment_category']}")
    print(f"Score: {result['sentiment_score']:.3f}")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
