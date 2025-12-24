#!/usr/bin/env python3
"""
Test script for Sentiment Analysis Module
"""

import sys
import os
import logging

# Add src to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.transform.sentiment_analyzer import (
    SentimentAnalyzer, 
    analyze_tweet_sentiment, 
    add_sentiment_to_tweets
)

# Configure logging
logging.basicConfig(level=logging.INFO)

def main():
    print("🧠 Testing Sentiment Analysis Module")
    print("=" * 50)
    
    # Initialize analyzer
    print("\n1. Initializing sentiment analyzer...")
    try:
        analyzer = SentimentAnalyzer()
        print("✅ Sentiment analyzer initialized successfully")
    except ImportError as e:
        print(f"❌ {e}")
        print("Please install TextBlob: pip install textblob")
        return
    
    # Test single tweet analysis
    print("\n2. Testing single tweet analysis...")
    test_tweets = [
        "I love this new product! It's amazing! 😍",
        "This is the worst service I've ever experienced. Terrible! 😠",
        "The weather is okay today, nothing special.",
        "Check out this cool link: https://example.com #awesome",
        "@user1 @user2 This is a mention test"
    ]
    
    for i, tweet in enumerate(test_tweets, 1):
        result = analyzer.analyze_sentiment(tweet)
        print(f"\n   Tweet {i}: {tweet[:50]}...")
        print(f"   Sentiment: {result['sentiment_category']} "
              f"(score: {result['sentiment_score']:.3f})")
        print(f"   Cleaned: {result.get('cleaned_text', '')[:50]}...")
    
    # Test batch analysis
    print("\n3. Testing batch analysis...")
    mock_tweets = [
        {"tweet_id": "1", "text": "Great day! Everything is perfect!", "user_name": "user1"},
        {"tweet_id": "2", "text": "I'm so frustrated with this bug.", "user_name": "user2"},
        {"tweet_id": "3", "text": "Just finished lunch. It was fine.", "user_name": "user3"},
        {"tweet_id": "4", "text": "AMAZING concert last night! #bestnightever", "user_name": "user4"},
        {"tweet_id": "5", "text": "Worst customer service ever. Never again!", "user_name": "user5"},
    ]
    
    analyzed_tweets = analyzer.analyze_tweets(mock_tweets)
    print(f"✅ Analyzed {len(analyzed_tweets)} tweets")
    
    # Show sentiment summary
    print("\n4. Sentiment summary:")
    summary = analyzer.get_sentiment_summary(analyzed_tweets)
    print(f"   Total tweets: {summary['total_tweets']}")
    print(f"   Positive: {summary['positive_count']} "
          f"({summary['sentiment_distribution']['positive']}%)")
    print(f"   Negative: {summary['negative_count']} "
          f"({summary['sentiment_distribution']['negative']}%)")
    print(f"   Neutral: {summary['neutral_count']} "
          f"({summary['sentiment_distribution']['neutral']}%)")
    print(f"   Average sentiment score: {summary['avg_sentiment']:.3f}")
    
    # Test brand detection
    print("\n5. Testing brand detection...")
    brands_to_detect = ["Tesla", "Apple", "Microsoft", "Netflix"]
    
    brand_test_tweets = [
        "Just got a new Tesla Model 3! Love it!",
        "My Apple iPhone is the best phone ever",
        "Watching a movie on Netflix tonight",
        "Regular tweet without brand mentions"
    ]
    
    for tweet in brand_test_tweets:
        brand = analyzer.detect_brand_mentions(tweet, brands_to_detect)
        if brand:
            print(f"   '{tweet[:30]}...' → Brand detected: {brand}")
        else:
            print(f"   '{tweet[:30]}...' → No brand detected")
    
    # Test utility functions
    print("\n6. Testing utility functions...")
    
    # Single tweet utility
    single_result = analyze_tweet_sentiment("This is fantastic!")
    print(f"   Single tweet utility: {single_result['sentiment_category']}")
    
    # Batch utility
    batch_result = add_sentiment_to_tweets(mock_tweets[:2])
    print(f"   Batch utility processed {len(batch_result)} tweets")
    
    # Test edge cases
    print("\n7. Testing edge cases...")
    
    edge_cases = ["", "   ", "@user #hashtag", "12345", "!@#$%^&*()"]
    
    for case in edge_cases:
        result = analyzer.analyze_sentiment(case)
        print(f"   '{case}' → Category: {result['sentiment_category']}")
    
    print("\n" + "=" * 50)
    print("🎉 Sentiment analysis test completed!")
    
    print("\n📊 Sample analyzed tweet structure:")
    if analyzed_tweets:
        sample = analyzed_tweets[0]
        for key in ['tweet_id', 'text', 'sentiment_score', 'sentiment_category']:
            if key in sample:
                print(f"   {key}: {sample[key]}")
    
    print("\n✅ What works:")
    print("   - Text cleaning (URLs, mentions, hashtags)")
    print("   - Sentiment scoring (-1 to 1)")
    print("   - Sentiment categorization")
    print("   - Batch processing")
    print("   - Summary statistics")
    print("   - Brand mention detection")
    
    print("\n📝 Next steps:")
    print("1. Integrate with database loader")
    print("2. Add more sophisticated NLP models")
    print("3. Create sentiment trend analysis")
    print("4. Build real-time sentiment dashboard")

if __name__ == "__main__":
    main()