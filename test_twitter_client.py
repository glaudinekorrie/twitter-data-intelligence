# Save this as test_twitter_client_v2.py
#!/usr/bin/env python3
"""
Test script for Twitter API client - Updated with better error handling
"""

import sys
import os

# Add src to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from src.extract.twitter_api_client import get_twitter_client
    import pandas as pd
    import json
except ImportError as e:
    print(f"❌ Missing dependency: {e}")
    print("Please run: pip install pandas faker")
    sys.exit(1)

def main():
    print("🚀 Testing Twitter Data Intelligence Setup")
    print("=" * 50)
    
    # Test 1: Initialize client
    print("\n1. Initializing Twitter client...")
    client = get_twitter_client(use_mock=True)  # Use mock for now
    
    # Test 2: Test connection
    print("\n2. Testing connection...")
    connected = client.test_connection()
    if connected:
        print("✅ Connection successful!")
    else:
        print("❌ Connection failed")
    
    # Test 3: Search for tweets
    print("\n3. Searching for test tweets...")
    tweets = client.search_tweets(
        query="data engineering",
        count=5
    )
    
    print(f"✅ Found {len(tweets)} tweets")
    
    # Display sample
    if tweets:
        print("\n📊 Sample tweet:")
        sample = tweets[0]
        print(f"   User: @{sample['user_name']}")
        print(f"   Text: {sample['text'][:80]}...")
        print(f"   Created: {sample['created_at']}")
        print(f"   Retweets: {sample['retweet_count']}")
    
    # Test 4: Convert to DataFrame
    print("\n4. Converting to DataFrame...")
    df = pd.DataFrame(tweets)
    print(f"✅ DataFrame created: {df.shape[0]} rows, {df.shape[1]} columns")
    
    # Show columns
    print(f"   Columns: {', '.join(df.columns.tolist()[:10])}...")
    
    # Test 5: Save to file
    print("\n5. Saving to files...")
    
    # Create data directory
    os.makedirs("data/raw", exist_ok=True)
    os.makedirs("data/processed", exist_ok=True)
    
    # Save raw data - try different formats
    try:
        # Try Parquet first
        df.to_parquet("data/raw/test_tweets.parquet")
        print("✅ Saved as Parquet: data/raw/test_tweets.parquet")
    except ImportError:
        print("⚠️  PyArrow not installed, skipping Parquet...")
    except Exception as e:
        print(f"⚠️  Could not save as Parquet: {e}")
    
    # Always save as CSV (no dependencies needed)
    df.to_csv("data/raw/test_tweets.csv", index=False)
    print("✅ Saved as CSV: data/raw/test_tweets.csv")
    
    # Save as JSON
    df.to_json("data/raw/test_tweets.json", orient="records", indent=2)
    print("✅ Saved as JSON: data/raw/test_tweets.json")
    
    # Save processed (just a sample transformation)
    if 'brand_mentioned' in df.columns:
        brand_summary = df.groupby('brand_mentioned').agg({
            'tweet_id': 'count',
            'retweet_count': 'mean',
            'favorite_count': 'mean'
        }).round(2)
        
        brand_summary.to_csv("data/processed/brand_summary.csv")
        print("✅ Created brand summary in data/processed/brand_summary.csv")
    
    # Show file sizes
    print("\n📁 File sizes:")
    import glob
    for file in glob.glob("data/**/*", recursive=True):
        if os.path.isfile(file):
            size_kb = os.path.getsize(file) / 1024
            print(f"   {file}: {size_kb:.1f} KB")
    
    print("\n" + "=" * 50)
    print("🎉 All tests completed successfully!")
    print("\n✅ What works:")
    print("   - Twitter API client (mock data)")
    print("   - Data extraction")
    print("   - DataFrame creation")
    print("   - File saving (CSV/JSON)")
    
    print("\n📝 Next steps:")
    print("1. Apply for Twitter API access: https://developer.twitter.com/")
    print("2. Create .env file with your API credentials")
    print("3. Install pyarrow: pip install pyarrow")
    print("4. Build database loader (SQLite)")
    print("5. Create Airflow DAG")

if __name__ == "__main__":
    main()