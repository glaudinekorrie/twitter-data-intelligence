"""
Sentiment analyzer for Twitter data
"""

from textblob import TextBlob


class SentimentAnalyzer:
    """Analyze sentiment of text"""
    
    def __init__(self, thresholds=None):
        """
        Initialize sentiment analyzer
        
        Args:
            thresholds: Custom sentiment thresholds
        """
        self.thresholds = thresholds or {
            'positive': 0.1,
            'negative': -0.1
        }
    
    def analyze_sentiment(self, text):
        """
        Analyze sentiment of a single text
        
        Args:
            text: Text to analyze
            
        Returns:
            Dictionary with sentiment metrics
        """
        if not text or not isinstance(text, str):
            return {
                'sentiment_score': 0.0,
                'sentiment_polarity': 0.0,
                'sentiment_subjectivity': 0.0,
                'sentiment_category': 'neutral',
                'confidence': 0.0
            }
        
        try:
            # Clean text (basic cleaning)
            cleaned = text.strip()
            
            # Analyze with TextBlob
            blob = TextBlob(cleaned)
            polarity = blob.sentiment.polarity  # -1 to 1
            subjectivity = blob.sentiment.subjectivity  # 0 to 1
            
            # Categorize sentiment
            if polarity > self.thresholds['positive']:
                category = 'positive'
            elif polarity < self.thresholds['negative']:
                category = 'negative'
            else:
                category = 'neutral'
            
            return {
                'sentiment_score': polarity,
                'sentiment_polarity': polarity,
                'sentiment_subjectivity': subjectivity,
                'sentiment_category': category,
                'confidence': abs(polarity) if category != 'neutral' else 1.0 - abs(polarity),
                'cleaned_text': cleaned[:100]  # First 100 chars
            }
            
        except Exception:
            # Return neutral on any error
            return {
                'sentiment_score': 0.0,
                'sentiment_polarity': 0.0,
                'sentiment_subjectivity': 0.0,
                'sentiment_category': 'neutral',
                'confidence': 0.0,
                'cleaned_text': ''
            }
    
    def analyze_tweets(self, tweets):
        """
        Analyze sentiment for multiple tweets
        
        Args:
            tweets: List of tweet dictionaries
            
        Returns:
            List of tweets with added sentiment analysis
        """
        if not tweets:
            return []
        
        analyzed_tweets = []
        for tweet in tweets:
            # Create a copy
            analyzed = tweet.copy()
            
            # Get text
            text = tweet.get('text') or tweet.get('content', '')
            
            # Analyze sentiment
            sentiment = self.analyze_sentiment(text)
            
            # Add sentiment fields
            analyzed.update({
                'sentiment_score': sentiment['sentiment_score'],
                'sentiment_polarity': sentiment['sentiment_polarity'],
                'sentiment_subjectivity': sentiment['sentiment_subjectivity'],
                'sentiment_category': sentiment['sentiment_category'],
                'sentiment_confidence': sentiment['confidence']
            })
            
            analyzed_tweets.append(analyzed)
        
        return analyzed_tweets


# Utility function for easy import
def analyze_tweet_sentiment(text):
    """Quick sentiment analysis"""
    analyzer = SentimentAnalyzer()
    return analyzer.analyze_sentiment(text)