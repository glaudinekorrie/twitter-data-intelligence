# config/settings.py
"""
Central configuration management for the Twitter Data Intelligence pipeline
"""
import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

# Load environment variables
env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)


class Settings:
    """Application settings"""
    
    # ========== PROJECT PATHS ==========
    BASE_DIR = Path(__file__).resolve().parent.parent
    DATA_DIR = BASE_DIR / "data"
    DATABASE_DIR = DATA_DIR / "database"
    
    # ========== TWITTER API SETTINGS ==========
    TWITTER_API_KEY = os.getenv("TWITTER_API_KEY", "")
    TWITTER_API_SECRET = os.getenv("TWITTER_API_SECRET", "")
    TWITTER_ACCESS_TOKEN = os.getenv("TWITTER_ACCESS_TOKEN", "")
    TWITTER_ACCESS_TOKEN_SECRET = os.getenv("TWITTER_ACCESS_TOKEN_SECRET", "")
    
    # ========== DATABASE SETTINGS ==========
    DATABASE_TYPE = os.getenv("DATABASE_TYPE", "sqlite")  # sqlite or postgres
    SQLITE_DB_PATH = DATABASE_DIR / "twitter.db"
    POSTGRES_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/twitter")
    
    @property
    def DATABASE_URL(self) -> str:
        """Get database URL based on type"""
        if self.DATABASE_TYPE == "postgres":
            return self.POSTGRES_URL
        else:
            return f"sqlite:///{self.SQLITE_DB_PATH}"
    
    # ========== PIPELINE SETTINGS ==========
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
    RETRY_DELAY = int(os.getenv("RETRY_DELAY", "5"))  # seconds
    
    # ========== SENTIMENT ANALYSIS SETTINGS ==========
    SENTIMENT_THRESHOLD_POSITIVE = float(os.getenv("SENTIMENT_THRESHOLD_POSITIVE", "0.1"))
    SENTIMENT_THRESHOLD_NEGATIVE = float(os.getenv("SENTIMENT_THRESHOLD_NEGATIVE", "-0.1"))
    
    # ========== LOGGING SETTINGS ==========
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE = DATA_DIR / "logs" / "pipeline.log"
    
    # ========== VALIDATION SETTINGS ==========
    MIN_TWEET_LENGTH = int(os.getenv("MIN_TWEET_LENGTH", "3"))
    MAX_TWEET_LENGTH = int(os.getenv("MAX_TWEET_LENGTH", "280"))
    
    # ========== MONITORING SETTINGS ==========
    ALERT_EMAIL = os.getenv("ALERT_EMAIL", "")
    SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
    
    def __init__(self):
        """Initialize and validate settings"""
        self._ensure_directories()
        self._validate_settings()
    
    def _ensure_directories(self):
        """Create necessary directories"""
        directories = [
            self.DATA_DIR,
            self.DATABASE_DIR,
            self.DATA_DIR / "raw",
            self.DATA_DIR / "processed",
            self.DATA_DIR / "logs",
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    def _validate_settings(self):
        """Validate critical settings"""
        if self.TWITTER_API_KEY and len(self.TWITTER_API_KEY) < 10:
            raise ValueError("TWITTER_API_KEY appears invalid")
        
        if self.DATABASE_TYPE not in ["sqlite", "postgres"]:
            raise ValueError(f"Invalid DATABASE_TYPE: {self.DATABASE_TYPE}")
    
    def is_twitter_configured(self) -> bool:
        """Check if Twitter API is configured"""
        return all([
            self.TWITTER_API_KEY,
            self.TWITTER_API_SECRET,
            self.TWITTER_ACCESS_TOKEN,
            self.TWITTER_ACCESS_TOKEN_SECRET
        ])
    
    def get_database_config(self) -> dict:
        """Get database configuration dictionary"""
        return {
            "db_type": self.DATABASE_TYPE,
            "db_path": str(self.SQLITE_DB_PATH) if self.DATABASE_TYPE == "sqlite" else None,
            "db_url": self.DATABASE_URL
        }


# Global settings instance
settings = Settings()