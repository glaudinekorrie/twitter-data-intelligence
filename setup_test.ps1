# setup_test.ps1 - Fixed version
Write-Host "🧪 Testing the setup..." -ForegroundColor Yellow

# Create a separate Python file for testing
$testPython = @"
import sys
sys.path.append('.')

try:
    from config.settings import settings
    print('✅ Configuration loaded successfully')
    print(f'   Database: {settings.DATABASE_TYPE}')
    print(f'   Data dir: {settings.DATA_DIR}')
    
    from src.transform.sentiment_analyzer import SentimentAnalyzer
    print('✅ Sentiment analyzer imported')
    
    analyzer = SentimentAnalyzer()
    result = analyzer.analyze_sentiment("Test tweet for setup verification")
    print(f'✅ Sentiment test: {result["sentiment_category"]}')
    
    print('\\n🎉 Setup completed successfully!')
    
except Exception as e:
    print(f'❌ Setup error: {e}')
    import traceback
    traceback.print_exc()
"@

$testPython | Out-File -FilePath "test_setup.py" -Encoding UTF8
python test_setup.py
Remove-Item test_setup.py -ErrorAction SilentlyContinue
