import os
import sys
import feedparser
import pandas as pd
import sqlite3
from datetime import datetime
import time
from langdetect import detect, DetectorFactory
import logging
import re
from urllib.error import URLError
import ssl

# Fix for SSL issues in some environments
try:
    _create_unverified_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_context

# Set seed for language detection reproducibility
DetectorFactory.seed = 0

# Configure logging
logging.basicConfig(filename='news_scraper.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# List of RSS feeds from 20+ countries
RSS_FEEDS = [
    {'country': 'United Kingdom', 'source': 'BBC News', 'url': 'http://feeds.bbci.co.uk/news/rss.xml'},
    {'country': 'United States', 'source': 'CNN', 'url': 'http://rss.cnn.com/rss/edition.rss'},
    {'country': 'Qatar', 'source': 'Al Jazeera', 'url': 'https://www.aljazeera.com/xml/rss/all.xml'},
    {'country': 'Japan', 'source': 'NHK', 'url': 'https://www3.nhk.or.jp/rss/news/cat0.xml'},
    {'country': 'India', 'source': 'The Times of India', 'url': 'https://timesofindia.indiatimes.com/rssfeedstopstories.cms'},
    {'country': 'Singapore', 'source': 'The Straits Times', 'url': 'https://www.straitstimes.com/news/rss'},
    {'country': 'Malaysia', 'source': 'The Star', 'url': 'https://www.thestar.com.my/rss'},
    {'country': 'Indonesia', 'source': 'Jakarta Post', 'url': 'https://www.thejakartapost.com/feed'},
    {'country': 'South Korea', 'source': 'Yonhap News', 'url': 'https://en.yna.co.kr/rss'},
    {'country': 'China', 'source': 'China Daily', 'url': 'http://www.chinadaily.com.cn/rss/world_rss.xml'},
    {'country': 'Australia', 'source': 'ABC News', 'url': 'https://www.abc.net.au/news/feed'},
    {'country': 'Germany', 'source': 'Deutsche Welle', 'url': 'https://rss.dw.com/xml/rss-en-world'},
    {'country': 'France', 'source': 'France 24', 'url': 'https://www.france24.com/en/rss'},
    {'country': 'Brazil', 'source': 'Folha de S.Paulo', 'url': 'https://www1.folha.uol.com.br/internacional/en/rss091.xml'},
    {'country': 'South Africa', 'source': 'News24', 'url': 'https://www.news24.com/news24/rss'},
    {'country': 'Nigeria', 'source': 'Punch Nigeria', 'url': 'https://punchng.com/feed/'},
    {'country': 'Canada', 'source': 'CBC News', 'url': 'https://www.cbc.ca/webfeed/rss/rss-topstories'},
    {'country': 'Russia', 'source': 'TASS', 'url': 'https://tass.com/rss/v2.xml'},
    {'country': 'Mexico', 'source': 'El Universal', 'url': 'https://www.eluniversal.com.mx/rss'},
    {'country': 'Egypt', 'source': 'Ahram Online', 'url': 'http://english.ahram.org.eg/RSS.aspx'},
    {'country': 'Turkey', 'source': 'Hurriyet Daily News', 'url': 'https://www.hurriyetdailynews.com/rss'},
    {'country': 'Pakistan', 'source': 'Dawn', 'url': 'https://www.dawn.com/feeds/home'},
    {'country': 'Argentina', 'source': 'Clarin', 'url': 'https://www.clarin.com/rss.html'},
    {'country': 'Thailand', 'source': 'Bangkok Post', 'url': 'https://www.bangkokpost.com/rss'},
    {'country': 'Kenya', 'source': 'The Standard', 'url': 'https://www.standardmedia.co.ke/rss'},
]

def clean_text(text):
    """Clean text by removing excessive whitespace and special characters."""
    if not text:
        return ''
    text = re.sub(r'\s+', ' ', text.strip())
    text = re.sub(r'[^\x00-\x7F]+', ' ', text)  # Remove non-ASCII for simplicity
    return text

def detect_language(text):
    """Detect the language of the given text."""
    try:
        return detect(text)
    except:
        return 'unknown'

def parse_rss_feed(feed_info):
    """Parse a single RSS feed and extract news items."""
    news_items = []
    try:
        feed = feedparser.parse(feed_info['url'])
        if feed.bozo:
            logging.error(f"Error parsing feed {feed_info['source']} ({feed_info['country']}): {feed.bozo_exception}")
            return news_items

        for entry in feed.entries:
            title = clean_text(entry.get('title', 'No Title'))
            description = clean_text(entry.get('summary', entry.get('description', 'No Description')))
            pub_date = entry.get('published', entry.get('updated', 'Unknown'))
            try:
                pub_date = datetime.strptime(pub_date, '%a, %d %b %Y %H:%M:%S %z').strftime('%Y-%m-%d %H:%M:%S') if pub_date != 'Unknown' else 'Unknown'
            except (ValueError, TypeError):
                pub_date = 'Unknown'
            link = entry.get('link', 'No URL')
            language = detect_language(title + ' ' + description)

            news_items.append({
                'country': feed_info['country'],
                'source': feed_info['source'],
                'title': title,
                'pub_date': pub_date,
                'description': description,
                'url': link,
                'language': language
            })

        logging.info(f"Successfully parsed {len(news_items)} items from {feed_info['source']} ({feed_info['country']})")
    except URLError as e:
        logging.error(f"Network error for {feed_info['source']} ({feed_info['country']}): {e}")
    except Exception as e:
        logging.error(f"Unexpected error for {feed_info['source']} ({feed_info['country']}): {e}")
    return news_items

def save_to_csv(data, filename='news_data.csv'):
    """Save news data to CSV file."""
    df = pd.DataFrame(data)
    df.drop_duplicates(subset=['title', 'url'], keep='first', inplace=True)
    df.to_csv(filename, index=False, encoding='utf-8')
    logging.info(f"Saved {len(df)} news items to {filename}")
    return len(df)

def save_to_sqlite(data, db_name='news_data.db'):
    """Save news data to SQLite database."""
    conn = sqlite3.connect(db_name)
    df = pd.DataFrame(data)
    df.drop_duplicates(subset=['title', 'url'], keep='first', inplace=True)
    df.to_sql('news', conn, if_exists='append', index=False)
    conn.commit()
    conn.close()
    logging.info(f"Saved {len(df)} news items to {db_name}")
    return len(df)

def generate_summary(data):
    """Generate summary of collected data."""
    df = pd.DataFrame(data)
    summary = df.groupby(['country', 'source']).size().reset_index(name='total_articles')
    summary['historical_data'] = 'Varies by feed (up to 1 year where available)'
    return summary.to_dict('records')

def main():
    """Main function to orchestrate news scraping."""
    all_news = []
    for feed in RSS_FEEDS:
        logging.info(f"Processing feed: {feed['source']} ({feed['country']})")
        news_items = parse_rss_feed(feed)
        all_news.extend(news_items)
        time.sleep(1)  # Respect rate limits

    # Save to CSV and SQLite
    total_csv = save_to_csv(all_news)
    total_db = save_to_sqlite(all_news)

    # Generate summary
    summary = generate_summary(all_news)
    logging.info(f"Summary: {summary}")

    # Save summary to CSV
    pd.DataFrame(summary).to_csv('news_summary.csv', index=False, encoding='utf-8')
    logging.info("Summary saved to news_summary.csv")

    print(f"Scraped {len(all_news)} news items from {len(RSS_FEEDS)} feeds.")
    print(f"Saved {total_csv} unique items to CSV and {total_db} to SQLite.")
    print("Summary of collected data:")
    for item in summary:
        print(f"Country: {item['country']}, Source: {item['source']}, Articles: {item['total_articles']}, Historical Data: {item['historical_data']}")

if __name__ == "__main__":
    main()