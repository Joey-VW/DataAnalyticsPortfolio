import argparse
import os
from scrapeX.scrapeX import ScrapeX  

def parse_args():
    parser = argparse.ArgumentParser(
        description='Twitter Scraper CLI - Scrape Twitter data based on search parameters'
    )
    
    # Required arguments
    parser.add_argument(
        '--username',
        '-u',
        type=str,
        default=os.environ.get('xun'),
        help='Twitter username (can also be set via xun environment variable)'
    )
    
    parser.add_argument(
        '--password',
        '-p',
        type=str,
        default=os.environ.get('xpw'),
        help='Twitter password (can also be set via xpw environment variable)'
    )
    
    parser.add_argument(
        '--target-url',
        '-t',
        type=str,
        default="https://x.com/search?q=(Elon%20OR%20Musk)%20lang%3Aen&src=typed_query",
        help='Target Twitter search URL'
    )
    
    # Optional arguments
    parser.add_argument(
        '--time-limit',
        '-l',
        type=str,
        default="00:20:00",
        help='Time limit for scraping in HH:MM:SS format'
    )
    
    parser.add_argument(
        '--existing-posts',
        '-e',
        type=str,
        default='post_data_20250324_1043.json',
        help='Path to existing posts JSON file'
    )
    
    parser.add_argument(
        '--scrape-engagements',
        '-s',
        action='store_true',
        help='Enable scraping of engagements (likes, retweets, etc.)'
    )
    
    parser.add_argument(
        '--no-headless',
        '-n',
        action='store_false',
        dest='headless',
        help='Run browser in non-headless mode (visible window)'
    )

    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    
    # Validate required arguments
    if not args.username or not args.password:
        raise ValueError("Username and password must be provided either via arguments or environment variables")
    
    # Initialize and run the scraper
    scraper = ScrapeX(
        username=args.username,
        password=args.password,
        target_url=args.target_url,
        time_limit=args.time_limit,
        existing_posts_path=args.existing_posts,
        do_scrape_engagements=args.scrape_engagements,
        headless=args.headless
    )
    scraper.run()