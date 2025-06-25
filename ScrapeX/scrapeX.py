#scrapeX.py
import json
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException, NoSuchElementException, ElementClickInterceptedException
import threading
import keyboard
import logging


# This code defines a `ScrapeX` class that is used to scrape tweets from X. 
# The class has methods to: 
    # initialize a "scraper" with necessary parameters, 
    # set up a WebDriver for scraping, 
    # log in to X, 
    # load target URL, 
    # scrape posts, 
    # extract data from tweets, 
    # scrape engagements for each tweet, 
    # handle errors during scraping, 
    # wait for elements to appear, 
    # calculate remaining time, 
    # clean strings, 
    # convert stats strings to a dictionary, 
    # generate a timestamped filename for the output JSON file, 
    # save scraped posts to ^ JSON file, 
    # listen for an abort signal to stop scraping, 
    # and execute the full scraping process.
class ScrapeX:
    # Constants for selectors
    TWEET_SELECTOR = '[data-testid="tweet"]'
    USERNAME_SELECTOR = '[data-testid="User-Name"]'
    TWEET_TEXT_SELECTOR = '[data-testid="tweetText"]'
    CARET_SELECTOR = '[role="button"][data-testid="caret"]'
    ENGAGEMENT_MENU_SELECTOR = '[role="menuitem"][data-testid="tweetEngagements"]'

    def __init__(self, username, password, target_url, time_limit, existing_posts_path=None, do_scrape_engagements=False, headless=False):
        """
        Initialize the scraper with necessary parameters.

        Args:
            username (str): X username.
            password (str): X password.
            target_url (str): URL to scrape tweets from (e.g., a search query).
            time_limit (str): Time limit for scraping in 'HH:MM:SS' format.
            existing_posts_path (str, optional): Path to JSON file with existing posts for duplicate checking.
            do_scrape_engagements (bool): Whether to scrape engagements for each tweet.
            headless (bool): Whether to run the webdriver in headless mode. Defaults to False.
        """
        self.username = username
        self.password = password
        self.target_url = target_url
        self.time_limit_seconds = self._parse_time_limit(time_limit)
        self.existing_posts_path = existing_posts_path
        self.do_scrape_engagements = do_scrape_engagements
        self.headless = headless
        self.driver = None
        self.existing_posts_set = set()  # For duplicate checking
        self.existing_posts_list = []    # To retain full data for output
        self.scraped_posts = []
        self.start_time = None
        self.wrap_up_time = None
        self.stop_scraping = False

    def _parse_time_limit(self, time_limit_str):
        """Convert time limit string (HH:MM:SS) to seconds."""
        hours, minutes, seconds = map(int, time_limit_str.split(':'))
        return hours * 3600 + minutes * 60 + seconds

    def _setup_driver(self):
        """Set up the Firefox WebDriver with optional headless mode."""
        service = Service(GeckoDriverManager().install())
        options = FirefoxOptions()
        if self.headless:
            options.add_argument("--headless")  # Enable headless mode if requested
        self.driver = webdriver.Firefox(service=service, options=options)
        if not self.headless:
            self.driver.maximize_window()  # Maximize only if not headless

    def stop(self):
        """Signal the scraper to stop and save the collected data."""
        self.stop_scraping = True

    def _login(self):
        """Log in to X with retry mechanism."""
        for attempt in range(3):
            self.driver.get('https://twitter.com/login')
            username_box = self._wait_on_element(5, By.CSS_SELECTOR, '[autocomplete="username"]')
            if username_box == 'error':
                continue
            username_box.send_keys(self.username)
            next_button = next((b for b in self.driver.find_elements(By.TAG_NAME, 'button') if b.text == 'Next'), None)
            if next_button:
                next_button.click()
            password_box = self._wait_on_element(5, By.CSS_SELECTOR, '[name="password"][type="password"]')
            if password_box == 'error':
                continue
            password_box.send_keys(self.password)
            login_button = next((b for b in self.driver.find_elements(By.TAG_NAME, 'button') if b.text == 'Log in'), None)
            if login_button:
                login_button.click()
            home_link = self._wait_on_element(5, By.CSS_SELECTOR, '[data-testid="AppTabBar_Home_Link"]')
            if home_link != 'error':
                logging.info("Logged in successfully.")
                return True
        logging.warning("Failed to log in after 3 attempts.")
        return False

    def _load_target(self):
        """Navigate to the target URL and wait for tweets to load."""
        self.driver.get(self.target_url)
        self._wait_on_elements(10, By.CSS_SELECTOR, self.TWEET_SELECTOR)
        logging.info(f"Loaded target URL: {self.target_url}")

    def _load_existing_posts(self):
        """Load existing posts from JSON file for duplicate checking and inclusion in output."""
        if not self.existing_posts_path:
            return
        try:
            with open(self.existing_posts_path, 'r') as f:
                tweets = json.load(f)
                self.existing_posts_list = tweets  # Keep the full list of existing posts
                for tweet in tweets:
                    tweet_tuple = (tweet['date_time'], tweet['profile_name'], tweet['tweet_text'])
                    self.existing_posts_set.add(tweet_tuple)  # Add to set for duplicate checking
            print(f"Loaded {len(self.existing_posts_set)} existing posts.")
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Error loading existing posts: {e}")
            self.existing_posts_list = []  # Reset to empty list on error

    def _scrape_posts(self):
        """Main scraping loop."""
        self._load_existing_posts()
        self.start_time = time.time()
        self.wrap_up_time = self.start_time + self.time_limit_seconds
        total_posts, tries, duplicates, stagnation_counter = 0, 0, 0, 0
        last_known_post_count = 0
        logging.info("Starting scraping.")

        try:
            while time.time() < self.wrap_up_time and not self.stop_scraping:
                print(f"Time Remaining: {self._get_remaining_time()}")
                print("Fetching posts...")

                # Fetch current visible tweets
                tweets = self._wait_on_elements(3, By.CSS_SELECTOR, self.TWEET_SELECTOR)
                if tweets == 'error':
                    action = self._handle_error()
                    if action == 'Retry':
                        continue
                else:
                    print(f"Found {len(tweets)} posts.")
                    # Process tweets dynamically without assuming a fixed range
                    current_tweets = self.driver.find_elements(By.CSS_SELECTOR, self.TWEET_SELECTOR)
                    for tweet in current_tweets:
                        tries += 1
                        try:
                            post_data = self._get_post_data(tweet)
                            if post_data:
                                post_tuple = (post_data['date_time'], post_data['profile_name'], post_data['tweet_text'])
                                if post_tuple not in self.existing_posts_set:
                                    self.existing_posts_set.add(post_tuple)
                                    print(f"{total_posts + 1}. {post_data['date_time']} - {post_data['profile_name']} - {post_data['tweet_text']}")
                                    if self.do_scrape_engagements:
                                        post_data['engagements'] = self._get_engagements(tweet)
                                    self.scraped_posts.append(post_data)
                                    total_posts += 1
                                else:
                                    duplicates += 1
                            else:
                                print(f"Failed to scrape post {tries}.")
                        except StaleElementReferenceException:
                            print(f"Stale element at attempt {tries}. Skipping...")
                            continue
                        except Exception as e:
                            print(f"Unexpected error in post scraping: {e}")
                            self.save_posts(filename=f'data_partial_{int(time.time())}.json')  # Save partial data
                            raise  # Re-raise to exit loop but after saving

                # Stagnation check
                if total_posts > last_known_post_count:
                    last_known_post_count = total_posts
                    stagnation_counter = 0
                else:
                    stagnation_counter += 1
                    if stagnation_counter >= 20:
                        print("No new posts for 20 attempts. Aborting...")
                        logging.info("Aborted due to stagnation: no new posts for 20 attempts.")
                        break

                # Scroll and wait for DOM to stabilize
                self.driver.execute_script('window.scrollBy(0, window.innerHeight);')
                time.sleep(1.5)  # Allow DOM to update after scroll
                print(f"Posts: {total_posts} | Tries: {tries} | Duplicates: {duplicates}")

            print(f"Scraped {total_posts} posts | Tries: {tries} | Duplicates: {duplicates}")
            logging.info(f"Scraping finished. Scraped {total_posts} posts | Tries: {tries} | Duplicates: {duplicates}")
        except Exception as e:
            print(f"Error in _scrape_posts: {e}")
            logging.error(f"Error in _scrape_posts: {e}")
            self.save_posts(filename=f'data_partial_{int(time.time())}.json')  # Save before exiting
            raise  # Re-raise to propagate the error up to run()

    def _get_post_data(self, tweet):
        """Extract data from a tweet element with retry mechanism."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                username_elem = tweet.find_element(By.CSS_SELECTOR, self.USERNAME_SELECTOR)
                profile_name = username_elem.find_element(By.TAG_NAME, 'span').find_element(By.TAG_NAME, 'span').text
                date_time = tweet.find_element(By.TAG_NAME, 'time').get_attribute('datetime')
                tweet_text = self._clean_string(tweet.find_element(By.CSS_SELECTOR, self.TWEET_TEXT_SELECTOR).text)
                stats = tweet.find_element(By.CSS_SELECTOR, '[role="group"]').get_attribute('aria-label')
                stat_dict = self._stats_to_dict(stats) if stats else {}
                return {
                    'date_time': date_time,
                    'profile_name': profile_name,
                    'tweet_text': tweet_text,
                    'stats': stat_dict
                }
            except StaleElementReferenceException:
                if attempt < max_retries - 1:
                    print(f"Stale element in _get_post_data, retrying ({attempt + 1}/{max_retries})...")
                    time.sleep(0.5)
                    continue
                print("Max retries reached for stale element. Skipping this tweet.")
                return None
            except (NoSuchElementException, TimeoutException) as e:
                print(f"Error extracting tweet data: {e}")
                return None

    def _get_engagements(self, tweet):
        """Scrape engagements for a tweet."""
        engagements = []
        print("Fetching engagements...")
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Wait for the caret button to be clickable
                menu = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, self.CARET_SELECTOR))
                )
                # Use JavaScript click as a fallback if regular click fails
                try:
                    menu.click()
                except ElementClickInterceptedException:
                    self.driver.execute_script("arguments[0].click();", menu)
                
                menu_item = self._wait_on_element(5, By.CSS_SELECTOR, self.ENGAGEMENT_MENU_SELECTOR)
                href = menu_item.get_attribute('href')
                self.driver.execute_script("window.open('');")
                self.driver.switch_to.window(self.driver.window_handles[1])
                self.driver.get(href)

                for _ in range(5):  # Scroll limit
                    eng_tweets = self._wait_on_elements(2, By.CSS_SELECTOR, self.TWEET_SELECTOR)
                    if eng_tweets == 'error':
                        action = self._handle_error()
                        if action == 'Retry':
                            continue
                        elif action == 'No Quotes yet':
                            break
                    for eng in eng_tweets:
                        try:
                            text = eng.find_element(By.CSS_SELECTOR, self.TWEET_TEXT_SELECTOR).text
                            if text and text not in engagements:
                                engagements.append(text)
                        except:
                            continue
                    self.driver.execute_script('window.scrollBy(0, window.innerHeight);')
                    time.sleep(0.5)

                self.driver.close()
                self.driver.switch_to.window(self.driver.window_handles[0])
                menu_item.send_keys(Keys.ESCAPE)
                return engagements
            except ElementClickInterceptedException as e:
                if attempt < max_retries - 1:
                    print(f"Click intercepted in _get_engagements, retrying ({attempt + 1}/{max_retries})...")
                    time.sleep(1)  # Wait for overlay to clear
                    continue
                print(f"Max retries reached for click interception: {e}")
                if len(self.driver.window_handles) > 1:
                    self.driver.close()
                self.driver.switch_to.window(self.driver.window_handles[0])
                return []
            except Exception as e:
                print(f"Error fetching engagements: {e}")
                if len(self.driver.window_handles) > 1:
                    self.driver.close()
                self.driver.switch_to.window(self.driver.window_handles[0])
                return []

    def _handle_error(self):
        """Handle specific errors during scraping."""
        error_handlers = {
            'Something went wrong. Try reloading.': lambda: (time.sleep(20), self.driver.find_element(By.XPATH, "//span[text()='Retry']").click(), 'Retry'),
            'No Quotes yet': lambda: 'No Quotes yet'
        }
        for span in self.driver.find_elements(By.TAG_NAME, 'span'):
            if span.text in error_handlers:
                result = error_handlers[span.text]()
                return result[-1] if isinstance(result, tuple) else result
        raise Exception("Unhandled error encountered.")

    def _wait_on_element(self, timeout, by, value):
        """Wait for a single element to appear."""
        try:
            return WebDriverWait(self.driver, timeout).until(EC.presence_of_element_located((by, value)))
        except:
            return 'error'

    def _wait_on_elements(self, timeout, by, value):
        """Wait for multiple elements to appear."""
        try:
            return WebDriverWait(self.driver, timeout).until(EC.presence_of_all_elements_located((by, value)))
        except:
            return 'error'

    def _get_remaining_time(self):
        """Calculate remaining time as a string."""
        remaining = int(self.wrap_up_time - time.time())
        hours, remainder = divmod(remaining, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours:02}:{minutes:02}:{seconds:02}"

    def _clean_string(self, text):
        """Clean and trim a string."""
        return text.replace('\n', '').replace('\r', '').strip() if text else None

    def _stats_to_dict(self, stats):
        """Convert stats string to a dictionary."""
        parts = stats.split(', ')
        return {part.split(' ', 1)[1]: int(part.split(' ', 1)[0]) for part in parts}

    def generate_json_filename(self, prefix="post_data"):
        """Generates a timestamped filename for the output JSON file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        return f"{prefix}_{timestamp}.json"

    def save_posts(self, filename=None):
        """Save both existing and newly scraped posts to a JSON file."""
        if filename is None:
            filename = self.generate_json_filename()  # Use generated filename if none provided
        # Combine existing posts with newly scraped posts
        all_posts = self.existing_posts_list + self.scraped_posts
        with open(filename, 'w') as f:
            json.dump(all_posts, f, indent=4)
        print(f"Saved {len(all_posts)} posts (including {len(self.existing_posts_list)} existing and {len(self.scraped_posts)} new) to {filename}.")
        logging.info(f"Data saved to {filename}")
        return filename

    def _listen_for_abort(self):
        """Listen for the 'q' key press to abort scraping."""
        keyboard.wait('q')  # Blocks until 'q' is pressed
        self.stop_scraping = True

    def run(self):
        """Execute the full scraping process."""
        # Configure logging to overwrite the log file each time
        logging.basicConfig(filename='scrape_log.txt', level=logging.INFO, filemode='w')
        logging.info("Beginning scrapeX run.")
        self._setup_driver()
        if not self._login():
            self.driver.quit()
            return
        self._load_target()
        print("Beginning scrape. Press 'q' to abort and save collected data.")
        abort_thread = threading.Thread(target=self._listen_for_abort, daemon=True)
        abort_thread.start()
        self._scrape_posts()
        output_filename = self.save_posts()  # Use generated filename by default
        self.driver.quit()
        print(f"Scraping completed. Data saved to {output_filename}.")


if __name__ == "__main__":
    import os
    xun = os.environ['xun']
    xpw = os.environ['xpw']
    scraper = ScrapeX(
        username=xun,
        password=xpw,
        target_url="https://x.com/search?q=(Elon%20OR%20Musk)%20lang%3Aen&src=typed_query", # e.g., https://x.com/search?q=(Elon%20OR%20Musk)%20lang%3Aen&src=typed_query
        time_limit="00:20:00",
        existing_posts_path='post_data_20250324_1043.json',  # e.g., "existing_posts.json"
        do_scrape_engagements=True,
        headless=True
    )
    scraper.run()