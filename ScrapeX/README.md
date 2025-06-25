# ScrapeX: Post Scraping Utility for X

Welcome to `scrapeX`, a robust tweet scraping utility built with Selenium. This tool automates the extraction of tweets from search results or timelines on **X (formerly Twitter)** and supports optional engagement scraping, time-limited runs, and CLI usage.

<br><br>

## 🧠 Purpose

This project is part of my [Data Analytics Portfolio](https://github.com/YOUR_USERNAME/Data-Analytics-Portfolio), showcasing real-world data acquisition techniques used for text analysis, sentiment tracking, and engagement insights.

<br><br>

## 📂 Project Structure

```plaintext
scrapeX/
├── scrapeX.py           # Main scraping class
├── scrapeX_CLI.py       # Command-line wrapper
├── post_data_*.json     # Saved tweet data
└── scrape_log.txt       # Log output
```

<br><br>

## 🔧 Features

* ✅ Headless or windowed browser automation
* 🔐 Secure login via env vars or arguments
* ⏱️ Time-based scrape limits (`HH:MM:SS`)
* ♻️ Duplicate filtering using existing JSONs
* 💬 Engagement scraping (quotes/retweets)
* 🧵 Threaded abort listener (`q` to quit cleanly)
* 🧪 Command-line interface via `scrapeX_CLI.py`

<br><br>

## 🛠️ Dependencies

```bash
pip install selenium webdriver-manager keyboard
```

Firefox and geckodriver are required (automatically handled via `webdriver-manager`).

<br><br>

## 🔐 Credential Setup

You can either pass credentials as arguments or store them as environment variables:

```bash
export xun="your_username"
export xpw="your_password"
```

<br><br>

## 🚀 CLI Usage (`scrapeX_CLI.py`)

This script simplifies launching a scrape via the terminal.

### ✅ Example:

```bash
python scrapeX_CLI.py \
  --target-url "https://x.com/search?q=(Elon%20OR%20Musk)%20lang%3Aen&src=typed_query" \
  --time-limit "00:20:00" \
  --existing-posts post_data_20250324_1043.json \
  --scrape-engagements \
  --no-headless
```

### 🧾 Arguments

| Flag                          | Description                                       |
| ----------------------------- | ------------------------------------------------- |
| `--username` / `-u`           | X username (can be set via `xun` env var)         |
| `--password` / `-p`           | X password (can be set via `xpw` env var)         |
| `--target-url` / `-t`         | URL to scrape (search results or timeline)        |
| `--time-limit` / `-l`         | Scrape duration (`HH:MM:SS`)                      |
| `--existing-posts` / `-e`     | JSON file with previously scraped posts           |
| `--scrape-engagements` / `-s` | Enable scraping post engagements                  |
| `--no-headless` / `-n`        | Run browser with GUI window (default is headless) |

> 💡 Username and password **must** be provided via arguments or environment variables.

<br><br>

## 📝 Output Example

Each post is stored in JSON like this:

```json
{
  "date_time": "2025-06-20T15:22:04.000Z",
  "profile_name": "Elon Musk",
  "tweet_text": "Excited about Starship launch this week.",
  "stats": {
    "Likes": 12500,
    "Replies": 420,
    "Reposts": 1100
  },
  "engagements": [
    "Can’t wait!",
    "Go SpaceX!",
    "Watching live!"
  ]
}
```

<br><br>

## ⚙️ Use Cases

* Sentiment and text analysis
* NLP model training datasets
* Quote tweet tracking for virality analysis
* Pipeline-ready tweet data collection

<br><br>

## 💡 Takeaways

This project highlights:

* Advanced Selenium scraping with retry handling
* Clean OOP design for reusable automation workflows
* Real-time user input handling with graceful shutdown
* Separation of interface (`scrapeX_CLI.py`) from logic (`scrapeX.py`)

<br><br>

## 🧑‍💻 Author

**Joey W.**
📂 [Data Analytics Portfolio](https://github.com/YOUR_USERNAME/Data-Analytics-Portfolio)

