# Bonus Task: Web Scraping to Kafka Pipeline

**Student ID:** 22b031177

## Overview

This project scrapes television broadcast data from Wikipedia and streams it into a Kafka topic. I chose this dataset because it has interesting viewership statistics that are publicly available and well-structured.

## Data Source

**URL:** https://en.wikipedia.org/wiki/List_of_most-watched_television_broadcasts

The page contains multiple tables with data about highly-watched TV broadcasts from different countries. Each table typically includes the show name, date aired, viewer count, and broadcasting network.

## What the script does

1. Fetches the Wikipedia page using requests
2. Parses all tables with BeautifulSoup 
3. Combines data from different tables into one DataFrame
4. Cleans and normalizes the data
5. Sends each row as a JSON message to Kafka topic `bonus_22b031177`
6. Saves the final cleaned dataset to `cleaned_data.csv`

## Data Cleaning Steps

I implemented several cleaning operations to make the data consistent:

1. **Removed citation markers** - Wikipedia has lots of `[1]`, `[note 2]` references that aren't useful for analysis
2. **Standardized text** - Converted titles to lowercase and trimmed extra whitespace
3. **Parsed viewer numbers** - The original data had inconsistent formats like "7.13 million", "7,130,000", or just "73". I converted everything to a float representing millions of viewers
4. **Extracted years** - Pulled out the 4-digit year from various date formats
5. **Filtered incomplete data** - Removed rows that were missing titles or viewer counts since those are essential fields

The cleaning reduced the dataset from around 150+ raw rows to about 50-100 clean records depending on what Wikipedia currently has.

## Sample Kafka Message

Here's what a typical message looks like:

```json
{
  "title": "Super Bowl XLIX",
  "title_clean": "super bowl xlix",
  "date": "February 1, 2015",
  "year": 2015,
  "viewers_millions": 114.4,
  "network": "NBC"
}
```

## Requirements

- Python 3.8+
- Kafka and Zookeeper running (I used Docker)
- Libraries: requests, beautifulsoup4, pandas, kafka-python, lxml

## Setup and Running

Install dependencies:
```bash
pip install requests beautifulsoup4 pandas kafka-python lxml
```

Make sure Kafka is running on localhost:9092. I used docker-compose for this.

Run the script:
```bash
python script.py
```

The script will print progress messages as it scrapes, cleans, and sends data to Kafka.

## Verifying the Data

To check if messages made it to Kafka:

```bash
# See if the topic exists
docker exec -it bonus_dta-kafka-1 kafka-topics --list --bootstrap-server localhost:9092

# Read some messages
docker exec -it bonus_dta-kafka-1 kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic bonus_22b031177 \
  --from-beginning \
  --max-messages 10
```
