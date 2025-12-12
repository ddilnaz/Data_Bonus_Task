import re
import json
import time
from typing import List
import requests
from bs4 import BeautifulSoup
import pandas as pd

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

URL = "https://en.wikipedia.org/wiki/List_of_most-watched_television_broadcasts"
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "bonus_22b031177"   
OUTPUT_CSV = "cleaned_data.csv"

def fetch_html(url):
    print("Fetching HTML...")

    headers = {
        "User-Agent": 
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0 Safari/537.36"
    }

    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.text

def extract_tables(html: str) -> List[pd.DataFrame]:
    soup = BeautifulSoup(html, "lxml")
    # Find all wiki tables (many country sections have tables with class 'wikitable')
    tables = soup.find_all("table", {"class": "wikitable"})
    dfs = []
    for i, table in enumerate(tables):
        try:
            df = pd.read_html(str(table))[0]
            df['_source_table_index'] = i
            dfs.append(df)
        except Exception as e:
            print(f"Warning: table {i} parse failed: {e}")
    return dfs

def unify_tables(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    if not dfs:
        raise ValueError("No tables found on the page.")
    normalized_rows = []
    for df in dfs:
        cols = list(df.columns)
        df.columns = [str(c) for c in cols]
        for _, row in df.iterrows():
            rowdict = row.to_dict()
            keys = {k.lower(): k for k in rowdict.keys()}
            title = rowdict.get(keys.get('show') or keys.get('programme') or keys.get('program') or keys.get('broadcast') or keys.get('episode') or keys.get('title'), None)
            date = rowdict.get(keys.get('date'), None)
            viewers = rowdict.get(keys.get('viewers') or keys.get('number of viewers') or keys.get('number of viewers (millions)') or keys.get('number of viewers (millions)'), None)
            network = rowdict.get(keys.get('network') or keys.get('channel'), None)
            if pd.isna(title):
                for k in rowdict:
                    v = rowdict[k]
                    if isinstance(v, str) and len(v.strip()) > 0:
                        title = v
                        break
            normalized_rows.append({
                'title': title,
                'date': date,
                'viewers': viewers,
                'network': network
            })
    out_df = pd.DataFrame(normalized_rows)
    return out_df

def remove_bracket_refs(s):
    if pd.isna(s):
        return s
    s = re.sub(r'\[.*?\]', '', str(s))   
    s = re.sub(r'†', '', s)
    return s.strip()

def parse_viewers(val):

    if pd.isna(val):
        return None
    s = str(val).lower()
    s = remove_bracket_refs(s)
    s = s.replace(',', '').strip()
    m = re.search(r'([0-9]+(?:\.[0-9]+)?)\s*million', s)
    if m:
        try:
            return float(m.group(1))
        except:
            return None
    m2 = re.search(r'([0-9]{4,})', s)  
    if m2:
        try:
            n = float(m2.group(1))
            return n / 1_000_000.0
        except:
            return None
    m3 = re.search(r'^([0-9]+(?:\.[0-9]+)?)$', s)
    if m3:
        try:
            v = float(m3.group(1))
            if v > 100:
                return v / 1_000_000.0
            else:
                return v
        except:
            return None
    return None

def extract_year(date_str):
    if pd.isna(date_str):
        return None
    s = remove_bracket_refs(str(date_str))
    m = re.search(r'(\d{4})', s)
    if m:
        return int(m.group(1))
    return None

def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in ['title', 'date', 'viewers', 'network']:
        if c in df.columns:
            df[c] = df[c].apply(remove_bracket_refs)
    
    if 'title' in df.columns:
        df['title'] = df['title'].fillna('').astype(str).str.strip()
        df['title'] = df['title'].str.replace(r'\s+', ' ', regex=True)
        df['title_clean'] = df['title'].str.lower()
   
    df['viewers_millions'] = df['viewers'].apply(parse_viewers)
   
    df['year'] = df['date'].apply(extract_year)
    df = df[~df['title'].isin(['', None])]
    df = df[df['viewers_millions'].notnull()]
   
    df = df.reset_index(drop=True)
    return df

def ensure_topic(admin_client: KafkaAdminClient, topic_name: str):
    try:
        t = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin_client.create_topics([t])
        print(f"Created topic '{topic_name}'")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic_name}' already exists")
    except Exception as e:
        print(f"Topic creation warning/failed: {e}")

def produce_to_kafka(df: pd.DataFrame, bootstrap: str, topic: str):
 
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=10000)
        ensure_topic(admin, topic)
    except Exception as e:
        print(f"Admin client warning: {e}")
    
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        retries=5
    )
    sent = 0
    for _, row in df.iterrows():
        msg = {
            'title': row.get('title'),
            'title_clean': row.get('title_clean'),
            'date': row.get('date'),
            'year': int(row['year']) if pd.notna(row['year']) else None,
            'viewers_millions': float(row['viewers_millions']) if pd.notna(row['viewers_millions']) else None,
            'network': row.get('network')
        }
        try:
            producer.send(topic, msg)
            sent += 1
        except Exception as e:
            print(f"Failed to send message for {row.get('title')}: {e}")
    producer.flush()
    producer.close()
    print(f"Produced {sent} messages to topic '{topic}'")

def main():
    print("Fetching HTML...")
    html = fetch_html(URL)
    print("Extracting tables...")
    dfs = extract_tables(html)
    print(f"Found {len(dfs)} tables on the page.")
    combined = unify_tables(dfs)
    print(f"Combined rows: {len(combined)}")
    print("Cleaning dataframe...")
    cleaned = clean_df(combined)
    print(f"Cleaned rows: {len(cleaned)}")
    if len(cleaned) < 20:
        print("Warning: less than 20 rows after cleaning. You may want to adjust heuristics.")
    
    cleaned.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved cleaned data to {OUTPUT_CSV}")
  
    try:
        produce_to_kafka(cleaned, KAFKA_BOOTSTRAP, TOPIC)
    except Exception as e:
        print(f"Kafka produce error: {e}")
        print("Make sure Kafka broker is running at", KAFKA_BOOTSTRAP)

if __name__ == "__main__":
    main()
