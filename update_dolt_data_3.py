import requests
import re
import json
import csv
from datetime import datetime, timedelta, timezone

# --- Utility Functions ---

def parse_jsonp(jsonp_str, callback_name):
    pattern = rf'^{callback_name}\((.*)\)$'
    match = re.match(pattern, jsonp_str)
    if not match:
        return None
    json_str = match.group(1)
    return json.loads(json_str)

def fetch_dukascopy_calendar(since_ms, until_ms, callback_name="_callbacks____22m8ev8alv"):
    base_url = "https://freeserv.dukascopy.com/2.0/index.php"
    params = {
        "path": "economic_calendar_new/getNews",
        "since": str(since_ms),
        "until": str(until_ms),
        "jsonp": callback_name
    }
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0"
        ),
        "Referer": "https://freeserv.dukascopy.com/2.0/"
    }
    resp = requests.get(base_url, params=params, headers=headers)
    if resp.status_code == 200:
        data = parse_jsonp(resp.text.strip(), callback_name)
        return data if isinstance(data, list) else []
    else:
        print(f"Error: {resp.status_code} for {since_ms}–{until_ms}")
        return []

def datetime_to_ms(dt):
    return int(dt.timestamp() * 1000)

def parse_datetime(dt_str):
    if not isinstance(dt_str, str):
        return None
    try:
        # Strip known UTC indicators
        if '+0000' in dt_str:
            dt_str = dt_str.replace('+0000', '')
        elif '+00:00' in dt_str:
            dt_str = dt_str.replace('+00:00', '')
        elif dt_str.endswith('Z'):
            dt_str = dt_str[:-1]

        # Parse and attach UTC timezone
        if 'T' in dt_str:
            try:
                dt = datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%S')
            except ValueError:
                dt = datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%S.%f')
        else:
            try:
                dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                try:
                    dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S.%f')
                except ValueError:
                    dt = datetime.strptime(dt_str, '%Y-%m-%d')
        return dt.replace(tzinfo=timezone.utc)
    except Exception as e:
        print(f"Warning: Could not parse datetime '{dt_str}': {e}")
        return None

def datetime_to_unix(dt):
    return "" if dt is None else str(int(dt.timestamp()))

def clean_text(text):
    if not isinstance(text, str):
        return text
    text = text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    text = text.replace('"', '""')
    text = ''.join(char for char in text if ord(char) >= 32 or char == '\t')
    return re.sub(r'\s+', ' ', text).strip()

def normalize_value(value, key):
    if value is None:
        return ""
    if key in ['date', 'dateRelease', 'dateExpiry', 'dateStart', 'dateEnd']:
        dt = parse_datetime(value)
        return dt.strftime('%Y-%m-%d %H:%M:%S') if dt else ""
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return clean_text(value)
    return str(value)

def gather_fields(event, global_fields):
    for key in event.keys():
        if key not in global_fields:
            global_fields.append(key)

# --- CSV Writing Function ---

def write_csv(events, output_csv):
    all_fields = []
    for event in events:
        gather_fields(event, all_fields)

    datetime_fields = ['date', 'dateRelease', 'dateExpiry', 'dateStart', 'dateEnd']
    extended_fields = []
    for field in all_fields:
        extended_fields.append(field)
        if field == 'date':
            extended_fields.append("unix_time")
        elif field in datetime_fields:
            extended_fields.append(f"{field}_unix")

    with open(output_csv, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=extended_fields, quoting=csv.QUOTE_ALL, delimiter=',')
        writer.writeheader()
        rows_written = 0
        for event in events:
            cleaned_event = {}
            for field in all_fields:
                value = event.get(field, "")
                cleaned_event[field] = normalize_value(value, field)
                if field == 'date':
                    dt = parse_datetime(value)
                    cleaned_event["unix_time"] = datetime_to_unix(dt)
                elif field in datetime_fields:
                    dt = parse_datetime(value)
                    cleaned_event[f"{field}_unix"] = datetime_to_unix(dt)
            try:
                writer.writerow(cleaned_event)
                rows_written += 1
            except Exception as e:
                print(f"Error writing row: {e}")
                print(f"Problematic data: {cleaned_event}")
                continue
    print(f"Done! Created {output_csv} with {len(extended_fields)} columns and {rows_written} rows.")

# --- Main Execution ---

def main():
    now = datetime.now(timezone.utc)
    start_date = now - timedelta(days=7)
    end_date = now + timedelta(days=14)

    print(f"Fetching events from {start_date.date()} to {end_date.date()} (UTC)...")
    since_ms = datetime_to_ms(start_date)
    until_ms = datetime_to_ms(end_date)

    events = fetch_dukascopy_calendar(since_ms, until_ms)
    print(f"Retrieved {len(events)} events.")

    if events and 'date' in events[0]:
        events.sort(key=lambda e: e.get('date', ''), reverse=True)

    output_csv = "dukascopy_weekly_update.csv"
    write_csv(events, output_csv)

if __name__ == "__main__":
    main()
