import os
import json
import hashlib
import requests
import feedparser
from datetime import datetime, timezone, timedelta
from dateutil import parser as dateparser

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DATABASE_ID = os.environ["NOTION_DATABASE_ID"]

NOTION_VERSION = "2022-06-28"
NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": NOTION_VERSION,
}

with open("sources.json", "r") as f:
    CONFIG = json.load(f)

MAX_AGE_DAYS = 30


def normalize_text(text: str) -> str:
    return " ".join((text or "").strip().split())


def make_duplicate_key(url: str, title: str) -> str:
    base = (url or title).strip().lower()
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def is_relevant(title: str, summary: str) -> tuple[bool, str, str]:
    hay = f"{title} {summary}".lower()
    matched_company = ""
    matched_keyword = ""

    for company in CONFIG["companies"]:
        if company.lower() in hay:
            matched_company = company
            break

    for keyword in CONFIG["keywords"]:
        if keyword.lower() in hay:
            matched_keyword = keyword
            break

    return bool(matched_company or matched_keyword), matched_company, matched_keyword


def query_existing_by_duplicate_key(dup_key: str) -> bool:
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    payload = {
        "filter": {
            "property": "Duplicate Key",
            "rich_text": {
                "equals": dup_key
            }
        },
        "page_size": 1
    }
    r = requests.post(url, headers=NOTION_HEADERS, json=payload, timeout=30)
    r.raise_for_status()
    data = r.json()
    return len(data.get("results", [])) > 0


def create_notion_page(item: dict):
    url = "https://api.notion.com/v1/pages"

    props = {
        "Name": {
            "title": [
                {
                    "text": {
                        "content": item["title"][:2000]
                    }
                }
            ]
        },
        "Published": {
            "date": {
                "start": item["published"]
            }
        },
        "Company": {
            "select": {"name": item["company"]} if item["company"] else None
        },
        "Category": {
            "select": {"name": item["category"]}
        },
        "Source": {
            "rich_text": [
                {"text": {"content": item["source"][:2000]}}
            ]
        },
        "URL": {
            "url": item["url"]
        },
        "Summary": {
            "rich_text": [
                {"text": {"content": item["summary"][:2000]}}
            ]
        },
        "Imported On": {
            "date": {
                "start": datetime.now(timezone.utc).date().isoformat()
            }
        },
        "Duplicate Key": {
            "rich_text": [
                {"text": {"content": item["duplicate_key"]}}
            ]
        }
    }

    if props["Company"]["select"] is None:
        del props["Company"]

    payload = {
        "parent": {"database_id": NOTION_DATABASE_ID},
        "properties": props
    }

    r = requests.post(url, headers=NOTION_HEADERS, json=payload, timeout=30)
    r.raise_for_status()


def categorize(company: str, keyword: str) -> str:
    if company:
        return "Competitor"
    if keyword and "bvlos" in keyword.lower():
        return "Regulation"
    return "Industry"


def parse_published_date(entry) -> datetime | None:
    published_raw = (
        getattr(entry, "published", None)
        or getattr(entry, "updated", None)
        or getattr(entry, "created", None)
    )

    if not published_raw:
        return None

    try:
        published_dt = dateparser.parse(published_raw)
        if published_dt is None:
            return None
        if published_dt.tzinfo is None:
            published_dt = published_dt.replace(tzinfo=timezone.utc)
        return published_dt.astimezone(timezone.utc)
    except Exception:
        return None


def is_recent_enough(published_dt: datetime | None) -> bool:
    if published_dt is None:
        return False
    cutoff = datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)
    return published_dt >= cutoff


def parse_entry(entry, source_name):
    title = normalize_text(getattr(entry, "title", "Untitled"))
    summary = normalize_text(
        getattr(entry, "summary", "") or getattr(entry, "description", "")
    )
    link = getattr(entry, "link", "").strip()

    published_dt = parse_published_date(entry)
    if not is_recent_enough(published_dt):
        return None

    relevant, company, keyword = is_relevant(title, summary)
    if not relevant:
        return None

    dup_key = make_duplicate_key(link, title)

    return {
        "title": title,
        "summary": summary[:1800] if summary else "No summary available.",
        "url": link,
        "published": published_dt.date().isoformat(),
        "company": company,
        "category": categorize(company, keyword),
        "source": source_name,
        "duplicate_key": dup_key,
    }


def run():
    new_count = 0

    for feed_url in CONFIG["feeds"]:
        parsed = feedparser.parse(feed_url)
        source_name = getattr(parsed.feed, "title", feed_url)

        for entry in parsed.entries[:30]:
            item = parse_entry(entry, source_name)
            if not item:
                continue

            if query_existing_by_duplicate_key(item["duplicate_key"]):
                continue

            create_notion_page(item)
            new_count += 1
            print(f"Added: {item['title']}")

    print(f"Done. Added {new_count} items.")


if __name__ == "__main__":
    run()
