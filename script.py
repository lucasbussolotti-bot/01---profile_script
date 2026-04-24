# ==============================
# script.py
# ==============================
# Versão inicial preparada para GitHub
# Observação: Remova chaves sensíveis e use variáveis de ambiente

import os
import re
import json
import time
import requests
import pandas as pd

from google import genai
from google.genai import types
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime
import pytz

# ==============================
# CONFIGURAÇÕES (usar ENV no futuro)
# ==============================

WORKDIR = os.getenv("WORKDIR", "./data")
os.makedirs(WORKDIR, exist_ok=True)

SOCIA_API_KEY = os.getenv("SOCIA_API_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

POST_URL = os.getenv("POST_URL")
SHORTCODE = os.getenv("SHORTCODE")

COMMENTS_LIMIT = int(os.getenv("COMMENTS_LIMIT", 100))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 20))

SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE")

SPREADSHEET_NAME = "comments_classificados"
SHEET_NAME = "Sheet1"
FOLDER_ID = os.getenv("FOLDER_ID")

# ==============================
# SCRAPING
# ==============================

def scrape_instagram_comments():
    url = "https://api.sociavault.com/v1/scrape/instagram/comments"

    headers = {"X-API-Key": SOCIA_API_KEY}

    base_params = {
        "url": POST_URL,
        "shortcode": SHORTCODE,
        "limit": COMMENTS_LIMIT
    }

    all_comments = []
    cursor = None
    page = 1

    while True:
        params = base_params.copy()

        if cursor:
            params["cursor"] = cursor

        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()

        data = response.json()

        items = (
            data.get("data", {}).get("data", {}).get("comments")
            or data.get("data", {}).get("comments")
            or []
        )

        page_comments = normalize_comments(items, page)
        all_comments.extend(page_comments)

        print(f"Página {page}: {len(page_comments)} comentários")

        cursor = (
            data.get("data", {}).get("data", {}).get("cursor")
            or data.get("data", {}).get("cursor")
        )

        if not cursor:
            break

        page += 1
        time.sleep(1)

    return all_comments


def normalize_comments(items, page):
    comments = []

    iterable = items.values() if isinstance(items, dict) else items

    for idx, item in enumerate(iterable, start=1):
        item["_page"] = page
        item["_custom_comment_id"] = f"{page}_{idx}"
        comments.append(item)

    return comments

# ==============================
# DATAFRAME
# ==============================

def has_emoji(text):
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"
        "\U0001F300-\U0001F5FF"
        "\U0001F680-\U0001F6FF"
        "\U0001F1E0-\U0001F1FF"
        "\U00002700-\U000027BF"
        "\U0001F900-\U0001F9FF"
        "\U00002600-\U000026FF"
        "]+",
        flags=re.UNICODE,
    )

    return bool(emoji_pattern.search(text))


def comments_to_dataframe(comments):
    rows = []

    for item in comments:
        user = item.get("user", {})

        rows.append({
            "Id Comentário": item.get("_custom_comment_id"),
            "text": item.get("text"),
            "username": user.get("username")
        })

    df = pd.DataFrame(rows).fillna("")
    df["tem_emoji"] = df["text"].apply(has_emoji)

    return df

# ==============================
# CLASSIFICAÇÃO
# ==============================

client = genai.Client(api_key=GEMINI_API_KEY)


def classificar_lote(comentarios):
    prompt = f"""
Classifique comentários em 'promotor' ou 'detrator'.

{json.dumps(comentarios, ensure_ascii=False)}
"""

    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
    )

    return response.text

# ==============================
# GOOGLE SHEETS
# ==============================

def get_services():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=scopes
    )

    drive_service = build("drive", "v3", credentials=creds)
    sheets_service = build("sheets", "v4", credentials=creds)

    return drive_service, sheets_service

# ==============================
# MAIN
# ==============================

def main():
    comments = scrape_instagram_comments()

    with open(os.path.join(WORKDIR, "comments.json"), "w") as f:
        json.dump(comments, f)

    df = comments_to_dataframe(comments)

    print(df.head())


if __name__ == "__main__":
    main()
