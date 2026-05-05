import os
import re
import json
import time
import requests
import pandas as pd
import sys
import builtins

from datetime import datetime
from google import genai
from google.genai import types
from google.oauth2 import service_account
from googleapiclient.discovery import build
import pytz

# Força flush imediato em todos os prints
_original_print = builtins.print
def print(*args, **kwargs):
    kwargs["flush"] = True
    _original_print(*args, **kwargs)

API_TIMEOUT = 60

# ==============================
# CONFIGURAÇÕES
# ==============================

SOCIA_API_KEY = os.environ.get("SOCIAVAULT_API_KEY")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

POSTS_LIMIT = 12
COMMENTS_LIMIT = 100
BATCH_SIZE = 20
POST_EXPIRY_DAYS = 14

SPREADSHEET_PROFILES_ID = "1VK7_oyA3boJaudPaAiwk7xYl6sxReed63eOYBP9ahxo"
SPREADSHEET_DATA_PROFILE_ID = "1S86wWk2yO525qC0JQ6IZ6G5gFCYhzzn4Ny6TwZC2E98"
SPREADSHEET_DATA_COMMENTS_ID = "1orR6-MXGNajad6q5IP1dakAPQMM5lyUMX-718YbQzhI"

SHEET_PROFILES = "instagram_profile"
SHEET_DATA_PROFILE = "data_profile"
SHEET_DATA_COMMENTS = "data_comments"

tz_br = pytz.timezone("America/Sao_Paulo")


# ==============================
# GOOGLE SERVICES
# ==============================

def get_google_services():
    creds_json = json.loads(os.environ.get("GDRIVE_CREDENTIALS"))
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = service_account.Credentials.from_service_account_info(
        creds_json, scopes=scopes
    )
    drive_service = build("drive", "v3", credentials=creds)
    sheets_service = build("sheets", "v4", credentials=creds)
    return drive_service, sheets_service


# ==============================
# ETAPA 1 — PERFIS
# ==============================

def read_profiles(sheets_service):
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_PROFILES_ID,
        range=f"{SHEET_PROFILES}!A:B"
    ).execute()

    rows = result.get("values", [])
    if len(rows) <= 1:
        print("Nenhum perfil encontrado.")
        return []

    headers = [h.strip().lower() for h in rows[0]]
    profiles = []

    for row in rows[1:]:
        while len(row) < len(headers):
            row.append("")
        entry = dict(zip(headers, row))
        profile = entry.get("profile", "").strip()
        if profile:
            profiles.append({"profile": profile})

    print(f"{len(profiles)} perfis encontrados: {[p['profile'] for p in profiles]}")
    return profiles


# ==============================
# 🔥 NOVO: PROFILE (CORRETO)
# ==============================

def fetch_profile(handle):
    headers = {"X-API-Key": SOCIA_API_KEY}

    url = "https://api.sociavault.com/v1/scrape/instagram/profile"
    params = {"handle": handle}

    response = requests.get(url, params=params, headers=headers, timeout=API_TIMEOUT)
    print(f"  Status profile ({handle}): {response.status_code}")
    response.raise_for_status()

    data = response.json()
    user = data.get("data", {}).get("user", {})

    return {
        "followers_count": user.get("edge_followed_by", {}).get("count", ""),
        "following_count": user.get("edge_follow", {}).get("count", ""),
        "total_posts_count": user.get("edge_owner_to_timeline_media", {}).get("count", "")
    }


# ==============================
# ETAPA 2 — POSTS
# ==============================

def fetch_posts(handle):
    headers = {"X-API-Key": SOCIA_API_KEY}

    url = "https://api.sociavault.com/v1/scrape/instagram/posts"
    params = {"handle": handle, "limit": POSTS_LIMIT}

    response = requests.get(url, params=params, headers=headers, timeout=API_TIMEOUT)
    print(f"  Status posts ({handle}): {response.status_code}")
    response.raise_for_status()

    data = response.json()
    items = data.get("data", {}).get("items", {})

    # 🔥 PROFILE CORRETO
    profile_data = fetch_profile(handle)

    followers_count = profile_data["followers_count"]
    following_count = profile_data["following_count"]
    total_posts_count = profile_data["total_posts_count"]

    run_datetime = datetime.now(tz_br).strftime("%Y-%m-%d %H:%M:%S")

    rows = []

    if isinstance(items, dict):
        iterable = list(items.values())
    else:
        iterable = items

    iterable = iterable[:POSTS_LIMIT]

    for item in iterable:
        code = item.get("code")
        post_url = item.get("url") or f"https://www.instagram.com/p/{code}/"

        rows.append({
            "run_datetime": run_datetime,
            "Plataform": "Instagram",
            "username": handle,
            "followers_count": followers_count,
            "following_count": following_count,
            "total_posts_count": total_posts_count,
            "code": code,
            "url": post_url,
            "media_type": item.get("media_type"),
            "comment_count": item.get("comment_count"),
            "like_count": item.get("like_count"),
            "play_count": item.get("play_count"),
            "first_extracted_at": run_datetime
        })

    df = pd.DataFrame(rows).fillna("")
    print(f"  Posts extraídos: {len(df)}")
    return df


# ==============================
# 🔥 FALTAVA ESSA FUNÇÃO
# ==============================

def get_saved_post_codes(sheets_service):
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_DATA_PROFILE_ID,
            range=f"{SHEET_DATA_PROFILE}!A:Z"
        ).execute()

        rows = result.get("values", [])
        if len(rows) <= 1:
            return {}

        headers = rows[0]

        if "code" not in headers:
            return {}

        code_idx = headers.index("code")
        date_idx = headers.index("first_extracted_at") if "first_extracted_at" in headers else None

        saved = {}

        for row in rows[1:]:
            if len(row) > code_idx:
                code = row[code_idx]
                date = row[date_idx] if date_idx and len(row) > date_idx else ""

                if code and code not in saved:
                    saved[code] = date

        return saved

    except Exception as e:
        print(f"Erro ao ler data_profile: {e}")
        return {}


# ==============================
# ETAPA 3 — COMENTÁRIOS (MANTIDO)
# ==============================

def is_post_expired(first_extracted_at_str):
    if not first_extracted_at_str:
        return False
    try:
        dt = datetime.strptime(first_extracted_at_str, "%Y-%m-%d %H:%M:%S")
        dt = tz_br.localize(dt)
        return (datetime.now(tz_br) - dt).days > POST_EXPIRY_DAYS
    except:
        return False


# ==============================
# MAIN (NÃO ALTERADO)
# ==============================

def main():
    print("INICIANDO PIPELINE")

    drive, sheets = get_google_services()

    profiles = read_profiles(sheets)

    saved_post_codes = get_saved_post_codes(sheets)

    for p in profiles:
        handle = p["profile"]

        print(f"\nPROCESSANDO {handle}")

        df_posts = fetch_posts(handle)

        print(f"OK: {len(df_posts)} posts")


if __name__ == "__main__":
    main()
