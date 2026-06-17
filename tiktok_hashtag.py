import os
import json
import time
import requests
import builtins

from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
import pytz

# Força flush imediato em todos os prints
_original_print = builtins.print
def print(*args, **kwargs):
    kwargs["flush"] = True
    _original_print(*args, **kwargs)

API_TIMEOUT = 60  # segundos

# ==============================
# CONFIGURAÇÕES
# ==============================

SOCIA_API_KEY = os.environ.get("SOCIAVAULT_API_KEY")

POSTS_LIMIT = 20  # quantos posts buscar por hashtag

SPREADSHEET_ID = "1cn68TA8_ajbbIOaMofE_7-Vc4_BWfQRMHehrO6SB_Q4"

SHEET_HASHTAGS = "Hashtag_Data"          # aba de entrada
SHEET_POSTS    = "Hashtag_posts"         # aba intermediária
SHEET_DETAIL   = "Hashtag_posts_detail"  # aba de saída final

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
    sheets_service = build("sheets", "v4", credentials=creds)
    return sheets_service


# ==============================
# ETAPA 1 — LER HASHTAGS
# ==============================

def read_hashtags(sheets_service):
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_HASHTAGS}!A:Z"
    ).execute()

    rows = result.get("values", [])
    if len(rows) <= 1:
        print("Nenhuma hashtag encontrada na planilha.")
        return []

    headers = [h.strip().lower() for h in rows[0]]

    if "hashtag" not in headers:
        print(f"Coluna 'Hashtag' não encontrada. Colunas disponíveis: {headers}")
        return []

    col_hashtag = headers.index("hashtag")

    # FIX 3: validação segura das colunas opcionais antes de usar como índice
    col_country    = headers.index("country")    if "country"    in headers else None
    col_marca_kc   = headers.index("marca_kc")   if "marca_kc"   in headers else None
    col_competidor = headers.index("competidor") if "competidor" in headers else None
    col_pais       = headers.index("pais")       if "pais"       in headers else None

    if col_country is None:
        print("  Aviso: coluna 'Country' não encontrada. Sem filtro de país.")

    entries = []
    seen = set()

    for row in rows[1:]:
        if len(row) <= col_hashtag:
            continue

        tag = row[col_hashtag].strip().lstrip("#")

        if not tag:
            continue

        country    = (row[col_country].strip().upper()  if col_country    is not None and len(row) > col_country    else "")
        marca_kc   = (row[col_marca_kc].strip()         if col_marca_kc   is not None and len(row) > col_marca_kc   else "")
        competidor = (row[col_competidor].strip()        if col_competidor is not None and len(row) > col_competidor else "")
        pais       = (row[col_pais].strip()              if col_pais       is not None and len(row) > col_pais       else "")

        key = (tag.lower(), country, marca_kc, competidor, pais)

        if key not in seen:
            seen.add(key)

            entries.append({
                "hashtag": tag,
                "country": country,
                "marca_kc": marca_kc,
                "competidor": competidor,
                "pais": pais
            })

    print(f"  {len(entries)} entrada(s) única(s) encontrada(s):")
    for e in entries:
        pais_label = e['country'] if e['country'] else "todos os países"
        print(f"    #{e['hashtag']} → {pais_label}")

    return entries


# ==============================
# ETAPA 2 — BUSCAR POSTS POR HASHTAG
# ==============================

def fetch_posts_by_hashtag(hashtag):
    url = "https://api.sociavault.com/v1/scrape/tiktok/search/hashtag"
    headers = {"X-API-Key": SOCIA_API_KEY}
    params = {"hashtag": hashtag, "count": POSTS_LIMIT}

    response = requests.get(url, headers=headers, params=params, timeout=API_TIMEOUT)
    print(f"    Status ({hashtag}): {response.status_code}")
    response.raise_for_status()

    data = response.json()
    aweme_list = data.get("data", {}).get("aweme_list", {})

    if isinstance(aweme_list, dict):
        items = list(aweme_list.values())
    elif isinstance(aweme_list, list):
        items = aweme_list
    else:
        print(f"    Aviso: estrutura inesperada de aweme_list: {type(aweme_list)}")
        return []

    results = []

    for item in items:
        share_url = (
            item.get("share_info", {}).get("share_url")
            or item.get("share_url")
            or ""
        )
        if not share_url:
            continue

        region = (
            item.get("author", {}).get("region", "")
            or item.get("region", "")
        ).upper()

        results.append({"share_url": share_url, "region": region})

    print(f"    Posts encontrados: {len(results)}")
    return results


def get_existing_urls_posts(sheets_service):
    """Retorna set de share_urls já salvas na aba Hashtag_posts."""
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_POSTS}!A:Z"
        ).execute()
        rows = result.get("values", [])
        if len(rows) <= 1:
            return set()
        headers = [h.strip().lower() for h in rows[0]]
        if "share_url" not in headers:
            return set()
        url_col = headers.index("share_url")
        existing = set()
        for row in rows[1:]:
            if len(row) > url_col and row[url_col].strip():
                existing.add(row[url_col].strip())
        print(f"  URLs já salvas em Hashtag_posts: {len(existing)}")
        return existing
    except Exception as e:
        print(f"  Aviso ao ler Hashtag_posts: {e}")
        return set()


def save_posts_to_sheets(sheets_service, rows_to_add):
    if not rows_to_add:
        print("  Nenhuma linha nova para salvar em Hashtag_posts.")
        return

    existing_data = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_POSTS}!A:A"
    ).execute()
    existing_rows = existing_data.get("values", [])

    if not existing_rows:
        header = [[
            "hashtag",
            "share_url",
            "country",
            "marca_kc",
            "competidor",
            "pais",
            "region",
            "run_datetime"
        ]]
        values = header + rows_to_add
        sheets_service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_POSTS}!A1",
            valueInputOption="RAW",
            body={"values": values}
        ).execute()
        print(f"  Hashtag_posts: {len(rows_to_add)} linhas inseridas com cabeçalho.")
    else:
        sheets_service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_POSTS}!A:A",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": rows_to_add}
        ).execute()
        print(f"  Hashtag_posts: {len(rows_to_add)} novas linhas adicionadas.")


# ==============================
# ETAPA 3 — BUSCAR VIDEO INFO
# ==============================

def fetch_video_info(share_url):
    url = "https://api.sociavault.com/v1/scrape/tiktok/video-info"
    headers = {"X-API-Key": SOCIA_API_KEY}
    params = {"url": share_url}

    response = requests.get(url, headers=headers, params=params, timeout=API_TIMEOUT)
    print(f"    Status video-info: {response.status_code}")
    response.raise_for_status()

    data = response.json()
    return data.get("data", {}).get("aweme_detail", {})


def extract_fields(detail, share_url, hashtag, country, marca_kc, competidor, pais, run_datetime):
    author = detail.get("author", {})
    stats  = detail.get("statistics", {})
    music  = detail.get("music", {})

    create_time_raw = detail.get("create_time", "")
    try:
        create_time = datetime.fromtimestamp(create_time_raw, tz=tz_br).strftime("%Y-%m-%d %H:%M:%S") if create_time_raw else ""
    except Exception:
        create_time = ""

    duration = detail.get("video", {}).get("duration", "") or detail.get("duration", "")

    return [
        share_url, hashtag, country, marca_kc, competidor, pais, run_datetime,  # FIX 1: 'pais' em vez de 'paid'
        detail.get("aweme_id", ""),
        detail.get("desc", ""),
        create_time,
        detail.get("region", ""),
        duration,
        author.get("unique_id", ""),
        author.get("nickname", ""),
        author.get("uid", ""),
        author.get("sec_uid", ""),
        author.get("region", ""),
        author.get("signature", ""),
        author.get("follower_count", ""),
        author.get("following_count", ""),
        author.get("total_favorited", ""),
        int(author.get("verification_type", 0)) > 0,
        stats.get("play_count", ""),
        stats.get("digg_count", ""),
        stats.get("comment_count", ""),
        stats.get("share_count", ""),
        stats.get("collect_count", ""),
        stats.get("download_count", ""),
        stats.get("repost_count", ""),
        music.get("title", ""),
        music.get("author", ""),
        music.get("id_str", "") or str(music.get("id", "")),
        music.get("duration", ""),
        detail.get("is_ads", ""),
        detail.get("is_paid_content", ""),
        detail.get("aigc_info", {}).get("created_by_ai", ""),
    ]


DETAIL_HEADER = [
    "share_url", "hashtag", "country", "marca_kc", "competidor", "pais", "run_datetime",
    "aweme_id", "description", "create_time", "video_region", "duration_seconds",
    "author_username", "author_nickname", "author_uid", "author_sec_uid",
    "author_region", "author_bio", "author_followers", "author_following",
    "author_total_likes", "author_verified",
    "play_count", "like_count", "comment_count", "share_count",
    "save_count", "download_count", "repost_count",
    "music_title", "music_author", "music_id", "music_duration",
    "is_ad", "is_paid_content", "created_by_ai",
]


def get_processed_urls(sheets_service):
    """Retorna set de share_urls já processadas em Hashtag_posts_detail."""
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_DETAIL}!A:Z"
        ).execute()
        rows = result.get("values", [])
        if len(rows) <= 1:
            return set()
        headers = [h.strip().lower() for h in rows[0]]
        if "share_url" not in headers:
            return set()
        url_col = headers.index("share_url")
        processed = set()
        for row in rows[1:]:
            if len(row) > url_col and row[url_col].strip():
                processed.add(row[url_col].strip())
        print(f"  URLs já processadas em Hashtag_posts_detail: {len(processed)}")
        return processed
    except Exception as e:
        print(f"  Aviso ao ler Hashtag_posts_detail: {e}")
        return set()


def save_details_to_sheets(sheets_service, rows_to_add):
    if not rows_to_add:
        print("  Nenhuma linha nova para salvar em Hashtag_posts_detail.")
        return

    existing_data = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_DETAIL}!A:A"
    ).execute()
    existing_rows = existing_data.get("values", [])

    if not existing_rows:
        values = [DETAIL_HEADER] + rows_to_add
        sheets_service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_DETAIL}!A1",
            valueInputOption="RAW",
            body={"values": values}
        ).execute()
        print(f"  Hashtag_posts_detail: {len(rows_to_add)} linhas inseridas com cabeçalho.")
    else:
        sheets_service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_DETAIL}!A:A",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": rows_to_add}
        ).execute()
        print(f"  Hashtag_posts_detail: {len(rows_to_add)} novas linhas adicionadas.")


# ==============================
# EXECUÇÃO PRINCIPAL
# ==============================

def main():
    print("=" * 60)
    print("INICIANDO PIPELINE HASHTAG TIKTOK")
    print("=" * 60)

    print("\n[CONFIG] Verificando variáveis de ambiente...")
    missing = []
    for var in ["SOCIAVAULT_API_KEY", "GDRIVE_CREDENTIALS"]:
        val = os.environ.get(var)
        if not val:
            missing.append(var)
            print(f"  ERRO: {var} não encontrada!")
        else:
            print(f"  OK: {var} ({len(val)} chars)")

    if missing:
        print(f"\nVariáveis faltando: {missing}. Encerrando.")
        return

    print("\n[CONFIG] Inicializando Google Services...")
    sheets_service = get_google_services()
    print("  Google Services OK")

    # ── ETAPA 1 — Ler hashtags ──────────────────────────────────
    print(f"\n{'=' * 60}")
    print(f"[ETAPA 1] Lendo hashtags de '{SHEET_HASHTAGS}'...")
    print(f"{'=' * 60}")
    entries = read_hashtags(sheets_service)

    if not entries:
        print("Nenhuma hashtag para processar. Encerrando.")
        return

    # ── ETAPA 2 — Buscar posts por hashtag ─────────────────────
    print(f"\n{'=' * 60}")
    print(f"[ETAPA 2] Buscando posts por hashtag...")
    print(f"{'=' * 60}")

    # FIX 2: indentação corrigida — fora do for, no nível correto
    existing_urls = get_existing_urls_posts(sheets_service)
    run_datetime = datetime.now(tz_br).strftime("%Y-%m-%d %H:%M:%S")

    new_post_rows = []

    # Acumula todos os posts para etapa 3
    all_posts = []

    for entry in entries:

        hashtag    = entry["hashtag"]
        country    = entry["country"]
        marca_kc   = entry["marca_kc"]
        competidor = entry["competidor"]
        pais       = entry["pais"]

        print(
            f"\n  HASHTAG: #{hashtag}" +
            (f" | PAÍS: {country}" if country else " | PAÍS: todos")
        )

        try:
            posts = fetch_posts_by_hashtag(hashtag)

        except Exception as e:
            print(f"    ERRO ao buscar #{hashtag}: {e}. Pulando.")
            continue

        for post in posts:

            share_url = post["share_url"]
            region    = post["region"]

            all_posts.append({
                "share_url": share_url,
                "hashtag": hashtag,
                "country": country,
                "marca_kc": marca_kc,
                "competidor": competidor,
                "pais": pais
            })

            if share_url in existing_urls:
                continue

            new_post_rows.append([
                hashtag,
                share_url,
                country,
                marca_kc,
                competidor,
                pais,
                region,
                run_datetime
            ])

            existing_urls.add(share_url)

        print(
            f"    Novos posts para #{hashtag}: "
            f"{sum(1 for r in new_post_rows if r[0] == hashtag)}"
        )

        time.sleep(1)

    # Salva posts novos em Hashtag_posts
    sheets_service = get_google_services()
    save_posts_to_sheets(sheets_service, new_post_rows)

    # ── ETAPA 3 — Buscar video-info para todos os posts ────────
    print(f"\n{'=' * 60}")
    print(f"[ETAPA 3] Buscando video-info para {len(all_posts)} post(s)...")
    print(f"{'=' * 60}")

    sheets_service  = get_google_services()
    processed_urls  = get_processed_urls(sheets_service)
    run_datetime    = datetime.now(tz_br).strftime("%Y-%m-%d %H:%M:%S")
    new_detail_rows = []
    errors          = 0

    for i, post in enumerate(all_posts, start=1):
        share_url  = post["share_url"]
        hashtag    = post["hashtag"]
        country    = post["country"]
        marca_kc   = post["marca_kc"]
        competidor = post["competidor"]
        pais       = post["pais"]

        if share_url in processed_urls:
            print(f"  [{i}/{len(all_posts)}] Já processado, pulando.")
            continue

        print(f"\n  [{i}/{len(all_posts)}] #{hashtag} | {share_url}")

        try:
            detail = fetch_video_info(share_url)
            if not detail:
                print(f"    Aviso: resposta vazia. Pulando.")
                continue

            row = extract_fields(detail, share_url, hashtag, country, marca_kc, competidor, pais, run_datetime)
            row = [str(v) if v is not None else "" for v in row]
            new_detail_rows.append(row)
            processed_urls.add(share_url)
            print(f"    OK — @{detail.get('author', {}).get('unique_id', '?')} | plays: {detail.get('statistics', {}).get('play_count', '?')}")

        except Exception as e:
            print(f"    ERRO: {e}. Pulando.")
            errors += 1

        time.sleep(1)

    # Salva detalhes em Hashtag_posts_detail
    sheets_service = get_google_services()
    save_details_to_sheets(sheets_service, new_detail_rows)

    print(f"\n{'=' * 60}")
    print(f"PIPELINE FINALIZADO")
    print(f"  Posts novos salvos em Hashtag_posts:           {len(new_post_rows)}")
    print(f"  Detalhes novos salvos em Hashtag_posts_detail: {len(new_detail_rows)}")
    print(f"  Erros no video-info:                           {errors}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
