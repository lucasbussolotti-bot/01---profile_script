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

SPREADSHEET_ID = "1cn68TA8_ajbbIOaMofE_7-Vc4_BWfQRMHehrO6SB_Q4"

SHEET_POSTS  = "Hashtag_posts"         # aba de entrada (leitura)
SHEET_DETAIL = "Hashtag_posts_detail"  # aba de saída (escrita)

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
# ETAPA 1 — LER POSTS DA Hashtag_posts
# ==============================

def read_posts(sheets_service):
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_POSTS}!A:Z"
    ).execute()

    rows = result.get("values", [])
    if len(rows) <= 1:
        print("Nenhum post encontrado na planilha Hashtag_posts.")
        return []

    headers = [h.strip().lower() for h in rows[0]]

    if "share_url" not in headers:
        print(f"Coluna 'share_url' não encontrada. Colunas: {headers}")
        return []

    url_col     = headers.index("share_url")
    hashtag_col = headers.index("hashtag") if "hashtag" in headers else None
    country_col = headers.index("country") if "country" in headers else None

    posts = []
    for row in rows[1:]:
        if len(row) <= url_col:
            continue
        share_url = row[url_col].strip()
        if not share_url:
            continue
        hashtag = row[hashtag_col].strip() if hashtag_col is not None and len(row) > hashtag_col else ""
        country = row[country_col].strip() if country_col is not None and len(row) > country_col else ""
        posts.append({"share_url": share_url, "hashtag": hashtag, "country": country})

    print(f"  {len(posts)} post(s) encontrado(s) na Hashtag_posts.")
    return posts


# ==============================
# ETAPA 2 — LER URLs JÁ PROCESSADAS EM Hashtag_posts_detail
# ==============================

def get_processed_urls(sheets_service):
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


# ==============================
# ETAPA 3 — BUSCAR VIDEO INFO
# ==============================

def fetch_video_info(share_url):
    url = "https://api.sociavault.com/v1/scrape/tiktok/video-info"
    headers = {"X-API-Key": SOCIA_API_KEY}
    params = {"url": share_url}

    response = requests.get(url, headers=headers, params=params, timeout=API_TIMEOUT)
    print(f"    Status: {response.status_code}")
    response.raise_for_status()

    data = response.json()
    detail = data.get("data", {}).get("aweme_detail", {})
    return detail


def extract_fields(detail, share_url, hashtag, country, run_datetime):
    """Extrai todos os campos relevantes do aweme_detail."""

    # Author
    author = detail.get("author", {})

    # Statistics
    stats = detail.get("statistics", {})

    # Music
    music = detail.get("music", {})

    # create_time → formata para string legível
    create_time_raw = detail.get("create_time", "")
    try:
        create_time = datetime.fromtimestamp(create_time_raw, tz=tz_br).strftime("%Y-%m-%d %H:%M:%S") if create_time_raw else ""
    except Exception:
        create_time = ""

    # duration em segundos
    duration = detail.get("video", {}).get("duration", "") or detail.get("duration", "")

    return [
        # Chave de ligação
        share_url,
        hashtag,
        country,
        run_datetime,

        # Identificadores do vídeo
        detail.get("aweme_id", ""),
        detail.get("desc", ""),
        create_time,
        detail.get("region", ""),
        duration,

        # Autor
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

        # Estatísticas
        stats.get("play_count", ""),
        stats.get("digg_count", ""),
        stats.get("comment_count", ""),
        stats.get("share_count", ""),
        stats.get("collect_count", ""),
        stats.get("download_count", ""),
        stats.get("repost_count", ""),

        # Música
        music.get("title", ""),
        music.get("author", ""),
        music.get("id_str", "") or str(music.get("id", "")),
        music.get("duration", ""),

        # Flags do vídeo
        detail.get("is_ads", ""),
        detail.get("is_paid_content", ""),
        detail.get("aigc_info", {}).get("created_by_ai", ""),
    ]


HEADER = [
    "share_url", "hashtag", "country", "run_datetime",
    "aweme_id", "description", "create_time", "video_region", "duration_seconds",
    "author_username", "author_nickname", "author_uid", "author_sec_uid",
    "author_region", "author_bio", "author_followers", "author_following",
    "author_total_likes", "author_verified",
    "play_count", "like_count", "comment_count", "share_count",
    "save_count", "download_count", "repost_count",
    "music_title", "music_author", "music_id", "music_duration",
    "is_ad", "is_paid_content", "created_by_ai",
]


# ==============================
# ETAPA 4 — SALVAR NA PLANILHA
# ==============================

def save_details_to_sheets(sheets_service, rows_to_add):
    if not rows_to_add:
        print("  Nenhuma linha nova para salvar.")
        return

    existing_data = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_DETAIL}!A:A"
    ).execute()
    existing_rows = existing_data.get("values", [])

    if not existing_rows:
        values = [HEADER] + rows_to_add
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
    print("INICIANDO PIPELINE VIDEO DETAIL")
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

    # ETAPA 1 — Ler posts
    print(f"\n[ETAPA 1] Lendo posts da aba '{SHEET_POSTS}'...")
    posts = read_posts(sheets_service)

    if not posts:
        print("Nenhum post para processar. Encerrando.")
        return

    # ETAPA 2 — Carregar URLs já processadas
    print(f"\n[DEDUP] Carregando URLs já processadas em '{SHEET_DETAIL}'...")
    processed_urls = get_processed_urls(sheets_service)

    run_datetime = datetime.now(tz_br).strftime("%Y-%m-%d %H:%M:%S")
    all_new_rows = []
    errors = 0

    # ETAPA 3 — Buscar detalhes de cada post
    print(f"\n[ETAPA 3] Buscando video-info para {len(posts)} post(s)...")

    for i, post in enumerate(posts, start=1):
        share_url = post["share_url"]
        hashtag   = post["hashtag"]
        country   = post["country"]

        if share_url in processed_urls:
            print(f"  [{i}/{len(posts)}] Já processado, pulando: {share_url}")
            continue

        print(f"\n  [{i}/{len(posts)}] #{hashtag} | {share_url}")

        try:
            detail = fetch_video_info(share_url)
            if not detail:
                print(f"    Aviso: resposta vazia para {share_url}. Pulando.")
                continue

            row = extract_fields(detail, share_url, hashtag, country, run_datetime)
            row = [str(v) if v is not None else "" for v in row]
            all_new_rows.append(row)
            processed_urls.add(share_url)
            print(f"    OK — @{detail.get('author', {}).get('unique_id', '?')} | plays: {detail.get('statistics', {}).get('play_count', '?')}")

        except Exception as e:
            print(f"    ERRO ao buscar detalhes: {e}. Pulando.")
            errors += 1

        time.sleep(1)  # respeita rate limit da API

    # ETAPA 4 — Salvar
    print(f"\n[ETAPA 4] Salvando {len(all_new_rows)} linhas novas na planilha...")

    # Reconecta antes de salvar (evita timeout SSL por inatividade)
    sheets_service = get_google_services()
    save_details_to_sheets(sheets_service, all_new_rows)

    print(f"\n{'=' * 60}")
    print(f"PIPELINE FINALIZADO — {len(all_new_rows)} salvos | {errors} erros")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
