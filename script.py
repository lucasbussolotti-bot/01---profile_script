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

API_TIMEOUT = 60  # segundos

# ==============================
# CONFIGURAÇÕES
# ==============================

SOCIA_API_KEY = os.environ.get("SOCIAVAULT_API_KEY")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

POSTS_LIMIT = 12
COMMENTS_LIMIT = 100
BATCH_SIZE = 20
POST_EXPIRY_DAYS = 14

# Spreadsheet IDs
SPREADSHEET_PROFILES_ID = "1VK7_oyA3boJaudPaAiwk7xYl6sxReed63eOYBP9ahxo"
SPREADSHEET_DATA_PROFILE_ID = "1S86wWk2yO525qC0JQ6IZ6G5gFCYhzzn4Ny6TwZC2E98"
SPREADSHEET_DATA_COMMENTS_ID = "1orR6-MXGNajad6q5IP1dakAPQMM5lyUMX-718YbQzhI"

# Sheet names
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
# ETAPA 1 — LER PERFIS
# ==============================

def read_profiles(sheets_service):
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_PROFILES_ID,
        range=f"{SHEET_PROFILES}!A:B"
    ).execute()

    rows = result.get("values", [])
    if len(rows) <= 1:
        print("Nenhum perfil encontrado na planilha instagram_profile.")
        return []

    headers = [h.strip().lower() for h in rows[0]]
    profiles = []

    for row in rows[1:]:
        while len(row) < len(headers):
            row.append("")
        entry = dict(zip(headers, row))
        profile = entry.get("profile", "").strip()
        date_added = entry.get("date added", "").strip()
        if profile:
            profiles.append({"profile": profile, "date_added": date_added})

    print(f"{len(profiles)} perfil(is) encontrado(s): {[p['profile'] for p in profiles]}")
    return profiles


# ==============================
# ETAPA 2 — EXTRAIR POSTS (Script 1 adaptado)
# ==============================

def fetch_posts(handle):
    api_key = SOCIA_API_KEY
    headers = {"X-API-Key": api_key}
    url = "https://api.sociavault.com/v1/scrape/instagram/posts"
    params = {"handle": handle, "limit": POSTS_LIMIT}

    response = requests.get(url, params=params, headers=headers, timeout=API_TIMEOUT)
    print(f"  Status posts ({handle}): {response.status_code}")
    response.raise_for_status()
    json_data = response.json()

    items = json_data.get("data", {}).get("items", {})
    top_username = json_data.get("data", {}).get("user", {}).get("username")

    run_datetime = datetime.now(tz_br).strftime("%Y-%m-%d %H:%M:%S")

    rows = []
    if isinstance(items, dict):
        iterable = list(items.values())
    elif isinstance(items, list):
        iterable = items
    else:
        iterable = []

    # Garante apenas os primeiros 12
    iterable = iterable[:POSTS_LIMIT]

    for item in iterable:
        username = item.get("user", {}).get("username", top_username)
        code = item.get("code")
        taken_at = item.get("taken_at")
        post_url = item.get("url")

        if not post_url and code:
            post_url = f"https://www.instagram.com/p/{code}/"

        media_type = item.get("media_type")
        comment_count = item.get("comment_count")
        like_count = item.get("like_count")
        play_count = item.get("play_count")

        image_versions2 = item.get("image_versions2", {})
        candidates = image_versions2.get("candidates", {})
        preview_image_url = None

        if isinstance(candidates, dict):
            best_candidate = candidates.get("0") or next(iter(candidates.values()), {})
            preview_image_url = best_candidate.get("url")
        elif isinstance(candidates, list) and len(candidates) > 0:
            preview_image_url = candidates[0].get("url")

        additional_candidates = image_versions2.get("additional_candidates", {})
        first_frame_url = None
        if additional_candidates and isinstance(additional_candidates, dict):
            first_frame_url = additional_candidates.get("first_frame", {}).get("url")

        rows.append({
            "run_datetime": run_datetime,
            "username": username,
            "code": code,
            "taken_at": taken_at,
            "url": post_url,
            "media_type": media_type,
            "comment_count": comment_count,
            "like_count": like_count,
            "play_count": play_count,
            "preview_image_url": preview_image_url,
            "first_frame_url": first_frame_url,
            "first_extracted_at": run_datetime  # nova coluna
        })

    df = pd.DataFrame(rows)
    df = df.fillna("")
    print(f"  Posts extraídos: {len(df)}")
    return df


def get_saved_post_codes(sheets_service):
    """Retorna dict {code: first_extracted_at} já salvos no data_profile."""
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
        code_col = headers.index("code")
        extracted_col = headers.index("first_extracted_at") if "first_extracted_at" in headers else None
        saved = {}
        for row in rows[1:]:
            if len(row) > code_col:
                code = row[code_col].strip()
                first_extracted = row[extracted_col].strip() if extracted_col and len(row) > extracted_col else ""
                if code:
                    saved[code] = first_extracted
        return saved
    except Exception as e:
        print(f"  Aviso ao ler data_profile: {e}")
        return {}


def save_posts_to_sheets(sheets_service, df):
    df = df.fillna("")

    existing_data = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_DATA_PROFILE_ID,
        range=f"{SHEET_DATA_PROFILE}!A:A"
    ).execute()
    existing_rows = existing_data.get("values", [])

    if not existing_rows:
        values = [df.columns.tolist()] + df.astype(str).values.tolist()
        sheets_service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_DATA_PROFILE_ID,
            range=f"{SHEET_DATA_PROFILE}!A1",
            valueInputOption="RAW",
            body={"values": values}
        ).execute()
        print(f"  data_profile: {len(df)} linhas inseridas com cabeçalho.")
    else:
        append_values = df.astype(str).values.tolist()
        sheets_service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_DATA_PROFILE_ID,
            range=f"{SHEET_DATA_PROFILE}!A:A",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": append_values}
        ).execute()
        print(f"  data_profile: {len(append_values)} linhas adicionadas.")


# ==============================
# ETAPA 3 — COMENTÁRIOS (Script 2 adaptado)
# ==============================

def is_post_expired(first_extracted_at_str):
    """Retorna True se o post foi extraído há mais de 14 dias."""
    if not first_extracted_at_str:
        return False
    try:
        first_extracted = datetime.strptime(first_extracted_at_str, "%Y-%m-%d %H:%M:%S")
        first_extracted = tz_br.localize(first_extracted)
        hoje = datetime.now(tz_br)
        return (hoje - first_extracted).days > POST_EXPIRY_DAYS
    except Exception:
        return False


def scrape_instagram_comments(post_url, shortcode):
    url = "https://api.sociavault.com/v1/scrape/instagram/comments"
    headers = {"X-API-Key": SOCIA_API_KEY}
    base_params = {
        "url": post_url,
        "shortcode": shortcode,
        "limit": COMMENTS_LIMIT
    }

    all_comments = []
    cursor = None
    page = 1

    while True:
        params = base_params.copy()
        if cursor:
            params["cursor"] = cursor

        response = requests.get(url, params=params, headers=headers, timeout=API_TIMEOUT)
        response.raise_for_status()
        data = response.json()

        items = (
            data.get("data", {}).get("data", {}).get("comments")
            or data.get("data", {}).get("comments")
            or []
        )

        page_comments = normalize_comments(items, page)
        all_comments.extend(page_comments)
        print(f"    Página {page}: {len(page_comments)} comentários")

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
    if isinstance(items, dict):
        iterable = items.values()
    elif isinstance(items, list):
        iterable = items
    else:
        iterable = []

    for idx, item in enumerate(iterable, start=1):
        item["_page"] = page
        item["_comment_number"] = idx
        item["_custom_comment_id"] = f"{page}_{idx}"
        comments.append(item)

    return comments


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


def get_saved_comment_ids(sheets_service, post_url):
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_DATA_COMMENTS_ID,
            range=f"{SHEET_DATA_COMMENTS}!A:Z"
        ).execute()
        rows = result.get("values", [])
        if len(rows) <= 1:
            return set()
        headers = rows[0]
        if "id" not in headers or "post_url" not in headers:
            return set()
        id_col = headers.index("id")
        url_col = headers.index("post_url")
        saved_ids = set()
        for row in rows[1:]:
            if len(row) > max(id_col, url_col):
                if row[url_col].strip() == post_url.strip():
                    saved_ids.add(row[id_col].strip())
        print(f"    IDs já salvos para {post_url}: {len(saved_ids)}")
        return saved_ids
    except Exception as e:
        print(f"    Aviso ao ler data_comments: {e}")
        return set()


def comments_to_dataframe(comments, post_url, perfil, saved_ids):
    rows = []
    skipped = 0

    for item in comments:
        comment_id = str(item.get("id", ""))
        if comment_id in saved_ids:
            skipped += 1
            continue

        user = item.get("user", {})
        rows.append({
            "post_url": post_url,
            "perfil": perfil,
            "Id Comentário": item.get("_custom_comment_id"),
            "id": comment_id,
            "text": item.get("text"),
            "comment_like_count": item.get("like_count"),
            "child_comment_count": item.get("child_comment_count", 0),
            "created_at": item.get("created_at"),
            "user": json.dumps(user, ensure_ascii=False),
            "username": user.get("username"),
            "id_user": user.get("id"),
            "is_unpublished": item.get("is_unpublished"),
            "pk": user.get("pk"),
            "is_verified": user.get("is_verified")
        })

    print(f"    Comentários novos: {len(rows)} | Já salvos (ignorados): {skipped}")

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df = df.fillna("")
    df["text"] = df["text"].astype(str)
    df["Id Comentário"] = df["Id Comentário"].astype(str)
    df["text_debug"] = df["text"].apply(repr)
    df["tem_emoji"] = df["text"].apply(has_emoji)

    return df


# ==============================
# CLASSIFICAÇÃO GEMINI
# ==============================

def classificar_lote_comentarios(comentarios):
    client = genai.Client(api_key=GEMINI_API_KEY)
    prompt = f"""
Você é um especialista em análise de sentimentos para redes sociais.
Sua tarefa é classificar comentários em 'promotor', 'neutro' ou 'detrator'.

REGRAS CRÍTICAS:
1. Não existe "neutro" ou não demonstra nenhum tipo de comentário.
2. Se o comentário for positivo, elogio ou neutro-positivo (ex: "ok", "gostei", emojis), classifique como 'promotor'.
3. Se houver qualquer reclamação, dúvida técnica, ironia ou crítica, classifique como 'detrator'.

Comentários para análise:
{json.dumps(comentarios, ensure_ascii=False)}
"""

    schema = {
        "type": "object",
        "properties": {
            "results": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "Id Comentário": {"type": "string"},
                        "sentimento_nps": {
                            "type": "string",
                            "enum": ["promotor", "detrator"]
                        },
                        "justificativa": {"type": "string"}
                    },
                    "required": ["Id Comentário", "sentimento_nps", "justificativa"]
                }
            }
        },
        "required": ["results"]
    }

    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
        config=types.GenerateContentConfig(
            response_mime_type="application/json",
            response_json_schema=schema,
            temperature=0.1
        )
    )

    return json.loads(response.text)["results"]


def classificar_dataframe(df):
    resultados = []
    print(f"    Classificando {len(df)} comentários...")

    for i in range(0, len(df), BATCH_SIZE):
        lote = df.iloc[i:i + BATCH_SIZE]
        comentarios_lote = [
            {"Id Comentário": row["Id Comentário"], "text": row["text"]}
            for _, row in lote.iterrows()
        ]

        try:
            classificados = classificar_lote_comentarios(comentarios_lote)
            resultados.extend(classificados)
            print(f"    Lote {i // BATCH_SIZE + 1} OK")
        except Exception as e:
            print(f"    Erro no lote {i // BATCH_SIZE + 1}: {e}")
            for item in comentarios_lote:
                resultados.append({
                    "Id Comentário": item["Id Comentário"],
                    "sentimento_nps": "FALHA_API",
                    "justificativa": str(e)
                })

        time.sleep(2)

    df_result = pd.DataFrame(resultados)
    df_result["Id Comentário"] = df_result["Id Comentário"].astype(str)
    df = df.drop(columns=["sentimento_nps", "justificativa"], errors="ignore")
    df = df.merge(df_result, on="Id Comentário", how="left")

    return df


def save_comments_to_sheets(sheets_service, df):
    df = df.fillna("")

    existing_data = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_DATA_COMMENTS_ID,
        range=f"{SHEET_DATA_COMMENTS}!A:A"
    ).execute()
    existing_rows = existing_data.get("values", [])

    if not existing_rows:
        values = [df.columns.tolist()] + df.astype(str).values.tolist()
        sheets_service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_DATA_COMMENTS_ID,
            range=f"{SHEET_DATA_COMMENTS}!A1",
            valueInputOption="RAW",
            body={"values": values}
        ).execute()
        print(f"    data_comments: {len(df)} linhas inseridas com cabeçalho.")
    else:
        append_values = df.astype(str).values.tolist()
        sheets_service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_DATA_COMMENTS_ID,
            range=f"{SHEET_DATA_COMMENTS}!A:A",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": append_values}
        ).execute()
        print(f"    data_comments: {len(append_values)} linhas adicionadas.")


# ==============================
# EXECUÇÃO PRINCIPAL
# ==============================

def main():
    print("=" * 60)
    print("INICIANDO PIPELINE INSTAGRAM")
    print("=" * 60)

    # Checagem de variáveis de ambiente
    print("\n[CONFIG] Verificando variáveis de ambiente...")
    missing = []
    for var in ["SOCIAVAULT_API_KEY", "GEMINI_API_KEY", "GDRIVE_CREDENTIALS"]:
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
    drive_service, sheets_service = get_google_services()
    print("  Google Services OK")

    # ETAPA 1 — Ler perfis
    print("\n[ETAPA 1] Lendo perfis...")
    profiles = read_profiles(sheets_service)

    if not profiles:
        print("Nenhum perfil para processar. Encerrando.")
        return

    # Carrega códigos já salvos no data_profile (para deduplicação)
    saved_post_codes = get_saved_post_codes(sheets_service)
    print(f"Posts já salvos no data_profile: {len(saved_post_codes)}")

    for profile_entry in profiles:
        handle = profile_entry["profile"]
        print(f"\n{'=' * 60}")
        print(f"PERFIL: {handle}")
        print(f"{'=' * 60}")

        # ETAPA 2 — Extrair posts
        print(f"\n[ETAPA 2] Extraindo posts de @{handle}...")
        try:
            df_posts = fetch_posts(handle)
        except Exception as e:
            print(f"  ERRO ao buscar posts de @{handle}: {e}. Pulando perfil.")
            continue

        if df_posts.empty:
            print(f"  Nenhum post encontrado para @{handle}. Pulando.")
            continue

        # Filtra posts já salvos (deduplicação por code)
        new_posts = df_posts[~df_posts["code"].isin(saved_post_codes.keys())]
        already_saved = df_posts[df_posts["code"].isin(saved_post_codes.keys())]

        print(f"  Posts novos: {len(new_posts)} | Já salvos (ignorados): {len(already_saved)}")

        if not new_posts.empty:
            save_posts_to_sheets(sheets_service, new_posts)
            # Atualiza o dict local com os novos posts
            hoje_str = datetime.now(tz_br).strftime("%Y-%m-%d %H:%M:%S")
            for code in new_posts["code"].tolist():
                saved_post_codes[code] = hoje_str

        # ETAPA 3 — Processar comentários de cada post
        print(f"\n[ETAPA 3] Processando comentários dos posts de @{handle}...")

        for _, post_row in df_posts.iterrows():
            post_url = post_row.get("url", "")
            post_code = post_row.get("code", "")

            if not post_url:
                print(f"  Post sem URL (code={post_code}), pulando.")
                continue

            # Verifica expiração (14 dias desde first_extracted_at)
            first_extracted_at = saved_post_codes.get(post_code, "")
            if is_post_expired(first_extracted_at):
                print(f"  Post expirado (>14 dias): {post_url}. Pulando.")
                continue

            print(f"\n  Post: {post_url}")

            try:
                # Busca IDs já salvos para esse post
                saved_ids = get_saved_comment_ids(sheets_service, post_url)

                # Scraping de comentários
                all_comments = scrape_instagram_comments(post_url, post_url)

                # Filtra novos e transforma
                df_comments = comments_to_dataframe(all_comments, post_url, handle, saved_ids)

                if df_comments.empty:
                    print("    Nenhum comentário novo. Pulando classificação.")
                    continue

                # Classifica com Gemini
                df_comments = classificar_dataframe(df_comments)
                df_comments["data_execucao"] = datetime.now(tz_br).strftime("%Y-%m-%d %H:%M:%S")

                # Salva no data_comments
                save_comments_to_sheets(sheets_service, df_comments)

            except Exception as e:
                print(f"    ERRO ao processar comentários de {post_url}: {e}. Pulando post.")
                continue

    print(f"\n{'=' * 60}")
    print("PIPELINE FINALIZADO COM SUCESSO")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
