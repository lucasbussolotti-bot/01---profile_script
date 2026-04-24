# ==============================
# SETUP
# ==============================

import os
import re
import json
import time
import requests
import pandas as pd

from datetime import datetime
from google import genai
from google.genai import types
from google.oauth2 import service_account
from googleapiclient.discovery import build
import pytz


# ==============================
# CONFIGURAÇÕES
# ==============================

SOCIA_API_KEY = os.environ.get("SOCIAVAULT_API_KEY")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

COMMENTS_LIMIT = 100
BATCH_SIZE = 20

SPREADSHEET_NAME = "comments_classificados"
SHEET_RESULTS = "Sheet1"
SHEET_INPUTS = "inputs"
FOLDER_ID = "1OGQOSc23ajvUJ8r0AL6UVZorV87Lws85"

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


def get_spreadsheet_id(drive_service):
    query = (
        f"name='{SPREADSHEET_NAME}' and "
        f"mimeType='application/vnd.google-apps.spreadsheet' and "
        f"'{FOLDER_ID}' in parents and trashed=false"
    )
    existing = drive_service.files().list(
        q=query,
        fields="files(id, name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()
    files = existing.get("files", [])
    if not files:
        raise Exception(f"Planilha '{SPREADSHEET_NAME}' não encontrada.")
    return files[0]["id"]


# ==============================
# LER ABA INPUTS
# ==============================

def read_inputs(sheets_service, spreadsheet_id):
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{SHEET_INPUTS}!A:E"
    ).execute()

    rows = result.get("values", [])
    if len(rows) <= 1:
        print("Nenhuma entrada na aba inputs.")
        return []

    headers = rows[0]
    inputs = []
    for i, row in enumerate(rows[1:], start=2):  # linha real no Sheets (começa em 2)
        while len(row) < 5:
            row.append("")
        entry = {
            "row_index": i,
            "url": row[0].strip(),
            "perfil": row[1].strip(),
            "data_insercao": row[2].strip(),
            "status": row[3].strip().lower(),
            "ultima_execucao": row[4].strip()
        }
        inputs.append(entry)

    return inputs


def filter_active_inputs(inputs):
    hoje = datetime.now(tz_br).date()
    ativos = []

    for entry in inputs:
        if entry["status"] == "expirado":
            continue

        try:
            data_insercao = datetime.strptime(entry["data_insercao"], "%Y-%m-%d").date()
        except ValueError:
            print(f"Data inválida para {entry['url']}, pulando.")
            continue

        dias_ativos = (hoje - data_insercao).days
        if dias_ativos > 14:
            print(f"URL expirada: {entry['url']}")
            entry["status"] = "expirado"
            ativos.append(entry)  # ainda adiciona para atualizar o status
        else:
            entry["status"] = "ativo"
            ativos.append(entry)

    return ativos


# ==============================
# ATUALIZAR ABA INPUTS
# ==============================

def update_input_row(sheets_service, spreadsheet_id, row_index, status, ultima_execucao):
    range_status = f"{SHEET_INPUTS}!D{row_index}"
    range_execucao = f"{SHEET_INPUTS}!E{row_index}"

    sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=range_status,
        valueInputOption="RAW",
        body={"values": [[status]]}
    ).execute()

    sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=range_execucao,
        valueInputOption="RAW",
        body={"values": [[ultima_execucao]]}
    ).execute()


# ==============================
# BUSCAR IDs JÁ SALVOS
# ==============================

def get_saved_ids(sheets_service, spreadsheet_id, post_url):
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{SHEET_RESULTS}!A:Z"
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

    print(f"IDs já salvos para {post_url}: {len(saved_ids)}")
    return saved_ids


# ==============================
# SCRAPING DE COMENTÁRIOS
# ==============================

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


# ==============================
# TRANSFORMAR EM DATAFRAME
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

    print(f"Comentários novos: {len(rows)} | Já salvos (ignorados): {skipped}")

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

client = genai.Client(api_key=GEMINI_API_KEY)


def classificar_lote_comentarios(comentarios):
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
    print(f"Iniciando classificação de {len(df)} comentários...")

    for i in range(0, len(df), BATCH_SIZE):
        lote = df.iloc[i:i + BATCH_SIZE]
        comentarios_lote = [
            {"Id Comentário": row["Id Comentário"], "text": row["text"]}
            for _, row in lote.iterrows()
        ]

        try:
            classificados = classificar_lote_comentarios(comentarios_lote)
            resultados.extend(classificados)
            print(f"Lote {i // BATCH_SIZE + 1} OK")
        except Exception as e:
            print(f"Erro no lote {i // BATCH_SIZE + 1}: {e}")
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


# ==============================
# SALVAR NO SHEETS
# ==============================

def send_dataframe_to_sheets(sheets_service, spreadsheet_id, df):
    df = df.fillna("")

    existing_data = sheets_service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{SHEET_RESULTS}!A:A"
    ).execute()

    existing_rows = existing_data.get("values", [])

    if not existing_rows:
        values = [df.columns.tolist()] + df.astype(str).values.tolist()
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{SHEET_RESULTS}!A1",
            valueInputOption="RAW",
            body={"values": values}
        ).execute()
        print("Dados inseridos com cabeçalho.")
    else:
        append_values = df.astype(str).values.tolist()
        sheets_service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"{SHEET_RESULTS}!A:A",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": append_values}
        ).execute()
        print(f"{len(append_values)} linhas adicionadas.")


# ==============================
# EXECUÇÃO PRINCIPAL
# ==============================

drive_service, sheets_service = get_google_services()
spreadsheet_id = get_spreadsheet_id(drive_service)

inputs = read_inputs(sheets_service, spreadsheet_id)
active_inputs = filter_active_inputs(inputs)

if not active_inputs:
    print("Nenhuma URL ativa para processar.")
else:
    for entry in active_inputs:
        post_url = entry["url"]
        perfil = entry["perfil"]
        hoje_str = datetime.now(tz_br).strftime("%Y-%m-%d %H:%M:%S")

        print(f"\n{'='*50}")
        print(f"Processando: {perfil} | {post_url}")
        print(f"{'='*50}")

        # Atualiza status imediatamente (expirado ou ativo)
        update_input_row(
            sheets_service, spreadsheet_id,
            entry["row_index"], entry["status"], hoje_str
        )

        if entry["status"] == "expirado":
            print("URL expirada, status atualizado.")
            continue

        # Busca IDs já salvos
        saved_ids = get_saved_ids(sheets_service, spreadsheet_id, post_url)

        # Scraping
        shortcode = post_url  # a API aceita a URL completa como shortcode também
        all_comments = scrape_instagram_comments(post_url, shortcode)

        # Filtra novos e transforma
        df = comments_to_dataframe(all_comments, post_url, perfil, saved_ids)

        if df.empty:
            print("Nenhum comentário novo. Pulando classificação.")
            continue

        # Classifica
        df = classificar_dataframe(df)
        df["data_execucao"] = hoje_str

        # Salva
        send_dataframe_to_sheets(sheets_service, spreadsheet_id, df)

        print(f"Concluído: {perfil}")

print("\n--- Processo finalizado com sucesso ---")
