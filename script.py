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

POST_URL = "https://www.instagram.com/p/DEXr0A7u48L"
SHORTCODE = "https://www.instagram.com/reels/DEXr0A7u48L"

COMMENTS_LIMIT = 100
BATCH_SIZE = 20

SPREADSHEET_NAME = "comments_classificados"
SHEET_NAME = "Sheet1"
FOLDER_ID = "1OGQOSc23ajvUJ8r0AL6UVZorV87Lws85"


# ==============================
# SCRAPING DE COMENTÁRIOS
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
        print(f"Total acumulado: {len(all_comments)}")

        cursor = (
            data.get("data", {}).get("data", {}).get("cursor")
            or data.get("data", {}).get("cursor")
        )

        if not cursor:
            print("Paginação finalizada.")
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


def comments_to_dataframe(comments):
    rows = []
    for item in comments:
        user = item.get("user", {})
        rows.append({
            "Id Comentário": item.get("_custom_comment_id"),
            "id": item.get("id"),
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
# GOOGLE SHEETS
# ==============================

def get_google_services():
    # ✅ Lê credenciais da variável de ambiente em vez do arquivo .json
    creds_json = json.loads(os.environ.get("GDRIVE_CREDENTIALS"))
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = service_account.Credentials.from_service_account_info(
        creds_json,
        scopes=scopes
    )
    drive_service = build("drive", "v3", credentials=creds)
    sheets_service = build("sheets", "v4", credentials=creds)
    return drive_service, sheets_service


def get_or_create_spreadsheet(drive_service):
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
    if files:
        print(f"Planilha existente encontrada: {SPREADSHEET_NAME}")
        return files[0]["id"]

    file_metadata = {
        "name": SPREADSHEET_NAME,
        "mimeType": "application/vnd.google-apps.spreadsheet",
        "parents": [FOLDER_ID]
    }
    created_file = drive_service.files().create(
        body=file_metadata,
        fields="id",
        supportsAllDrives=True
    ).execute()

    print(f"Planilha criada no Drive: {created_file['id']}")
    return created_file["id"]


def send_dataframe_to_sheets(sheets_service, spreadsheet_id, df):
    df = df.fillna("")
    values = [df.columns.tolist()] + df.astype(str).values.tolist()

    existing_data = sheets_service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{SHEET_NAME}!A:A"
    ).execute()

    existing_rows = existing_data.get("values", [])

    if not existing_rows:
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{SHEET_NAME}!A1",
            valueInputOption="RAW",
            body={"values": values}
        ).execute()
        print("Dados inseridos com cabeçalho.")
    else:
        append_values = df.astype(str).values.tolist()
        sheets_service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"{SHEET_NAME}!A:A",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": append_values}
        ).execute()
        print("Dados adicionados ao final da planilha.")


# ==============================
# EXECUÇÃO PRINCIPAL
# ==============================

all_comments = scrape_instagram_comments()

with open("comments.json", "w", encoding="utf-8") as f:
    json.dump(all_comments, f, indent=4, ensure_ascii=False)

print("JSON consolidado salvo em comments.json")

df = comments_to_dataframe(all_comments)
print(df[["Id Comentário", "text", "text_debug", "tem_emoji"]].head(30))

df = classificar_dataframe(df)

tz_br = pytz.timezone("America/Sao_Paulo")
df["data_execucao"] = datetime.now(tz_br).strftime("%Y-%m-%d %H:%M:%S")

print("\n--- Classificação finalizada ---")
print(df[["text", "sentimento_nps", "justificativa"]].head())

drive_service, sheets_service = get_google_services()
spreadsheet_id = get_or_create_spreadsheet(drive_service)

send_dataframe_to_sheets(
    sheets_service=sheets_service,
    spreadsheet_id=spreadsheet_id,
    df=df
)

print("\n--- Processo finalizado com sucesso ---")
