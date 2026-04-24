import requests
import pandas as pd
import json
import os
from datetime import datetime

from googleapiclient.discovery import build
from google.oauth2 import service_account

# ==============================
# 1. API Sociavault
# ==============================
api_key = os.environ.get("SOCIAVAULT_API_KEY")
headers = {"X-API-Key": api_key}

url = "https://api.sociavault.com/v1/scrape/instagram/posts"
params = {
    "handle": "natgeo",
    "limit": 50
}

response = requests.get(url, params=params, headers=headers)
print("Status code:", response.status_code)
print("Resposta:", response.text[:500])

response.raise_for_status()
json_data = response.json()

items = json_data.get("data", {}).get("items", {})
top_username = json_data.get("data", {}).get("user", {}).get("username")

# Data e hora da execução do script
run_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

rows = []

# Trata items como dict ou list
if isinstance(items, dict):
    iterable = items.values()
elif isinstance(items, list):
    iterable = items
else:
    iterable = []

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
        "first_frame_url": first_frame_url
    })

df = pd.DataFrame(rows)

# Remove NaN para não quebrar o payload do Google Sheets
df = df.fillna("")

print(df.head())
print(f"Total de linhas: {len(df)}")

# ==============================
# 2. Autenticação Google
# ==============================
creds_json = json.loads(os.environ.get("GDRIVE_CREDENTIALS"))
print("Service account:", creds_json.get("client_email"))

creds = service_account.Credentials.from_service_account_info(
    creds_json,
    scopes=[
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets"
    ]
)

drive_service = build("drive", "v3", credentials=creds)
sheets_service = build("sheets", "v4", credentials=creds)

# ==============================
# 3. Configuração da planilha
# ==============================
SPREADSHEET_NAME = f"instagram_posts"
SHEET_NAME = "Sheet1"
FOLDER_ID = "1OGQOSc23ajvUJ8r0AL6UVZorV87Lws85"

# ==============================
# 4. Procurar planilha existente
# ==============================
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

existing_files = existing.get("files", [])

if existing_files:
    spreadsheet_id = existing_files[0]["id"]
    print(f"Planilha existente encontrada: {SPREADSHEET_NAME} ({spreadsheet_id})")
else:
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

    spreadsheet_id = created_file["id"]
    print(f"Planilha criada direto no Drive com sucesso! ID: {spreadsheet_id}")

# ==============================
# 5. Preparar dados para envio
# ==============================
values = [df.columns.tolist()] + df.astype(str).values.tolist()

# ==============================
# 6. Verificar se a planilha já tem conteúdo
# ==============================
existing_data = sheets_service.spreadsheets().values().get(
    spreadsheetId=spreadsheet_id,
    range=f"{SHEET_NAME}!A:A"
).execute()

existing_rows = existing_data.get("values", [])

if not existing_rows:
    # Planilha vazia -> escreve cabeçalho + dados
    sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{SHEET_NAME}!A1",
        valueInputOption="RAW",
        body={"values": values}
    ).execute()
    print("Dados inseridos na nova planilha com cabeçalho.")
else:
    # Planilha já tem dados -> adiciona só as linhas no final
    append_values = df.astype(str).values.tolist()

    sheets_service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"{SHEET_NAME}!A:A",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": append_values}
    ).execute()
    print("Dados adicionados ao final da planilha com sucesso!")
