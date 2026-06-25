import os
import io
import re
import json
import sys
import builtins
from datetime import datetime

import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import pytz

# Força flush imediato em todos os prints (igual ao script de referência)
_original_print = builtins.print
def print(*args, **kwargs):
    kwargs["flush"] = True
    _original_print(*args, **kwargs)

tz_br = pytz.timezone("America/Sao_Paulo")

# ==============================
# CONFIGURAÇÕES
# ==============================

DRIVE_FOLDER_ID = "1k8NDz3qxQ9ffZzkS2EsWT1tNSk3ghklU"
SPREADSHEET_ID = "1FnauIqLuTe1c2N8Z-HQPy8wambQzBhpbLJY24JMCMNY"  # planilha de SAÍDA
SHEET_NAME = "Hoja 2"

BASELINE_SPREADSHEET_ID = "1qMl_c5KgCDb0QCr4wOPnjnhdnmrHrDw3Fqs9T5F1wyg"  # planilha de baseline (ENTRADA)
BASELINE_SHEET_RANGE = "A:Z"  # primeira aba da planilha de baseline

SOURCE_TAB = "Winclap_Organic"
HEADER_SKIPROWS = 2  # linha 1 = info do dashboard, linha 2 = vazia, linha 3 = cabeçalho real

FILENAME_PATTERN = re.compile(r"^(\d{2})(\d{2})(\d{4})\.xlsx$")  # DDMMAAAA.xlsx

KEY_COLUMN = "Organic_ID"

BASELINE_COLUMNS_MAP = {
    "Engagement Rate": "Baseline Engagement Rate",
    "Eng. Rate Neg. Com.": "Baseline Neg. Com.",
    "Video Views": "Baseline Video Views",
    "Shares": "Baseline Shares",
    "Post Likes And Reactions": "Baseline Post Likes And Reactions",
    "Post Comments / X Replies (SUM)": "Baseline Post Comments",
}



# ==============================
# GOOGLE SERVICES
# ==============================

def get_google_services():
    creds_json = json.loads(os.environ.get("GDRIVE_CREDENTIALS"))
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = service_account.Credentials.from_service_account_info(
        creds_json, scopes=scopes
    )
    drive_service = build("drive", "v3", credentials=creds)
    sheets_service = build("sheets", "v4", credentials=creds)
    return drive_service, sheets_service


# ==============================
# ETAPA 1 — ACHAR O ARQUIVO MAIS RECENTE NO DRIVE
# ==============================

def find_latest_file(drive_service):
    query = f"'{DRIVE_FOLDER_ID}' in parents and trashed = false"
    results = drive_service.files().list(
        q=query,
        fields="files(id, name)",
        pageSize=1000,
    ).execute()
    files = results.get("files", [])

    candidates = []
    for f in files:
        m = FILENAME_PATTERN.match(f["name"])
        if not m:
            continue
        dd, mm, yyyy = m.groups()
        try:
            file_date = datetime(int(yyyy), int(mm), int(dd))
        except ValueError:
            continue
        candidates.append((file_date, f))

    if not candidates:
        raise RuntimeError(
            "Nenhum arquivo no padrão DDMMAAAA.xlsx foi encontrado na pasta do Drive."
        )

    candidates.sort(key=lambda x: x[0])
    latest_date, latest_file = candidates[-1]
    print(f"Arquivo mais recente encontrado: {latest_file['name']} (data {latest_date.date()})")
    return latest_file["id"], latest_file["name"]


def download_file(drive_service, file_id, local_path):
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    with open(local_path, "wb") as f:
        f.write(fh.getvalue())
    print(f"Arquivo baixado em: {local_path}")


# ==============================
# ETAPA 2 — LER E TRATAR A ABA ORGANIC
# ==============================

def extract_organic_id(row):
    url = row["Permalink (EXTERNAL_VALUE)"]
    network = row["Social Network"]
    category = row["Outbound Message Category"]

    if not isinstance(url, str):
        return None

    if network == "Instagram" and category == "Reels":
        match = re.search(r"/reel/([^/]+)/", url)
    elif network == "Instagram" and category == "Story":
        match = re.search(r"/stories/[^/]+/(\d+)", url)
    elif network == "Instagram" and category == "Update":
        match = re.search(r"/p/([^/]+)/", url)
    elif network == "TikTok" and category == "Update":
        match = re.search(r"/video/(\d+)", url)
    else:
        return None

    return match.group(1) if match else None


def read_baseline(sheets_service):
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=BASELINE_SPREADSHEET_ID,
        range=BASELINE_SHEET_RANGE,
    ).execute()
    rows = result.get("values", [])
    if not rows:
        raise RuntimeError("Planilha de baseline está vazia.")

    headers = rows[0]
    data_rows = rows[1:]
    # Normaliza largura das linhas (Sheets API corta colunas vazias no fim)
    data_rows = [r + [""] * (len(headers) - len(r)) for r in data_rows]
    baseline = pd.DataFrame(data_rows, columns=headers)

    needed_cols = ["Concatenate"] + list(BASELINE_COLUMNS_MAP.keys())
    missing = [c for c in needed_cols if c not in baseline.columns]
    if missing:
        raise RuntimeError(f"Colunas faltando na planilha de baseline: {missing}")

    baseline = baseline[needed_cols].rename(columns=BASELINE_COLUMNS_MAP)

    # Converte colunas numéricas (vêm como string da Sheets API)
    numeric_cols = list(BASELINE_COLUMNS_MAP.values())
    for col in numeric_cols:
        baseline[col] = pd.to_numeric(
            baseline[col].astype(str).str.replace(",", ".").str.replace("%", ""),
            errors="coerce",
        )

    print(f"Baseline lido: {len(baseline)} linhas.")
    return baseline


def classify_boost(row):
    if row["Delta Eng. Rate (p.p.)"] > 0 and row["Delta Neg. Sent. (p.p.)"] <= 0:
        return "Boost"
    elif row["Delta Eng. Rate (p.p.)"] > 0 and row["Delta Neg. Sent. (p.p.)"] > 0:
        return "Review"
    else:
        return "No Boost"


def read_organic_sheet(local_path, baseline_df):
    df = pd.read_excel(local_path, sheet_name=SOURCE_TAB, header=HEADER_SKIPROWS)

    # Descarta Replies
    df = df[df["Outbound Message Category"] != "Reply"].copy()

    # Extrai Organic_ID a partir da URL
    df["Organic_ID"] = df.apply(extract_organic_id, axis=1)

    # Mantém só uma linha por link orgânico
    df = df.drop_duplicates(subset="Permalink (EXTERNAL_VALUE)")

    # Descarta linhas sem Organic_ID (URL não reconhecida pelo padrão)
    df = df[df["Organic_ID"].notna()].copy()

    # Métricas calculadas
    df["Engagement Rate"] = df.apply(
        lambda row: row["Post Likes And Reactions (SUM)"] / row["Video Views (SUM)"]
        if row["Video Views (SUM)"] != 0 else 0,
        axis=1,
    )

    def safe_pct(row, numerator_col):
        total = (
            row["Count of Negative Comments (SUM)"]
            + row["Count of Positive Comments (SUM)"]
            + row["Count of Neutral Comments (SUM)"]
        )
        if row["Post Comments (SUM)"] == 0 or total == 0:
            return 0
        return row[numerator_col] / total

    df["Sent. Negativo (%)"] = df.apply(lambda r: safe_pct(r, "Count of Negative Comments (SUM)"), axis=1)
    df["Sent. Positivo (%)"] = df.apply(lambda r: safe_pct(r, "Count of Positive Comments (SUM)"), axis=1)
    df["Sent. Neutro (%)"] = df.apply(lambda r: safe_pct(r, "Count of Neutral Comments (SUM)"), axis=1)

    # Join com baseline
    df["Concatenate"] = (
        df[["Account", "Country of Origin (Account)", "Outbound Message Category"]]
        .fillna("")
        .agg("".join, axis=1)
    )
    df = df.merge(baseline_df, on="Concatenate", how="left")

    # Deltas vs baseline e classificação
    df["Delta Eng. Rate (p.p.)"] = (df["Engagement Rate"] - df["Baseline Engagement Rate"]) * 100
    df["Delta Neg. Sent. (p.p.)"] = (df["Sent. Negativo (%)"] - df["Baseline Neg. Com."]) * 100
    df["Accionable"] = df.apply(classify_boost, axis=1)

    if "Published Date" in df.columns:
        df["Published Date"] = pd.to_datetime(df["Published Date"]).dt.strftime("%Y-%m-%d %H:%M:%S")

    df = df.fillna("")

    print(f"Linhas tratadas da aba {SOURCE_TAB} (sem Reply, deduplicadas por Permalink): {len(df)}")
    return df


# ==============================
# ETAPA 3 — UPSERT NO GOOGLE SHEETS
# ==============================

def read_existing_sheet(sheets_service):
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A:ZZ",
    ).execute()
    rows = result.get("values", [])

    if not rows:
        return [], {}

    headers = rows[0]
    key_col = headers.index(KEY_COLUMN) if KEY_COLUMN in headers else 0

    key_to_row_idx = {}
    for i, row in enumerate(rows[1:], start=2):  # linha 2 = primeira linha de dados (1-indexed no Sheets)
        if len(row) > key_col and row[key_col]:
            key_to_row_idx[row[key_col]] = i

    return headers, key_to_row_idx


def ensure_header(sheets_service, existing_headers, sheet_columns):
    if existing_headers:
        return existing_headers
    sheets_service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A1",
        valueInputOption="RAW",
        body={"values": [sheet_columns]},
    ).execute()
    print("Cabeçalho criado na planilha.")
    return sheet_columns


def upsert_rows(sheets_service, df):
    # Coloca Organic_ID na frente e adiciona timestamp no final
    other_cols = [c for c in df.columns if c != KEY_COLUMN]
    sheet_columns = [KEY_COLUMN] + other_cols + ["Last Updated At"]

    existing_headers, key_to_row_idx = read_existing_sheet(sheets_service)
    headers = ensure_header(sheets_service, existing_headers, sheet_columns)

    now_str = datetime.now(tz_br).strftime("%Y-%m-%d %H:%M:%S")

    update_data = []  # [{"range": "...", "values": [[...]]}]
    append_rows = []

    for _, row in df.iterrows():
        key = str(row[KEY_COLUMN]).strip()
        row_values = [row.get(col, "") for col in sheet_columns[:-1]] + [now_str]
        row_values = [str(v) for v in row_values]

        if key in key_to_row_idx:
            sheet_row = key_to_row_idx[key]
            end_col_letter = _col_letter(len(headers))
            update_data.append({
                "range": f"{SHEET_NAME}!A{sheet_row}:{end_col_letter}{sheet_row}",
                "values": [row_values],
            })
        else:
            append_rows.append(row_values)

    if update_data:
        sheets_service.spreadsheets().values().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"valueInputOption": "RAW", "data": update_data},
        ).execute()
        print(f"Linhas atualizadas: {len(update_data)}")

    if append_rows:
        sheets_service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A:A",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": append_rows},
        ).execute()
        print(f"Linhas novas inseridas: {len(append_rows)}")

    if not update_data and not append_rows:
        print("Nenhuma linha para atualizar ou inserir.")


def _col_letter(n):
    letters = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


# ==============================
# EXECUÇÃO PRINCIPAL
# ==============================

def main():
    print("=" * 60)
    print("INICIANDO ATUALIZAÇÃO DE STATUS DE POSTS (WINCLAP ORGANIC)")
    print("=" * 60)

    missing = [v for v in ["GDRIVE_CREDENTIALS"] if not os.environ.get(v)]
    if missing:
        print(f"Variáveis de ambiente faltando: {missing}. Encerrando.")
        sys.exit(1)

    drive_service, sheets_service = get_google_services()

    file_id, file_name = find_latest_file(drive_service)
    local_path = f"/tmp/{file_name}"
    download_file(drive_service, file_id, local_path)

    baseline_df = read_baseline(sheets_service)
    df = read_organic_sheet(local_path, baseline_df)
    upsert_rows(sheets_service, df)

    print("=" * 60)
    print("FINALIZADO COM SUCESSO")
    print("=" * 60)


if __name__ == "__main__":
    main()
