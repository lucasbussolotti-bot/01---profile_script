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
 
# Planilha única — abas diferentes
SPREADSHEET_ID = "1cn68TA8_ajbbIOaMofE_7-Vc4_BWfQRMHehrO6SB_Q4"
 
SHEET_HASHTAGS = "Hashtag_Data"    # aba de entrada (leitura)
SHEET_POSTS    = "Hashtag_posts"   # aba de saída (escrita)
 
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
 
    col_index = headers.index("hashtag")
 
    hashtags = []
    for row in rows[1:]:
        if len(row) > col_index:
            tag = row[col_index].strip().lstrip("#")
            if tag:
                hashtags.append(tag)
 
    # Deduplicação
    seen = set()
    unique = []
    for tag in hashtags:
        if tag.lower() not in seen:
            seen.add(tag.lower())
            unique.append(tag)
 
    print(f"{len(unique)} hashtag(s) única(s) encontrada(s): {unique}")
    return unique
 
 
# ==============================
# ETAPA 2 — BUSCAR POSTS POR HASHTAG
# ==============================
 
def fetch_posts_by_hashtag(hashtag):
    """
    Busca posts do TikTok por hashtag via SociaVault.
    Retorna lista de share_urls.
    """
    url = "https://api.sociavault.com/v1/scrape/tiktok/search/hashtag"
    headers = {"X-API-Key": SOCIA_API_KEY}
    params = {
        "hashtag": hashtag,
        "count": POSTS_LIMIT
    }
 
    response = requests.get(url, headers=headers, params=params, timeout=API_TIMEOUT)
    print(f"  Status ({hashtag}): {response.status_code}")
    response.raise_for_status()
 
    data = response.json()
 
    aweme_list = data.get("data", {}).get("aweme_list", {})
 
    # aweme_list pode ser dict (chaves "0", "1"...) ou list
    if isinstance(aweme_list, dict):
        items = list(aweme_list.values())
    elif isinstance(aweme_list, list):
        items = aweme_list
    else:
        print(f"  Aviso: estrutura inesperada de aweme_list: {type(aweme_list)}")
        return []
 
    share_urls = []
    for item in items:
        share_url = (
            item.get("share_info", {}).get("share_url")
            or item.get("share_url")
            or ""
        )
        if share_url:
            share_urls.append(share_url)
 
    print(f"  Posts encontrados para #{hashtag}: {len(share_urls)}")
    return share_urls
 
 
# ==============================
# ETAPA 3 — SALVAR NA PLANILHA
# ==============================
 
def get_existing_urls(sheets_service):
    """Retorna set de URLs já salvas na aba Hastag_posts."""
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
        print(f"  URLs já salvas na planilha: {len(existing)}")
        return existing
    except Exception as e:
        print(f"  Aviso ao ler Hastag_posts: {e}")
        return set()
 
 
def save_posts_to_sheets(sheets_service, rows_to_add):
    """Salva as linhas novas na aba Hastag_posts."""
    if not rows_to_add:
        print("  Nenhuma linha nova para salvar.")
        return
 
    # Verifica se a aba já tem cabeçalho
    existing_data = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_POSTS}!A:A"
    ).execute()
    existing_rows = existing_data.get("values", [])
 
    if not existing_rows:
        # Insere cabeçalho + dados
        header = [["hashtag", "share_url", "run_datetime"]]
        values = header + rows_to_add
        sheets_service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_POSTS}!A1",
            valueInputOption="RAW",
            body={"values": values}
        ).execute()
        print(f"  Hastag_posts: {len(rows_to_add)} linhas inseridas com cabeçalho.")
    else:
        sheets_service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_POSTS}!A:A",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": rows_to_add}
        ).execute()
        print(f"  Hastag_posts: {len(rows_to_add)} novas linhas adicionadas.")
 
 
# ==============================
# EXECUÇÃO PRINCIPAL
# ==============================
 
def main():
    print("=" * 60)
    print("INICIANDO PIPELINE HASHTAG TIKTOK")
    print("=" * 60)
 
    # Verifica variáveis de ambiente
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
 
    # ETAPA 1 — Ler hashtags
    print(f"\n[ETAPA 1] Lendo hashtags da aba '{SHEET_HASHTAGS}'...")
    hashtags = read_hashtags(sheets_service)
 
    if not hashtags:
        print("Nenhuma hashtag para processar. Encerrando.")
        return
 
    # Carrega URLs já salvas para evitar duplicatas
    print(f"\n[DEDUP] Carregando URLs já salvas na aba '{SHEET_POSTS}'...")
    existing_urls = get_existing_urls(sheets_service)
 
    run_datetime = datetime.now(tz_br).strftime("%Y-%m-%d %H:%M:%S")
    all_new_rows = []
 
    # ETAPA 2 — Buscar posts por hashtag
    for hashtag in hashtags:
        print(f"\n{'=' * 60}")
        print(f"HASHTAG: #{hashtag}")
        print(f"{'=' * 60}")
 
        try:
            share_urls = fetch_posts_by_hashtag(hashtag)
        except Exception as e:
            print(f"  ERRO ao buscar #{hashtag}: {e}. Pulando.")
            continue
 
        novos = 0
        for url in share_urls:
            if url in existing_urls:
                continue  # já salvo, ignora
            all_new_rows.append([hashtag, url, run_datetime])
            existing_urls.add(url)  # atualiza controle local
            novos += 1
 
        print(f"  Novos posts para #{hashtag}: {novos}")
        time.sleep(1)  # respeita rate limit da API
 
    # ETAPA 3 — Salvar tudo de uma vez
    print(f"\n[ETAPA 3] Salvando {len(all_new_rows)} linhas novas na planilha...")
 
    # Reconecta antes de salvar (evita timeout SSL por inatividade)
    sheets_service = get_google_services()
    save_posts_to_sheets(sheets_service, all_new_rows)
 
    print(f"\n{'=' * 60}")
    print("PIPELINE FINALIZADO COM SUCESSO")
    print(f"{'=' * 60}")
 
 
if __name__ == "__main__":
    main()
