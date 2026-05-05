import requests
import pandas as pd
from datetime import datetime
import pytz

# CONFIG
SOCIA_API_KEY = "SUA_API_KEY"
POSTS_LIMIT = 12
API_TIMEOUT = 30

tz_br = pytz.timezone("America/Sao_Paulo")


# ─────────────────────────────────────────────
# 🔹 FUNÇÃO 1: BUSCA DADOS DO PERFIL
# ─────────────────────────────────────────────
def fetch_profile(handle):
    url = "https://api.sociavault.com/v1/scrape/instagram/profile"
    headers = {"X-API-Key": SOCIA_API_KEY}
    params = {"handle": handle}

    response = requests.get(url, headers=headers, params=params, timeout=API_TIMEOUT)
    print(f"Status profile ({handle}): {response.status_code}")
    response.raise_for_status()

    data = response.json()

    user = data.get("data", {}).get("user", {})

    return {
        "followers_count": user.get("edge_followed_by", {}).get("count", ""),
        "following_count": user.get("edge_follow", {}).get("count", ""),
        "total_posts_count": user.get("edge_owner_to_timeline_media", {}).get("count", ""),
        "username": user.get("username", handle)
    }


# ─────────────────────────────────────────────
# 🔹 FUNÇÃO 2: BUSCA POSTS
# ─────────────────────────────────────────────
def fetch_posts(handle):
    url = "https://api.sociavault.com/v1/scrape/instagram/posts"
    headers = {"X-API-Key": SOCIA_API_KEY}
    params = {"handle": handle, "limit": POSTS_LIMIT}

    response = requests.get(url, headers=headers, params=params, timeout=API_TIMEOUT)
    print(f"Status posts ({handle}): {response.status_code}")
    response.raise_for_status()

    json_data = response.json()
    items = json_data.get("data", {}).get("items", {})

    # 🔥 NOVO: busca dados do perfil separado
    profile_data = fetch_profile(handle)

    run_datetime = datetime.now(tz_br).strftime("%Y-%m-%d %H:%M:%S")

    rows = []

    if isinstance(items, dict):
        iterable = list(items.values())
    elif isinstance(items, list):
        iterable = items
    else:
        iterable = []

    iterable = iterable[:POSTS_LIMIT]

    for item in iterable:
        username_shared = item.get("user", {}).get("username", profile_data["username"])
        code = item.get("code")
        taken_at = item.get("taken_at")

        post_url = item.get("url")
        if not post_url and code:
            post_url = f"https://www.instagram.com/p/{code}/"

        media_type = item.get("media_type")
        comment_count = item.get("comment_count")
        like_count = item.get("like_count")
        play_count = item.get("play_count")

        # imagem
        preview_image_url = None
        image_versions2 = item.get("image_versions2", {})
        candidates = image_versions2.get("candidates", {})

        if isinstance(candidates, dict):
            best = candidates.get("0") or next(iter(candidates.values()), {})
            preview_image_url = best.get("url")
        elif isinstance(candidates, list) and len(candidates) > 0:
            preview_image_url = candidates[0].get("url")

        # first frame (vídeo)
        first_frame_url = None
        additional_candidates = image_versions2.get("additional_candidates", {})
        if isinstance(additional_candidates, dict):
            first_frame_url = additional_candidates.get("first_frame", {}).get("url")

        rows.append({
            "run_datetime": run_datetime,
            "Plataform": "Instagram",
            "username": handle,
            "username_shared": username_shared,

            # 🔥 DADOS DO PERFIL (AGORA FUNCIONA)
            "followers_count": profile_data["followers_count"],
            "following_count": profile_data["following_count"],
            "total_posts_count": profile_data["total_posts_count"],

            # POSTS
            "code": code,
            "taken_at": taken_at,
            "url": post_url,
            "media_type": media_type,
            "comment_count": comment_count,
            "like_count": like_count,
            "play_count": play_count,
            "preview_image_url": preview_image_url,
            "first_frame_url": first_frame_url,
            "first_extracted_at": run_datetime
        })

    df = pd.DataFrame(rows)
    df = df.fillna("")

    print(f"Posts extraídos: {len(df)}")
    return df


# ─────────────────────────────────────────────
# 🔹 EXECUÇÃO
# ─────────────────────────────────────────────
if __name__ == "__main__":
    handle = "suramu_sushi"

    df = fetch_posts(handle)

    print(df.head())

    # salvar local (opcional)
    df.to_csv("instagram_posts.csv", index=False)
