import json
import time
import asyncio
import random
import re
import requests
from datetime import datetime, timezone
from bs4 import BeautifulSoup

USER_AGENT = "Mozilla/5.0 (compatible; KogamaXPWatcher/2.0)"
GAIN_INTERVAL = 180
GAIN_LOOPS = 10
PERIODIC_INTERVAL = 3600

def load_config():
    with open("config.json", "r", encoding="utf-8") as f:
        return json.load(f)

def fetch_html(url):
    r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=20)
    r.raise_for_status()
    return r.text

def normalize_int(v):
    if v is None:
        return None
    d = re.sub(r"[^\d\-]", "", str(v))
    return int(d) if d else None

def extract_bootstrap(html):
    m = re.search(r"options\.bootstrap\s*=\s*({)", html)
    if not m:
        return None
    i = m.start(1)
    depth = 0
    for j in range(i, len(html)):
        if html[j] == "{":
            depth += 1
        elif html[j] == "}":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(html[i:j+1].replace("undefined", "null"))
                except:
                    return None
    return None

def parse_profile(html):
    soup = BeautifulSoup(html, "html.parser")
    title = soup.find("h1").get_text(strip=True) if soup.find("h1") else None
    xp_dom = soup.select_one("div._3WDmu.vKjpS ._2ydTi")
    xp_dom = normalize_int(xp_dom.get_text(strip=True)) if xp_dom else None
    img = soup.select_one("img._3rgDV, div._4OXDk img")
    thumb = img["src"] if img and img.get("src") else None
    if thumb and thumb.startswith("//"):
        thumb = "https:" + thumb
    return title, xp_dom, thumb

def iso_ts(v):
    try:
        return int(datetime.fromisoformat(v).replace(tzinfo=timezone.utc).timestamp())
    except:
        return None

def build_embed(profile, before, after, color):
    gain = after - before
    sign = "+" if gain >= 0 else ""
    return {
        "title": profile["title"],
        "url": profile["url"],
        "color": color,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "thumbnail": {"url": profile["thumb"]} if profile["thumb"] else None,
        "fields": [
            {"name": "XP", "value": f"{before:,} → {after:,}", "inline": False},
            {"name": "Δ", "value": f"{sign}{gain:,}", "inline": True},
            *(
                [{"name": "Level", "value": str(profile["level"]), "inline": True}]
                if profile["level"] is not None else []
            ),
            *(
                [{"name": "Rank", "value": str(profile["rank"]), "inline": True}]
                if profile["rank"] is not None else []
            ),
            *(
                [{"name": "Last ping", "value": f"<t:{profile['last_ping']}:R>", "inline": False}]
                if profile["last_ping"] else []
            )
        ]
    }

def post(webhook, embed, mention=None):
    payload = {"embeds": [embed]}
    if mention:
        payload["content"] = mention
    requests.post(webhook, json=payload, timeout=20)

async def monitor_profile(cfg, state):
    pid = state["id"]
    color = state["color"]
    url = f"https://www.kogama.com/profile/{pid}/"
    loops_left = 0

    while True:
        html = fetch_html(url)
        title, xp_dom, thumb = parse_profile(html)
        opts = extract_bootstrap(html)

        obj = (opts or {}).get("object") or {}
        xp = normalize_int(obj.get("xp")) or xp_dom
        level = obj.get("level")
        rank = obj.get("leaderboard_rank")
        last_ping = iso_ts(obj.get("last_ping")) if obj.get("last_ping") else None

        if xp is not None:
            if state["xp"] is None:
                state["xp"] = xp

            elif xp != state["xp"]:
                embed = build_embed(
                    {
                        "title": title or f"Profile {pid}",
                        "url": url,
                        "thumb": thumb,
                        "level": level,
                        "rank": rank,
                        "last_ping": last_ping
                    },
                    state["xp"],
                    xp,
                    color
                )
                mention = f"<@{cfg['mention_id']}>" if abs(xp - state["xp"]) > 500 else None
                post(cfg["webhook"], embed, mention)
                state["xp"] = xp
                loops_left = GAIN_LOOPS

            elif loops_left > 0:
                loops_left -= 1

        await asyncio.sleep(GAIN_INTERVAL if loops_left else PERIODIC_INTERVAL)

async def main():
    cfg = load_config()
    states = [
        {
            "id": p["id"],
            "xp": None,
            "color": random.randint(0x2F3136, 0xFFFFFF)
        }
        for p in cfg["profiles"]
    ]
    await asyncio.gather(*(monitor_profile(cfg, s) for s in states))

asyncio.run(main())
