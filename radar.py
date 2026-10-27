#!/usr/bin/env python3
"""
Roblox Trend-Radar (GitHub-Version)
===================================

Läuft automatisch jede Stunde über GitHub Actions (siehe .github/workflows/radar.yml).
Speichert die Messwerte im Ordner "data" und baut die Webseite im Ordner "site".

Lokal testen (optional, braucht Python 3.9+):
    python radar.py --data data --site site     # messen + Seite bauen
    python radar.py --demo                      # Vorschau mit erfundenen Daten in "vorschau/"
"""

import argparse
import base64
import gzip
import json
import math
import os
import random
import sys
import time
import uuid
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

EXPLORE = "https://apis.roblox.com/explore-api/v1"
GAMES_API = "https://games.roblox.com/v1/games"
VOTES_API = "https://games.roblox.com/v1/games/votes"
ICONS_API = "https://thumbnails.roblox.com/v1/games/icons"

TRACK_MIN_PLAYERS = 20   # Spiele, die 3 Tage lang darunter bleiben, werden nicht mehr gemessen
KEEP_DAYS = 40           # so lange werden Messwerte aufbewahrt
HISTORY_DAYS = 31        # so viele Tage Verlauf gehen ins Dashboard
SLOTS = HISTORY_DAYS + 2
BATCH = 50
PAUSE = 0.35
MAX_SORT_PAGES = 15
UA = "Mozilla/5.0 (compatible; RobloxTrendRadar/2.0)"


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# ---------------------------------------------------------------- Netzwerk

def get_json(url, params=None, retries=5):
    if params:
        url = url + "?" + urlencode(params)
    delay, last = 2.0, None
    for _ in range(retries):
        try:
            req = Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
            with urlopen(req, timeout=25) as r:
                return json.loads(r.read().decode("utf-8"))
        except HTTPError as e:
            last = e
            if e.code == 429 or e.code >= 500:
                time.sleep(delay)
                delay *= 2
                continue
            raise
        except (URLError, TimeoutError, ConnectionError) as e:
            last = e
            time.sleep(delay)
            delay *= 2
    raise RuntimeError(f"Anfrage fehlgeschlagen: {url} ({last})")


def chunks(items, n):
    for i in range(0, len(items), n):
        yield items[i:i + n]


def find_games(obj, out, label):
    if isinstance(obj, dict):
        uid = obj.get("universeId")
        if isinstance(uid, int) or (isinstance(uid, str) and uid.isdigit()):
            labels = out.setdefault(int(uid), set())
            if label:
                labels.add(label)
        for v in obj.values():
            if isinstance(v, (dict, list)):
                find_games(v, out, label)
    elif isinstance(obj, list):
        for v in obj:
            find_games(v, out, label)


def discover():
    base = {"sessionId": str(uuid.uuid4()), "device": "computer", "country": "all"}
    found, sorts, token = {}, [], None
    for _ in range(10):
        p = dict(base)
        if token:
            p["sortsPageToken"] = token
        try:
            data = get_json(EXPLORE + "/get-sorts", p)
        except Exception as e:
            log(f"Entdecken-Listen nicht abrufbar: {e}")
            break
        for s in data.get("sorts") or []:
            label = s.get("topic") or s.get("sortDisplayName") or s.get("sortId") or ""
            find_games(s, found, label)
            if s.get("sortId"):
                sorts.append((s["sortId"], label))
        token = data.get("nextSortsPageToken")
        time.sleep(PAUSE)
        if not token:
            break
    done = set()
    for sort_id, label in sorts:
        if sort_id in done:
            continue
        done.add(sort_id)
        token = None
        for _ in range(MAX_SORT_PAGES):
            p = dict(base, sortId=sort_id)
            if token:
                p["pageToken"] = token
            try:
                data = get_json(EXPLORE + "/get-sort-content", p)
            except Exception:
                break
            find_games(data.get("games") or [], found, label)
            token = data.get("nextPageToken")
            time.sleep(PAUSE)
            if not token or not data.get("games"):
                break
    log(f"{len(sorts)} Entdecken-Listen gelesen, {len(found)} Spiele gefunden")
    return found


def fetch_batched(url, uids, extra=None):
    out = []
    for part in chunks(uids, BATCH):
        params = {"universeIds": ",".join(map(str, part))}
        params.update(extra or {})
        try:
            out.extend(get_json(url, params).get("data") or [])
        except Exception as e:
            log(f"Abfrage übersprungen ({url}): {e}")
        time.sleep(PAUSE)
    return out


# ---------------------------------------------------------------- Speicher

def load_json(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def save_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, path)


def day_of(ts):
    return time.strftime("%Y-%m-%d", time.gmtime(ts))


def append_snaps(data_dir, ts, rows):
    path = os.path.join(data_dir, "snaps", day_of(ts) + ".csv.gz")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with gzip.open(path, "at", encoding="utf-8") as f:
        for r in rows:
            f.write(",".join("" if v is None else str(v) for v in r) + "\n")


def load_snaps(data_dir, since):
    folder = os.path.join(data_dir, "snaps")
    if not os.path.isdir(folder):
        return []
    first_day = day_of(since - 86400)
    rows = []
    for name in sorted(os.listdir(folder)):
        if not name.endswith(".csv.gz") or name[:10] < first_day:
            continue
        try:
            with gzip.open(os.path.join(folder, name), "rt", encoding="utf-8") as f:
                for line in f:
                    parts = line.strip().split(",")
                    if len(parts) != 7:
                        continue
                    vals = [int(p) if p else None for p in parts]
                    if vals[1] >= since:
                        rows.append(vals)
        except (OSError, EOFError, ValueError) as e:
            log(f"Datei {name} beschädigt, wird übersprungen: {e}")
    rows.sort(key=lambda r: (r[0], r[1]))
    return rows


def prune(data_dir, now):
    folder = os.path.join(data_dir, "snaps")
    if not os.path.isdir(folder):
        return
    limit = day_of(now - KEEP_DAYS * 86400)
    for name in os.listdir(folder):
        if name.endswith(".csv.gz") and name[:10] < limit:
            os.remove(os.path.join(folder, name))


# ---------------------------------------------------------------- Messen

def collect(data_dir):
    ts = int(time.time())
    status = load_json(os.path.join(data_dir, "status.json"), {})
    status["last_attempt"] = ts
    games = load_json(os.path.join(data_dir, "games.json"), {})
    runs = load_json(os.path.join(data_dir, "runs.json"), [])
    try:
        log("Messung startet ...")
        found = discover()
        recent = load_snaps(data_dir, ts - 3 * 86400)
        best = {}
        for r in recent:
            best[r[0]] = max(best.get(r[0], 0), r[2] or 0)
        tracked = {u for u, p in best.items() if p >= TRACK_MIN_PLAYERS}
        uids = sorted(set(found) | tracked)
        if not uids:
            raise RuntimeError("Keine Spiele gefunden. Eventuell hat Roblox die Schnittstelle geändert "
                               "oder blockt gerade Anfragen von GitHub.")
        details = {int(g["id"]): g for g in fetch_batched(GAMES_API, uids) if "id" in g}
        if not details:
            raise RuntimeError("Roblox hat keine Spieldaten geliefert.")
        votes = {int(v["id"]): (v.get("upVotes"), v.get("downVotes"))
                 for v in fetch_batched(VOTES_API, list(details))}
        need_icon = [u for u in details if not (games.get(str(u)) or {}).get("icon")]
        icons = {int(t["targetId"]): t["imageUrl"] for t in fetch_batched(
            ICONS_API, need_icon, {"returnPolicy": "PlaceHolder", "size": "150x150",
                                   "format": "Png", "isCircular": "false"})
            if t.get("state") == "Completed" and t.get("imageUrl")}

        rows = []
        for uid, g in details.items():
            genre = g.get("genre_l1") or g.get("genre") or ""
            old = games.get(str(uid)) or {}
            games[str(uid)] = {
                "place": g.get("rootPlaceId"), "name": g.get("name") or f"Spiel {uid}",
                "creator": (g.get("creator") or {}).get("name") or "",
                "genre": "" if genre == "All" else genre,
                "created": g.get("created"), "updated": g.get("updated"),
                "icon": icons.get(uid) or old.get("icon"),
                "sorts": sorted(found.get(uid, set())),
                "first_seen": old.get("first_seen", ts), "last_seen": ts,
            }
            up, down = votes.get(uid, (None, None))
            rows.append((uid, ts, g.get("playing") or 0, g.get("visits") or 0,
                         g.get("favoritedCount") or 0, up, down))
        append_snaps(data_dir, ts, rows)
        runs = [r for r in runs if r >= ts - KEEP_DAYS * 86400] + [ts]
        games = {k: v for k, v in games.items() if v.get("last_seen", 0) >= ts - KEEP_DAYS * 86400}
        save_json(os.path.join(data_dir, "games.json"), games)
        save_json(os.path.join(data_dir, "runs.json"), runs)
        prune(data_dir, ts)
        status.update(last_ok=ts, ok=True, message="", games=len(rows))
        log(f"Messung fertig: {len(rows)} Spiele gespeichert")
    except Exception as e:
        status.update(ok=False, message=str(e)[:300])
        log(f"Messung fehlgeschlagen: {e}")
    save_json(os.path.join(data_dir, "status.json"), status)


# ---------------------------------------------------------------- Seite bauen

def build_payload(data_dir):
    games = load_json(os.path.join(data_dir, "games.json"), {})
    runs = load_json(os.path.join(data_dir, "runs.json"), [])
    status = load_json(os.path.join(data_dir, "status.json"), {})
    now = max(runs) if runs else int(time.time())
    first = min(runs) if runs else now
    series = {}
    for r in load_snaps(data_dir, now - SLOTS * 86400):
        series.setdefault(r[0], []).append(r)
    out = []
    for uid, snaps in series.items():
        meta = games.get(str(uid))
        last = snaps[-1]
        if not meta or last[1] < now - 2 * 86400 or max(s[2] or 0 for s in snaps) < 5:
            continue
        sums, cnts = [0] * SLOTS, [0] * SLOTS
        vis, fav = [None] * SLOTS, [None] * SLOTS
        for _, ts, playing, visits, favs, _, _ in snaps:
            d = int((now - ts) // 86400)
            if 0 <= d < SLOTS:
                sums[d] += playing or 0
                cnts[d] += 1
                if visits is not None:
                    vis[d] = visits if vis[d] is None else max(vis[d], visits)
                if favs is not None:
                    fav[d] = favs
        out.append({
            "id": uid, "place": meta.get("place"), "name": meta.get("name"),
            "creator": meta.get("creator"), "genre": meta.get("genre"),
            "created": meta.get("created"), "updated": meta.get("updated"),
            "icon": meta.get("icon"), "sorts": meta.get("sorts") or [],
            "now": last[2], "up": last[5], "down": last[6],
            "avg": [round(sums[i] / cnts[i]) if cnts[i] else None for i in range(SLOTS)],
            "vis": vis, "fav": fav,
        })
    return {"generated": now, "first": first, "runs": len(runs), "status": status, "games": out}


def build_site(data_dir, site_dir, inline=False, out_name="index.html"):
    os.makedirs(site_dir, exist_ok=True)
    payload = build_payload(data_dir)
    data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")
    if inline:
        html = HTML.replace("__INLINE__", f"<script>window.RADAR={data};window.RADAR_INLINE=true;</script>")
    else:
        html = HTML.replace("__INLINE__", "")
        with open(os.path.join(site_dir, "data.js"), "w", encoding="utf-8") as f:
            f.write("window.RADAR=" + data + ";")
        manifest = {
            "name": "Roblox Trend-Radar", "short_name": "Trend-Radar", "start_url": "./",
            "display": "standalone", "background_color": "#0a1012", "theme_color": "#0a1012",
            "icons": [{"src": "icon-192.png", "sizes": "192x192", "type": "image/png"},
                      {"src": "icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "any maskable"}],
        }
        with open(os.path.join(site_dir, "manifest.webmanifest"), "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False)
        for size, b64 in (("192", ICON_192), ("512", ICON_512)):
            with open(os.path.join(site_dir, f"icon-{size}.png"), "wb") as f:
                f.write(base64.b64decode(b64))
        with open(os.path.join(site_dir, ".nojekyll"), "w") as f:
            f.write("")
    with open(os.path.join(site_dir, out_name), "w", encoding="utf-8") as f:
        f.write(html)
    log(f"Seite gebaut: {os.path.join(site_dir, out_name)} ({len(payload['games'])} Spiele)")


# ---------------------------------------------------------------- Demo

DEMO_NAMES = [
    "Beispiel: Garten-Tycoon", "Beispiel: Brainrot Klauen", "Beispiel: 99 Nächte Wald",
    "Beispiel: Fisch Simulator", "Beispiel: Tower Defense X", "Beispiel: Obby Turm",
    "Beispiel: Pet Sammler", "Beispiel: Rennstrecke", "Beispiel: Horror Hotel",
    "Beispiel: Anime Kämpfer", "Beispiel: Bauernhof Leben", "Beispiel: Parkour Stadt",
    "Beispiel: Schatzinsel", "Beispiel: Zombie Bunker", "Beispiel: Rollenspiel Stadt",
    "Beispiel: Schwert Arena", "Beispiel: Mining Tycoon", "Beispiel: Escape Gefängnis",
    "Beispiel: Kochen Chaos", "Beispiel: Monster Fangen", "Beispiel: Fußball Liga",
    "Beispiel: Weltraum Basis", "Beispiel: Ninja Training", "Beispiel: Auto Händler",
    "Beispiel: Verstecken Extrem", "Beispiel: Drachen Zucht", "Beispiel: Boot Bauen",
    "Beispiel: Restaurant Tycoon", "Beispiel: Sniper Duell", "Beispiel: Gummibärchen Obby",
]
DEMO_GENRES = ["Simulation", "Strategie", "Horror", "Action", "Obby & Platformer", "Rollenspiel", "Shooter"]


def make_demo(data_dir):
    rnd = random.Random(7)
    now = int(time.time()) // 3600 * 3600
    hours = 30 * 24
    patterns = ["rise", "explode", "fall", "flat", "spike", "new", "flat", "rise", "fall", "flat"]
    games, by_ts = {}, {}
    for i, name in enumerate(DEMO_NAMES):
        uid, pat = 900000 + i, patterns[i % len(patterns)]
        base = math.exp(rnd.uniform(math.log(150), math.log(60000)))
        created_days = 4 if pat == "new" else rnd.randint(40, 1500)
        sorts = (["Top Trending", "Up-and-Coming"] if pat in ("explode", "new")
                 else ["Up-and-Coming"] if pat == "rise" else [])
        games[str(uid)] = {
            "place": 100000 + i, "name": name, "creator": f"Studio {chr(65 + i % 26)}",
            "genre": rnd.choice(DEMO_GENRES),
            "created": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now - created_days * 86400)),
            "updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now - rnd.randint(0, 20) * 86400)),
            "icon": None, "sorts": sorts, "first_seen": now - hours * 3600, "last_seen": now,
        }
        visits, up = rnd.randint(10**5, 5 * 10**8), rnd.randint(1000, 200000)
        favs, down = visits // 200, int(up * rnd.uniform(0.05, 0.35))
        for h in range(hours + 1):
            ts, day = now - (hours - h) * 3600, h / 24
            trend = {"rise": math.exp(0.07 * day),
                     "explode": 1 + 18 / (1 + math.exp(-(day - 25.5) * 1.3)),
                     "fall": math.exp(-0.045 * day),
                     "spike": 1 + 3 * math.exp(-((day - 27) ** 2) / 1.5),
                     "new": 0 if day < 26 else 0.05 * math.exp(1.1 * (day - 26))}.get(pat, 1)
            daily = 1 + 0.4 * math.sin(2 * math.pi * (time.gmtime(ts).tm_hour - 9) / 24)
            playing = int(base * trend * daily * rnd.lognormvariate(0, 0.06))
            inc = int(playing * 1.6)
            visits, favs, up = visits + inc, favs + int(inc * 0.012), up + int(inc * 0.002)
            if pat == "new" and day < 26:
                continue
            by_ts.setdefault(ts, []).append((uid, ts, playing, visits, favs, up, down))
    for ts in sorted(by_ts):
        append_snaps(data_dir, ts, by_ts[ts])
    save_json(os.path.join(data_dir, "games.json"), games)
    save_json(os.path.join(data_dir, "runs.json"), sorted(by_ts))
    save_json(os.path.join(data_dir, "status.json"), {"last_attempt": now, "last_ok": now, "ok": True})


def main():
    ap = argparse.ArgumentParser(description="Roblox Trend-Radar")
    ap.add_argument("--data", default="data", help="Ordner für Messdaten")
    ap.add_argument("--site", default="site", help="Ordner für die Webseite")
    ap.add_argument("--build", action="store_true", help="nur Seite bauen, nicht messen")
    ap.add_argument("--demo", action="store_true", help="Vorschau mit erfundenen Daten")
    args = ap.parse_args()
    if args.demo:
        import shutil
        shutil.rmtree("vorschau_daten", ignore_errors=True)
        make_demo("vorschau_daten")
        build_site("vorschau_daten", "vorschau")
        return
    if not args.build:
        collect(args.data)
    build_site(args.data, args.site)


HTML = r"""<!doctype html>
<html lang="de">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<meta name="theme-color" content="#0a1012">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="apple-mobile-web-app-title" content="Trend-Radar">
<title>Trend-Radar</title>
<link rel="manifest" href="manifest.webmanifest">
<link rel="icon" href="icon-192.png">
<link rel="apple-touch-icon" href="icon-192.png">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Grenze+Gotisch:wght@600;800&family=Barlow+Condensed:wght@500;600;700&family=Barlow:wght@400;500;600&display=swap" rel="stylesheet">
<style>
:root{
  color-scheme: dark;
  --abyss:#0a1012; --slate:#111b1e; --slate2:#172427; --line:#263638;
  --bone:#efe4e8; --ash:#93a3a3; --blood:#e3124f; --fang:#ff3d8b;
  --fang-soft:rgba(255,61,139,.16); --glow:rgba(255,61,139,.5);
  --frost:#7fd1c7; --frost-soft:rgba(127,209,199,.14);
  --gothic:"Grenze Gotisch","UnifrakturCook",Georgia,"Times New Roman",serif;
  --cond:"Barlow Condensed","Arial Narrow","Roboto Condensed",system-ui,sans-serif;
  --body:"Barlow",system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;
  --cut:16px;
}
*{box-sizing:border-box}
html{background:var(--abyss)}
body{overflow-x:hidden;overflow-x:clip}
body{
  margin:0;color:var(--bone);font:15px/1.5 var(--body);min-height:100vh;
  background:
    radial-gradient(900px 520px at 12% -8%, rgba(227,18,79,.30), transparent 70%),
    radial-gradient(700px 480px at 92% 4%, rgba(255,61,139,.16), transparent 70%),
    radial-gradient(1200px 700px at 50% 115%, rgba(60,140,140,.20), transparent 70%),
    var(--abyss);
  padding:env(safe-area-inset-top,0) env(safe-area-inset-right,0) env(safe-area-inset-bottom,0) env(safe-area-inset-left,0);
}
/* Körnung und Kratzer über allem */
body::before{
  content:"";position:fixed;inset:0;pointer-events:none;z-index:50;opacity:.09;mix-blend-mode:overlay;
  background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='220' height='220'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='.85' numOctaves='3' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)'/%3E%3C/svg%3E");
}
body::after{
  content:"";position:fixed;inset:0;pointer-events:none;z-index:0;opacity:.35;
  background:repeating-linear-gradient(115deg, transparent 0 46px, rgba(255,61,139,.035) 46px 47px, transparent 47px 120px);
}
.wrap{position:relative;z-index:1;max-width:1440px;margin:0 auto;padding:0 20px 56px}

/* ------------ Kopf ------------ */
.hero{position:relative;padding:54px 0 30px;overflow:visible}
.shards{position:absolute;inset:0 -20px 0 auto;width:min(620px,70vw);pointer-events:none}
.shards i{
  position:absolute;display:block;
  background:linear-gradient(160deg,#ffd0e2 0%,var(--fang) 22%,var(--blood) 55%,rgba(120,8,48,.0) 100%);
  filter:drop-shadow(0 0 18px var(--glow));
  animation:shardIn .9s cubic-bezier(.2,.9,.2,1) both;
}
.shards i:nth-child(1){right:6%;top:14px;width:120px;height:300px;clip-path:polygon(50% 0,100% 38%,62% 100%,20% 62%);transform:rotate(24deg);animation-delay:.05s}
.shards i:nth-child(2){right:26%;top:60px;width:70px;height:190px;clip-path:polygon(40% 0,100% 55%,45% 100%,0 35%);transform:rotate(-18deg);opacity:.85;animation-delay:.15s}
.shards i:nth-child(3){right:17%;top:150px;width:46px;height:120px;clip-path:polygon(50% 0,100% 70%,30% 100%);transform:rotate(52deg);opacity:.7;animation-delay:.25s}
.shards i:nth-child(4){right:40%;top:20px;width:30px;height:90px;clip-path:polygon(50% 0,100% 100%,0 80%);transform:rotate(12deg);opacity:.55;animation-delay:.32s}
.shards i:nth-child(5){right:0;top:210px;width:60px;height:150px;clip-path:polygon(30% 0,100% 30%,70% 100%,0 70%);transform:rotate(-36deg);opacity:.6;animation-delay:.4s}
@keyframes shardIn{from{opacity:0;translate:60px -40px;scale:.6}to{}}
h1{
  position:relative;z-index:1;margin:0;font-family:var(--gothic);font-weight:800;
  font-size:clamp(56px,11vw,138px);line-height:.86;letter-spacing:-.01em;color:var(--bone);
  text-shadow:0 0 28px rgba(227,18,79,.55),0 0 2px #fff,0 4px 0 #5a0b25;
}
.slash{display:block;width:min(560px,80%);height:10px;margin:18px 0 18px;
  background:linear-gradient(90deg,var(--blood),var(--fang) 40%,transparent);
  clip-path:polygon(0 40%,100% 0,96% 60%,0 100%);filter:drop-shadow(0 0 10px var(--glow))}
.sub{position:relative;z-index:1;font-family:var(--cond);font-weight:600;font-size:clamp(19px,2.4vw,26px);letter-spacing:.01em;margin:0;color:var(--bone);max-width:40ch}
.meta{position:relative;z-index:1;display:flex;flex-wrap:wrap;gap:8px 16px;align-items:center;color:var(--ash);font-size:14px;margin:14px 0 0}
.btn{
  font:600 15px var(--cond);letter-spacing:.02em;color:var(--bone);cursor:pointer;
  background:linear-gradient(135deg,var(--blood),#8e0b37);border:0;padding:8px 18px;
  clip-path:polygon(10px 0,100% 0,calc(100% - 10px) 100%,0 100%);
  box-shadow:0 0 18px var(--glow);
}
.btn:hover{background:linear-gradient(135deg,var(--fang),var(--blood))}
.btn:disabled{opacity:.5;cursor:wait}

/* ------------ Rahmen mit abgeschnittenen Ecken ------------ */
.frame{
  position:relative;padding:1px;
  background:linear-gradient(135deg,var(--fang),rgba(227,18,79,.25) 30%,var(--line) 55%,rgba(127,209,199,.35));
  clip-path:polygon(0 0,calc(100% - var(--cut)) 0,100% var(--cut),100% 100%,var(--cut) 100%,0 calc(100% - var(--cut)));
}
.frame > .in{
  background:linear-gradient(180deg,rgba(23,36,39,.97),rgba(17,27,30,.97));
  clip-path:polygon(0 0,calc(100% - var(--cut)) 0,100% var(--cut),100% 100%,var(--cut) 100%,0 calc(100% - var(--cut)));
}

/* ------------ Steuerung ------------ */
.controls .in{padding:22px 26px 18px}
.sentence{font-family:var(--cond);font-weight:500;font-size:clamp(20px,2.4vw,28px);line-height:2.05;margin:0}
.sentence select,.sentence input{
  font:inherit;font-weight:700;color:var(--fang);background:transparent;border:0;border-radius:0;
  padding:0 2px;margin:0 3px;appearance:none;-webkit-appearance:none;cursor:pointer;
  background-image:linear-gradient(90deg,var(--blood),var(--fang));background-size:100% 3px;background-repeat:no-repeat;background-position:0 92%;
  text-shadow:0 0 14px rgba(255,61,139,.45);
}
.sentence input{width:5.4ch;cursor:text;-moz-appearance:textfield}
.sentence input::-webkit-inner-spin-button,.sentence input::-webkit-outer-spin-button{-webkit-appearance:none;margin:0}
select option{color:var(--bone);background:var(--slate);font-family:var(--body);font-weight:500}
.dir{font:600 14px var(--cond);letter-spacing:.02em;color:var(--ash);background:transparent;border:1px solid var(--line);padding:1px 10px;margin:0 6px 0 2px;cursor:pointer;vertical-align:middle;
  clip-path:polygon(6px 0,100% 0,calc(100% - 6px) 100%,0 100%)}
.dir:hover{color:var(--bone);border-color:var(--fang)}
.filters{display:flex;flex-wrap:wrap;gap:12px 22px;align-items:center;margin-top:14px;padding-top:16px;border-top:1px solid var(--line);color:var(--ash);font-size:14.5px}
.filters input[type=search],.filters select{font:inherit;color:var(--bone);background:rgba(10,16,18,.7);border:1px solid var(--line);padding:7px 11px}
.filters input[type=search]{width:250px;max-width:100%}
.filters input[type=search]:focus,.filters select:focus{border-color:var(--fang)}
.filters label{display:inline-flex;gap:7px;align-items:center;cursor:pointer}
.filters input[type=checkbox]{accent-color:var(--blood);width:17px;height:17px}
:focus-visible{outline:2px solid var(--fang);outline-offset:2px}

/* ------------ Podium ------------ */
.podium{display:grid;grid-template-columns:1.25fr 1fr 1fr;gap:16px;margin:26px 0 8px}
.podium:empty{display:none}
.pcard .in{position:relative;padding:20px 22px 18px;height:100%;overflow:hidden}
.pcard .in::after{content:"";position:absolute;right:-30px;top:-40px;width:140px;height:220px;opacity:.16;
  background:linear-gradient(160deg,var(--fang),transparent 70%);clip-path:polygon(50% 0,100% 40%,60% 100%,15% 60%);transform:rotate(28deg)}
.pplace{font-family:var(--gothic);font-weight:800;font-size:46px;line-height:1;color:var(--blood);text-shadow:0 0 18px var(--glow);position:absolute;right:22px;bottom:12px;opacity:.9}
.phead{display:flex;gap:12px;align-items:center;margin-bottom:12px;padding-right:20px}
.phead .icon{width:56px;height:56px}
.pname{font-family:var(--cond);font-weight:700;font-size:21px;line-height:1.1}
.psub{color:var(--ash);font-size:13px;margin-top:2px}
.pbig{font-family:var(--cond);font-weight:700;font-size:clamp(40px,5vw,60px);line-height:1;color:var(--fang);text-shadow:0 0 22px var(--glow);font-variant-numeric:tabular-nums}
.pbig.down{color:var(--frost);text-shadow:none}
.plabel{color:var(--ash);font-size:13.5px;margin-top:4px}
.pstats{display:flex;gap:22px;margin-top:12px;font-size:14px}
.pstats b{display:block;font-family:var(--cond);font-size:20px;font-weight:700;color:var(--bone);font-variant-numeric:tabular-nums}
.pstats span{color:var(--ash);font-size:12.5px}
.pspark{margin-top:12px}
.pspark .spark{width:100%;max-width:340px}

/* ------------ Hinweise ------------ */
.note{margin:18px 0 0;padding:12px 16px;border-left:3px solid var(--fang);background:var(--fang-soft);font-size:14.5px;max-width:110ch}
.note.err{border-color:#ffb020;background:rgba(255,176,32,.12)}
.note:empty{display:none}
.count{font-family:var(--cond);font-weight:600;font-size:16px;color:var(--ash);margin:22px 0 10px}

/* ------------ Tabelle ------------ */
.tablebox{overflow:auto;max-height:calc(100vh - 90px)}
table{border-collapse:separate;border-spacing:0;width:100%;min-width:1220px;font-variant-numeric:tabular-nums}
th{
  position:sticky;top:0;z-index:2;background:#13201f;text-align:right;
  font:600 14px var(--cond);letter-spacing:.02em;color:var(--ash);padding:14px 12px 11px;
  border-bottom:2px solid var(--blood);white-space:nowrap;user-select:none;
}
th.sortable{cursor:pointer}
th.sortable:hover,th.on{color:var(--bone)}
th .badge{display:inline-block;margin-left:6px;padding:0 6px;font-size:12px;line-height:18px;color:#fff;background:var(--blood);clip-path:polygon(4px 0,100% 0,calc(100% - 4px) 100%,0 100%)}
th .badge.two{background:#4a5c5e}
td{padding:10px 12px;border-bottom:1px solid var(--line);text-align:right;white-space:nowrap;vertical-align:middle}
tbody tr{transition:background-color .12s}
tbody tr:hover{background:rgba(255,61,139,.06)}
tbody tr:hover td:first-child{box-shadow:inset 3px 0 0 var(--fang)}
.l{text-align:left}
.rank{color:var(--ash);font:600 15px var(--cond)}
.game{display:flex;gap:12px;align-items:center;min-width:280px;max-width:390px;text-align:left}
.icon{width:44px;height:44px;flex:none;object-fit:cover;background:var(--slate2);display:grid;place-items:center;
  font:800 22px var(--gothic);color:var(--fang);
  clip-path:polygon(0 0,calc(100% - 9px) 0,100% 9px,100% 100%,9px 100%,0 calc(100% - 9px))}
.gname{font-weight:600;white-space:normal;line-height:1.25;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.gsub{font-size:12.5px;color:var(--ash);white-space:normal;line-height:1.35;margin-top:2px}
.chip{display:inline-block;font:600 12px var(--cond);letter-spacing:.02em;padding:0 8px;margin:4px 4px 0 0;color:var(--fang);background:var(--fang-soft);
  clip-path:polygon(5px 0,100% 0,calc(100% - 5px) 100%,0 100%)}
.chip.new{color:#fff;background:var(--blood);box-shadow:0 0 12px var(--glow)}
.heat{position:relative;display:inline-block;white-space:nowrap;min-width:100px;padding:3px 10px;font:700 16px var(--cond);
  clip-path:polygon(6px 0,100% 0,calc(100% - 6px) 100%,0 100%)}
.heat i{position:absolute;inset:0;z-index:0}
.heat span{position:relative;z-index:1}
.heat.up{color:#fff}.heat.up i{background:linear-gradient(90deg,#8e0b37,var(--blood) 60%,var(--fang))}
.heat.down{color:var(--frost)}.heat.down i{background:var(--frost-soft)}
.heat.flat{color:var(--ash)}
.up{color:var(--fang)} .down{color:var(--frost)} .flat{color:var(--ash)}
.approx{font-size:11.5px;color:var(--ash);margin-left:5px}
.dim{color:var(--ash)}
.spark{display:block;overflow:visible}
.links a{color:var(--bone);text-decoration:none;border-bottom:1px solid var(--blood);margin-right:12px;font-size:13.5px;padding-bottom:1px}
.links a:hover{color:var(--fang);border-color:var(--fang)}
.more{display:block;margin:20px auto 0}
.empty{padding:46px 20px;text-align:center;color:var(--ash);white-space:normal}
details{margin:30px 0 0;color:var(--ash);font-size:14.5px;max-width:90ch}
details summary{cursor:pointer;color:var(--bone);font:600 18px var(--cond)}
details p{margin:9px 0}
details b{color:var(--bone)}

/* ------------ Handy ------------ */
@media (max-width:900px){
  .podium{grid-template-columns:1fr}
}
@media (max-width:760px){
  .wrap{padding:0 12px 44px}
  .hero{padding:34px 0 22px}
  .shards{width:90vw;right:-30px;opacity:.38}
  .pcard .in{padding:16px 16px 14px}
  .pbig{font-size:44px}
  .controls .in{padding:16px 16px 14px}
  .sentence{line-height:2.15}
  .filters{gap:10px 16px}
  .filters input[type=search]{width:100%}
  .frame.tableframe{background:none;clip-path:none;padding:0}
  .frame.tableframe > .in{background:none;clip-path:none}
  .tablebox{max-height:none;overflow:visible}
  table{min-width:0}
  thead{display:none}
  tbody{display:grid;gap:12px}
  tbody tr{display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px 12px;padding:14px 14px 12px;position:relative;
    background:linear-gradient(180deg,rgba(23,36,39,.97),rgba(17,27,30,.97));border-top:2px solid var(--blood);
    clip-path:polygon(0 0,calc(100% - 14px) 0,100% 14px,100% 100%,14px 100%,0 calc(100% - 14px))}
  tbody tr:hover td:first-child{box-shadow:none}
  td{border:0;padding:0;text-align:left;white-space:normal}
  td[data-label]::before{content:attr(data-label);display:block;font:600 12px var(--cond);letter-spacing:.02em;color:var(--ash);margin-bottom:1px}
  td.rank{position:absolute;right:16px;top:12px;font:800 24px var(--gothic);color:var(--blood)}
  td.gcell{grid-column:1 / -1;padding-right:36px}
  td.sparkcell{grid-column:1 / -1}
  td.sparkcell .spark{width:100%;height:34px}
  td.links{grid-column:1 / -1;padding-top:4px}
  .game{min-width:0;max-width:none}
  .heat{min-width:0}
  .empty{grid-column:1 / -1}
}
@media (prefers-reduced-motion: reduce){
  .shards i{animation:none}
  tbody tr{transition:none}
}
</style>
__INLINE__
</head>
<body>
<div class="wrap">
  <header class="hero">
    <div class="shards" aria-hidden="true"><i></i><i></i><i></i><i></i><i></i></div>
    <h1>Trend-Radar</h1>
    <span class="slash" aria-hidden="true"></span>
    <p class="sub">Welche Roblox-Spiele gerade ausbrechen, bevor es alle anderen merken.</p>
    <div class="meta"><span id="meta">Daten werden geladen …</span><button class="btn" id="reload" type="button" hidden>Neu laden</button></div>
  </header>

  <section class="frame controls"><div class="in">
    <p class="sentence">
      Spiele mit mehr als
      <input id="minPlayers" type="number" min="0" step="100" inputmode="numeric" aria-label="Mindestzahl Spieler">
      Spielern, Wachstum über
      <select id="days" aria-label="Zeitraum in Tagen"></select>
      sortiert nach
      <select id="sort1" aria-label="Erstes Sortierkriterium"></select><button class="dir" id="dir1" type="button"></button>
      dann nach
      <select id="sort2" aria-label="Zweites Sortierkriterium"></select><button class="dir" id="dir2" type="button"></button>
    </p>
    <div class="filters">
      <input id="q" type="search" placeholder="Spiel oder Studio suchen" aria-label="Suche">
      <label>Genre <select id="genre"></select></label>
      <label>Spielalter <select id="maxAge">
        <option value="">egal</option><option value="14">bis 14 Tage</option><option value="30">bis 30 Tage</option>
        <option value="90">bis 3 Monate</option><option value="365">bis 1 Jahr</option></select></label>
      <label><input id="onlyUp" type="checkbox"> nur wachsende</label>
      <label><input id="onlyTrend" type="checkbox"> nur in Roblox-Trendlisten</label>
    </div>
  </div></section>

  <p class="note" id="note"></p>
  <section class="podium" id="podium" aria-label="Die drei stärksten Spiele"></section>

  <p class="count" id="count"></p>
  <div class="frame tableframe"><div class="in"><div class="tablebox">
    <table><thead><tr id="head"></tr></thead><tbody id="body"></tbody></table>
  </div></div></div>
  <button class="btn more" id="more" type="button" hidden>Weitere 200 anzeigen</button>

  <details>
    <summary>So wird gerechnet</summary>
    <p><b>Wachstum</b> vergleicht die durchschnittliche Spielerzahl der letzten 24 Stunden mit dem gleichen 24-Stunden-Fenster vor X Tagen. Durch den Tagesdurchschnitt verfälschen Uhrzeit-Schwankungen (nachmittags voll, nachts leer) das Ergebnis nicht.</p>
    <p><b>≈ 3 T</b> neben einem Wert heißt: Für den gewählten Zeitraum gibt es noch nicht genug Messdaten, verglichen wurde mit dem ältesten verfügbaren Tag.</p>
    <p><b>Trend-Score</b> verbindet Wachstum und Größe: (Plus Spieler ÷ (Spieler vorher + 50)) × log10(Spieler jetzt + 10). Ein Spiel, das von 2.000 auf 6.000 wächst, liegt damit vor einem, das von 20 auf 200 wächst.</p>
    <p><b>Besuche/Tag</b> ist der Zuwachs der Gesamtbesuche in den letzten 24 Stunden. <b>Besuche Wachstum</b> vergleicht das mit dem Tageswert vor X Tagen. Neue Spieler zeigen sich hier oft früher als bei den gleichzeitigen Spielern.</p>
    <p><b>Spielalter</b> zählt ab der Veröffentlichung, <b>Update</b> ab der letzten Aktualisierung durch die Entwickler. Große Updates lösen oft Sprünge aus.</p>
    <p>Der YouTube-Link sucht nach Videos aus dieser Woche. So siehst du schnell, wie viel Konkurrenz es zu einem Spiel schon gibt.</p>
  </details>
</div>

<script>
const DAY = 86400;
const nf = new Intl.NumberFormat('de-DE');
const nf1 = new Intl.NumberFormat('de-DE',{maximumFractionDigits:1});
const nfc = new Intl.NumberFormat('de-DE',{notation:'compact',maximumFractionDigits:1});
let DATA = null, historyDays = 0;

const SORTS = [
  {key:'score',  label:'Trend-Score',      dir:-1},
  {key:'pct',    label:'Wachstum in %',    dir:-1},
  {key:'abs',    label:'Plus Spieler',     dir:-1},
  {key:'now',    label:'Spieler jetzt',    dir:-1},
  {key:'avg',    label:'Ø Spieler 24 Std.',dir:-1},
  {key:'visDay', label:'Besuche pro Tag',  dir:-1},
  {key:'visPct', label:'Besuchs-Wachstum', dir:-1},
  {key:'fav',    label:'neue Favoriten',   dir:-1},
  {key:'likes',  label:'Like-Rate',        dir:-1},
  {key:'age',    label:'Spielalter',       dir:1},
  {key:'upd',    label:'letztes Update',   dir:1},
  {key:'name',   label:'Name',             dir:1},
];
const COLS = [
  {key:'rank',  label:'#', cls:'l'},
  {key:'name',  label:'Spiel', cls:'l', sort:true},
  {key:'now',   label:'Spieler jetzt', sort:true, tip:'Gleichzeitige Spieler bei der letzten Messung'},
  {key:'avg',   label:'Ø 24 Std.', sort:true, tip:'Durchschnitt der gleichzeitigen Spieler in den letzten 24 Stunden'},
  {key:'spark', label:'Verlauf', tip:'Tagesdurchschnitt der Spieler'},
  {key:'pct',   label:'Wachstum', sort:true, tip:'Ø Spieler letzte 24 Std. gegenüber vor X Tagen'},
  {key:'abs',   label:'Plus Spieler', sort:true},
  {key:'score', label:'Trend-Score', sort:true, tip:'Wachstum gewichtet nach Größe'},
  {key:'visDay',label:'Besuche/Tag', sort:true},
  {key:'visPct',label:'Besuche Wachstum', sort:true},
  {key:'fav',   label:'Neue Favoriten', sort:true, tip:'Neue Favoriten im gewählten Zeitraum'},
  {key:'likes', label:'Likes', sort:true, tip:'Anteil Daumen hoch'},
  {key:'age',   label:'Alter', sort:true},
  {key:'upd',   label:'Update', sort:true, tip:'Letzte Aktualisierung durch die Entwickler'},
  {key:'links', label:'Öffnen', cls:'l'},
];
const DAY_OPTIONS = [1,2,3,5,7,10,14,21,30];
const TREND_LISTS = /trend|up-and-coming|rising|popular now|hot/i;

let state = {minPlayers:500, days:7, sort:[{key:'score',dir:-1},{key:'now',dir:-1}], q:'', genre:'', maxAge:'', onlyUp:false, onlyTrend:false};
try { const s = JSON.parse(localStorage.getItem('robloxRadarState')||'null'); if (s) state = Object.assign(state, s); } catch(e) {}
if (!Array.isArray(state.sort) || !state.sort.length) state.sort = [{key:'score',dir:-1}];
let limit = 200;

const $ = id => document.getElementById(id);
const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const parseDate = s => { const t = Date.parse(s||''); return isNaN(t) ? null : t/1000; };
const save = () => { try { localStorage.setItem('robloxRadarState', JSON.stringify(state)); } catch(e) {} };

function metrics(g, X){
  const cur = g.avg[0];
  let p = null;
  for (let k = X; k >= 1; k--) { if (g.avg[k] != null) { p = k; break; } }
  const m = {cur, p, partial: p != null && p < X};
  if (cur != null && p != null) {
    const prev = g.avg[p];
    m.abs = cur - prev;
    m.pct = prev > 0 ? (cur - prev) / prev : null;
    m.score = ((cur - prev) / (prev + 50)) * Math.log10(cur + 10);
  }
  if (g.vis[0] != null && g.vis[1] != null) m.visDay = g.vis[0] - g.vis[1];
  if (m.visDay != null && p != null && g.vis[p] != null && g.vis[p+1] != null) {
    const before = g.vis[p] - g.vis[p+1];
    m.visPct = before > 0 ? (m.visDay - before) / before : null;
  }
  if (p != null && g.fav[0] != null && g.fav[p] != null) m.fav = g.fav[0] - g.fav[p];
  if (g.up != null && g.down != null && g.up + g.down > 0) m.likes = g.up / (g.up + g.down);
  const c = parseDate(g.created), u = parseDate(g.updated);
  if (c) m.age = (DATA.generated - c) / DAY;
  if (u) m.upd = Math.max(0, (DATA.generated - u) / DAY);
  return m;
}

function valueFor(row, key){
  if (key === 'name') return row.g.name.toLowerCase();
  if (key === 'now') return row.g.now;
  if (key === 'avg') return row.m.cur;
  return row.m[key];
}
function compare(a, b){
  for (const s of state.sort) {
    const va = valueFor(a, s.key), vb = valueFor(b, s.key);
    const na = va == null || Number.isNaN(va), nb = vb == null || Number.isNaN(vb);
    if (na && nb) continue;
    if (na) return 1;
    if (nb) return -1;
    if (va < vb) return -s.dir;
    if (va > vb) return s.dir;
  }
  return 0;
}

const fmtPct = v => v == null ? '–' : (v > 0 ? '+' : '') + nf.format(Math.round(v*100)) + ' %';
const fmtSigned = v => { if (v == null) return '–'; const r = Math.round(v); return (r > 0 ? '+' : '') + nf.format(r); };
const fmtBig = v => v == null ? '–' : Math.abs(v) >= 1000000 ? nfc.format(v) : nf.format(Math.round(v));
const fmtAge = d => d == null ? '–' : d < 1 ? 'heute' : d < 60 ? Math.floor(d) + ' T' : d < 730 ? Math.floor(d/30.4) + ' Mon.' : nf1.format(d/365) + ' J.';
const fmtUpd = d => d == null ? '–' : d < 1 ? 'heute' : d < 2 ? 'gestern' : 'vor ' + Math.floor(d) + ' T';
const cls = v => v == null ? 'flat' : v > 0.02 ? 'up' : v < -0.02 ? 'down' : 'flat';

let sparkId = 0;
function spark(g, X, trend, w = 100, h = 28){
  const span = Math.min(Math.max(X, 7), g.avg.length - 1);
  const pts = [];
  for (let k = span; k >= 0; k--) if (g.avg[k] != null) pts.push([span - k, g.avg[k]]);
  if (pts.length < 2) return '<span class="dim">–</span>';
  const max = Math.max(...pts.map(p=>p[1])), min = Math.min(...pts.map(p=>p[1])), rng = max - min || 1;
  const xy = pts.map(([x,y]) => [(x/span*(w-4)+2), (h-3-(y-min)/rng*(h-6))]);
  const line = xy.map(p => p[0].toFixed(1)+','+p[1].toFixed(1)).join(' ');
  const area = `M${xy[0][0].toFixed(1)},${h} L` + line.replace(/ /g,' L') + ` L${xy[xy.length-1][0].toFixed(1)},${h} Z`;
  const color = trend === 'up' ? 'var(--fang)' : trend === 'down' ? 'var(--frost)' : 'var(--ash)';
  const id = 'sg' + (++sparkId);
  return `<svg class="spark" width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none" aria-hidden="true">
    <defs><linearGradient id="${id}" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="${color}" stop-opacity=".35"/><stop offset="1" stop-color="${color}" stop-opacity="0"/></linearGradient></defs>
    <path d="${area}" fill="url(#${id})"/>
    <polyline points="${line}" fill="none" stroke="${color}" stroke-width="2" stroke-linejoin="round" stroke-linecap="round" vector-effect="non-scaling-stroke" ${trend==='up'?'style="filter:drop-shadow(0 0 4px var(--glow))"':''}/></svg>`;
}

function cleanName(n){
  const c = n.replace(/\[[^\]]*\]|\([^)]*\)|\{[^}]*\}/g, ' ').replace(/[^\p{L}\p{N}\s'!?&-]/gu, ' ').replace(/\s+/g, ' ').trim();
  return c || n;
}
function iconHtml(g){
  const letter = esc((cleanName(g.name).replace(/^Beispiel:?\s*/,'').match(/[\p{L}\p{N}]/u)||['?'])[0].toUpperCase());
  return g.icon ? `<img class="icon" src="${esc(g.icon)}" alt="" loading="lazy" referrerpolicy="no-referrer">` : `<span class="icon">${letter}</span>`;
}
function links(g){
  const robloxUrl = g.place ? `https://www.roblox.com/games/${g.place}` : `https://www.roblox.com/discover`;
  const ytUrl = `https://www.youtube.com/results?search_query=${encodeURIComponent(cleanName(g.name) + ' roblox')}&sp=EgIIAw%253D%253D`;
  return `<a href="${robloxUrl}" target="_blank" rel="noopener">Roblox</a><a href="${ytUrl}" target="_blank" rel="noopener" title="YouTube-Videos dieser Woche">YouTube</a>`;
}

function filtered(){
  const X = state.days, q = state.q.trim().toLowerCase(), rows = [];
  for (const g of DATA.games) {
    if ((g.now ?? 0) < state.minPlayers && (g.avg[0] ?? 0) < state.minPlayers) continue;
    if (q && !(g.name.toLowerCase().includes(q) || (g.creator||'').toLowerCase().includes(q))) continue;
    if (state.genre && g.genre !== state.genre) continue;
    if (state.onlyTrend && !g.sorts.some(s => TREND_LISTS.test(s))) continue;
    const m = metrics(g, X);
    if (state.maxAge && !(m.age != null && m.age <= +state.maxAge)) continue;
    if (state.onlyUp && !(m.abs > 0)) continue;
    rows.push({g, m});
  }
  return rows.sort(compare);
}

function renderPodium(rows){
  const box = $('podium');
  if (!rows.length || historyDays < 1) { box.innerHTML = ''; return; }
  const sortName = (SORTS.find(s => s.key === state.sort[0].key) || {}).label || '';
  box.innerHTML = rows.slice(0, 3).map(({g, m}, i) => {
    const t = cls(m.pct);
    const sub = [g.creator, g.genre].filter(Boolean).map(esc).join(', ');
    return `<article class="frame pcard"><div class="in">
      <div class="phead">${iconHtml(g)}<div><div class="pname">${esc(g.name)}</div><div class="psub">${sub}</div></div></div>
      <div class="pbig ${t === 'down' ? 'down' : ''}">${fmtPct(m.pct)}</div>
      <div class="plabel">Wachstum in ${m.partial ? m.p : state.days} ${(m.partial ? m.p : state.days) === 1 ? 'Tag' : 'Tagen'}, Platz ${i+1} nach ${esc(sortName)}</div>
      <div class="pspark">${spark(g, state.days, t, 260, 46)}</div>
      <div class="pstats"><div><b>${fmtBig(g.now)}</b><span>Spieler jetzt</span></div><div><b>${fmtSigned(m.abs)}</b><span>Plus Spieler</span></div><div><b>${fmtBig(m.visDay)}</b><span>Besuche/Tag</span></div></div>
      <div class="links" style="margin-top:12px">${links(g)}</div>
      <div class="pplace" aria-hidden="true">${i+1}</div>
    </div></article>`;
  }).join('');
}

function render(){
  sparkId = 0;
  const X = state.days, rows = filtered();
  renderPodium(rows);
  $('count').textContent = `${nf.format(rows.length)} von ${nf.format(DATA.games.length)} Spielen passen zum Filter`;
  const body = $('body');
  if (!rows.length) {
    body.innerHTML = `<tr><td colspan="${COLS.length}" class="empty">${DATA.games.length ? 'Kein Spiel passt zu diesen Filtern. Senk die Mindestzahl an Spielern oder entferne einen Filter.' : 'Noch keine Messdaten. Sobald die erste Messung durchgelaufen ist, füllt sich diese Tabelle.'}</td></tr>`;
    $('more').hidden = true;
    return;
  }
  body.innerHTML = rows.slice(0, limit).map(({g, m}, i) => {
    const t = cls(m.pct);
    const approx = m.partial ? `<span class="approx" title="Nur ${m.p} Tage Messdaten verfügbar">≈ ${m.p} T</span>` : '';
    const heat = m.pct == null ? 0 : 0.25 + 0.75 * Math.min(Math.abs(m.pct), 1);
    const chips = [];
    if (m.age != null && m.age <= 30) chips.push('<span class="chip new">neu</span>');
    g.sorts.filter(s => TREND_LISTS.test(s)).slice(0,2).forEach(s => chips.push(`<span class="chip">${esc(s)}</span>`));
    const sub = [g.creator, g.genre].filter(Boolean).map(esc).join(', ');
    return `<tr>
      <td class="l rank">${i+1}</td>
      <td class="l gcell"><div class="game">${iconHtml(g)}<div><div class="gname" title="${esc(g.name)}">${esc(g.name)}</div><div class="gsub">${sub}${sub && chips.length ? '<br>' : ''}${chips.join('')}</div></div></div></td>
      <td data-label="Spieler jetzt">${fmtBig(g.now)}</td>
      <td data-label="Ø 24 Std.">${fmtBig(m.cur)}</td>
      <td class="sparkcell">${spark(g, X, t)}</td>
      <td data-label="Wachstum"><span class="heat ${t}"><i style="opacity:${heat.toFixed(2)}"></i><span>${fmtPct(m.pct)}</span></span>${approx}</td>
      <td data-label="Plus Spieler" class="${cls(m.abs)}">${fmtSigned(m.abs)}</td>
      <td data-label="Trend-Score" class="${cls(m.score)}">${m.score == null ? '–' : nf1.format(m.score)}</td>
      <td data-label="Besuche/Tag">${fmtBig(m.visDay)}</td>
      <td data-label="Besuche Wachstum" class="${cls(m.visPct)}">${fmtPct(m.visPct)}</td>
      <td data-label="Neue Favoriten">${m.fav == null ? '–' : fmtSigned(m.fav)}</td>
      <td data-label="Likes">${m.likes == null ? '–' : Math.round(m.likes*100) + ' %'}</td>
      <td data-label="Alter">${fmtAge(m.age)}</td>
      <td data-label="Update">${fmtUpd(m.upd)}</td>
      <td class="l links">${links(g)}</td>
    </tr>`;
  }).join('');
  $('more').hidden = rows.length <= limit;
}

function renderHead(){
  $('head').innerHTML = COLS.map(c => {
    const idx = state.sort.findIndex(s => s.key === c.key);
    const badge = idx >= 0 ? `<span class="badge ${idx ? 'two' : ''}">${idx+1}${state.sort[idx].dir < 0 ? '↓' : '↑'}</span>` : '';
    const attrs = c.sort ? `class="sortable ${c.cls||''} ${idx>=0?'on':''}" data-key="${c.key}" tabindex="0"` : `class="${c.cls||''}"`;
    return `<th ${attrs} ${c.tip ? `title="${esc(c.tip)}"` : ''}>${esc(c.label)}${badge}</th>`;
  }).join('');
}

function fitSelect(el){
  const cs = getComputedStyle(el), s = document.createElement('span');
  s.style.cssText = `position:absolute;visibility:hidden;white-space:pre;font-weight:${cs.fontWeight};font-size:${cs.fontSize};font-family:${cs.fontFamily}`;
  s.textContent = el.options[el.selectedIndex] ? el.options[el.selectedIndex].text : '';
  document.body.appendChild(s);
  el.style.width = Math.ceil(s.getBoundingClientRect().width + 10) + 'px';
  s.remove();
}
function syncControls(){
  $('minPlayers').value = state.minPlayers;
  $('days').value = state.days;
  $('sort1').value = state.sort[0].key;
  $('sort2').value = state.sort[1] ? state.sort[1].key : '';
  $('dir1').textContent = state.sort[0].dir < 0 ? 'absteigend' : 'aufsteigend';
  $('dir2').textContent = state.sort[1] && state.sort[1].dir < 0 ? 'absteigend' : 'aufsteigend';
  $('dir2').hidden = !state.sort[1];
  $('q').value = state.q; $('genre').value = state.genre; $('maxAge').value = state.maxAge;
  $('onlyUp').checked = state.onlyUp; $('onlyTrend').checked = state.onlyTrend;
  ['days','sort1','sort2'].forEach(id => fitSelect($(id)));
}

function updateNote(){
  const n = $('note'), st = DATA.status || {}, nowSec = Date.now()/1000;
  n.className = 'note';
  let t = '';
  if (st.ok === false) {
    n.className = 'note err';
    t = 'Die letzte Messung ist fehlgeschlagen' + (st.message ? `: ${st.message}` : '.') + ' Angezeigt werden die Daten der letzten erfolgreichen Messung.';
  } else if (DATA.runs && nowSec - DATA.generated > 3 * 3600) {
    n.className = 'note err';
    t = `Die letzte Messung ist ${Math.round((nowSec - DATA.generated)/3600)} Stunden her. Prüf auf GitHub unter „Actions“, ob der Zeitplan noch läuft.`;
  } else if (!DATA.runs) t = 'Noch keine Messung vorhanden. Starte den Ablauf auf GitHub unter „Actions“, dann erscheinen hier die Spiele.';
  else if (historyDays < 1) t = `Bisher erst ${nf1.format(historyDays*24)} Stunden Messdaten. Wachstumswerte erscheinen, sobald mindestens 24 Stunden gesammelt sind.`;
  else if (historyDays < state.days) t = `Bisher ${nf1.format(historyDays)} Tage Messdaten. Für ${state.days} Tage reicht das noch nicht, Werte mit ≈ beziehen sich auf den kürzeren Zeitraum.`;
  n.textContent = t;
}

function updateMeta(){
  const fmt = x => new Date(x*1000).toLocaleString('de-DE',{day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'});
  const mins = Math.max(0, Math.round((Date.now()/1000 - DATA.generated)/60));
  const ago = mins < 60 ? `vor ${mins} Min.` : `vor ${Math.round(mins/60)} Std.`;
  $('meta').textContent = DATA.runs
    ? `Letzte Messung ${fmt(DATA.generated)} Uhr (${ago}). Gemessen seit ${fmt(DATA.first)} Uhr, ${nf.format(DATA.games.length)} Spiele.`
    : 'Noch keine Messungen.';
}

function update(){ save(); limit = 200; syncControls(); renderHead(); updateNote(); render(); }

function setSortFromHeader(key, secondary){
  const def = (SORTS.find(s=>s.key===key)||{dir:-1}).dir;
  if (secondary) {
    if (state.sort[0].key === key) return;
    state.sort[1] = state.sort[1] && state.sort[1].key === key ? {key, dir:-state.sort[1].dir} : {key, dir:def};
  } else if (state.sort[0].key === key) state.sort[0].dir *= -1;
  else state.sort = [{key, dir:def}, state.sort[0]];
  update();
}

function setData(d){
  DATA = d;
  historyDays = (DATA.generated - DATA.first) / DAY;
  const genres = [...new Set(DATA.games.map(g => g.genre).filter(Boolean))].sort((a,b)=>a.localeCompare(b,'de'));
  $('genre').innerHTML = `<option value="">alle</option>` + genres.map(g => `<option>${esc(g)}</option>`).join('');
  if (state.genre && !genres.includes(state.genre)) state.genre = '';
  updateMeta();
  update();
}

function loadData(){
  return new Promise((resolve, reject) => {
    const s = document.createElement('script');
    s.src = 'data.js?v=' + Date.now();
    s.onload = () => { s.remove(); window.RADAR ? resolve(window.RADAR) : reject(); };
    s.onerror = () => { s.remove(); reject(); };
    document.head.appendChild(s);
  });
}

function init(){
  $('days').innerHTML = DAY_OPTIONS.map(n => `<option value="${n}">${n} ${n === 1 ? 'Tag' : 'Tage'}</option>`).join('');
  if (!DAY_OPTIONS.includes(+state.days)) state.days = 7;
  const opts = SORTS.map(s => `<option value="${s.key}">${esc(s.label)}</option>`).join('');
  $('sort1').innerHTML = opts;
  $('sort2').innerHTML = `<option value="">nichts weiter</option>` + opts;

  $('minPlayers').addEventListener('input', e => { state.minPlayers = Math.max(0, +e.target.value || 0); save(); limit = 200; render(); });
  $('days').addEventListener('change', e => { state.days = +e.target.value; update(); });
  $('sort1').addEventListener('change', e => {
    const key = e.target.value;
    state.sort[0] = {key, dir:(SORTS.find(s=>s.key===key)||{dir:-1}).dir};
    if (state.sort[1] && state.sort[1].key === key) state.sort.length = 1;
    update();
  });
  $('sort2').addEventListener('change', e => {
    const key = e.target.value;
    if (!key || key === state.sort[0].key) state.sort.length = 1;
    else state.sort[1] = {key, dir:(SORTS.find(s=>s.key===key)||{dir:-1}).dir};
    update();
  });
  $('dir1').addEventListener('click', () => { state.sort[0].dir *= -1; update(); });
  $('dir2').addEventListener('click', () => { if (state.sort[1]) { state.sort[1].dir *= -1; update(); } });
  $('q').addEventListener('input', e => { state.q = e.target.value; save(); limit = 200; render(); });
  $('genre').addEventListener('change', e => { state.genre = e.target.value; update(); });
  $('maxAge').addEventListener('change', e => { state.maxAge = e.target.value; update(); });
  $('onlyUp').addEventListener('change', e => { state.onlyUp = e.target.checked; update(); });
  $('onlyTrend').addEventListener('change', e => { state.onlyTrend = e.target.checked; update(); });
  $('head').addEventListener('click', e => { const th = e.target.closest('th[data-key]'); if (th) setSortFromHeader(th.dataset.key, e.shiftKey); });
  $('head').addEventListener('keydown', e => { const th = e.target.closest('th[data-key]'); if (th && (e.key === 'Enter' || e.key === ' ')) { e.preventDefault(); setSortFromHeader(th.dataset.key, e.shiftKey); } });
  $('more').addEventListener('click', () => { limit += 200; render(); });
  $('reload').addEventListener('click', async () => {
    const b = $('reload'); b.disabled = true; b.textContent = 'Lädt …';
    try { setData(await loadData()); } catch(e) { $('note').className = 'note err'; $('note').textContent = 'Daten konnten nicht geladen werden. Prüf deine Internetverbindung.'; }
    b.disabled = false; b.textContent = 'Neu laden';
  });
  if (document.fonts && document.fonts.ready) document.fonts.ready.then(() => DATA && ['days','sort1','sort2'].forEach(id => fitSelect($(id))));
  setInterval(() => DATA && updateMeta(), 60000);

  if (window.RADAR_INLINE) { setData(window.RADAR); return; }
  $('reload').hidden = false;
  loadData().then(setData).catch(() => {
    $('meta').textContent = 'Die Messdaten (data.js) wurden nicht gefunden.';
    $('note').className = 'note err';
    $('note').textContent = 'Die Seite ist da, aber es gibt noch keine Daten. Starte auf GitHub unter „Actions“ den Ablauf „Trend-Radar“ und lade die Seite nach ein paar Minuten neu.';
  });
}
init();
</script>
</body>
</html>
"""

# App-Symbole (eigenes Design)
ICON_192 = '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'
ICON_512 = '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'


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
