# %% [markdown]
# # Journal S2 : scan Binance + MEXC pour le tableau de bord (continuation, FVG 2 h, stop journalier)
#
# **Version automatique** : ce fichier tourne aussi tout seul sur GitHub (toutes les 2 h) et envoie les notifications ntfy.
#
# **Chaque jour (ou plusieurs fois par jour) :** `Exécution > Tout exécuter` dans Google Colab (≈ 5 min). Le notebook télécharge
# un fichier `scan_AAAAMMJJ_HHMM.json` (une copie est aussi gardée sur votre Drive) : déposez-le dans le tableau de bord en ligne.
#
# **Une seule fois, pour l'historique :** mettez `MODE = "backtest"` (cellule 1), exécutez (≈ 30-40 min), déposez le fichier
# `backtest_….json` dans le tableau de bord, puis remettez `MODE = "quotidien"`.
#
# **Règle S2 · FVG 2 h, stop journalier, figée le 07/11/2026, ne pas modifier** : pendant une hausse d'au moins 15 % depuis le plus bas
# des 7 derniers jours, un FVG haussier **2 h** se forme · ordre limite au milieu du FVG, valable 3 jours · pas d'entrée si le prix ouvre
# directement sous le FVG · objectif 1 = retour au plus haut atteint avant l'entrée (objectif 2 = ce plus haut + 0,618 × la profondeur
# du repli, en information) · stop : clôture journalière sous le bas du FVG, creux de départ de la hausse en stop de sécurité · trade
# limité à 10 jours · une position à la fois par crypto · un trade ne compte que si BTC est au-dessus de sa moyenne 50 jours à l'entrée.
#
# Univers : Top 200 Binance (filtre halal) + cryptos classées jusqu'au rang 300 absentes de Binance mais cotées sur MEXC ; pour ces
# dernières, un signal ne compte que si la crypto a échangé au moins 1 M$ sur MEXC dans les 24 h qui le précèdent.
#
# **Étiquette suivie en parallèle** : « mieux que BTC sur 7 jours ».
#
# Le suivi réel (« paper trading ») compte les setups devenus exploitables à partir du **28/10/2026** (recalculés avec cette règle).

# %%
# =====================================================================
# 1. CONFIGURATION
# =====================================================================
MODE = "quotidien"                        # "quotidien" (chaque jour) ou "backtest" (une fois : historique 2024-2026)

import sys, subprocess, importlib
for _p in ["requests", "pandas", "numpy"]:
    try:
        importlib.import_module(_p)
    except ImportError:
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", _p], check=False)
import os, re, time, threading, warnings, hashlib, json, math
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import pandas as pd
import requests

warnings.filterwarnings("ignore")
UTC = "UTC"
CFG = {
    "OFFLINE_SYNTHETIC_DEMO": False,
    "OUT_DIR": os.environ.get("OUT_DIR", "/content/journal_fibo_fvg"),   # sur GitHub : dossier « data » du dépôt
    "DRIVE_DIR": "/content/drive/MyDrive/journal_fibo_fvg",
    "JOURNAL_START": "2026-10-28",            # début du suivi réel (setups exploitables à partir de cette date)
    "BACKTEST_START": "2024-10-27",
    "LOOKBACK_DAYS": 110,                     # historique lu chaque jour (moyenne 50 j de BTC + pumps + ordres + trades)
    # ---- RÈGLE S2 · STOP JOURNALIER (figée le 03/11/2026, ne pas modifier) ----
    "S2": {"run_min": 0.15, "lookback_h": 168, "expiry_h": 72, "max_hold_h": 240, "t2_ext": 0.618, "min_room": 0.02,
           "stop": "clôture journalière sous le FVG", "btc_filter": "SMA50 daily, veille", "fvg_tf": "2h"},
    "ETIQUETTE_RS_JOURS": 7,
    "ETIQUETTE_VOLUME": 1.5,                  # étiquette « volume réveillé » : volume 24 h ≥ 1,5 × l'habitude (suivie à part, hors règle)
    # ---- CRYPTOS MEXC AJOUTÉES LE 04/11/2026 : classées jusqu'au rang 300, absentes de Binance, ≥ 1 M$ échangés sur 24 h au signal ----
    "MEXC": {"actif": True, "rang_max": 300, "volume_24h_min": 1e6, "interval": "60m"},
    "REGLES_PRECEDENTES": ["13c72d5863f0"],   # empreinte avant l'ajout de MEXC (le journal continue)                  # étiquette « mieux que BTC sur 7 jours » (suivie en parallèle, ne change pas la règle)
    "CRITERIA": {"MIN_TRADES": 50, "MIN_GAIN": 0.005, "REVIEW_MONTHS": [6, 12]},
    "FEE": 0.001, "STOP_SLIP": 0.0005,
    "CHART_DAYS": 30,                         # profondeur du mini-graphique 4 h
    "KEEP_CLOSED_DAYS": 7,                    # trades terminés encore affichés dans le scan du jour
    "TOP_N": 200,
    "EXCLUDE": ["USDT", "USDC", "BUSD", "TUSD", "USDP", "DAI", "FDUSD", "USDD", "USDE", "USD1", "PYUSD", "USDS", "USDX",
                "RLUSD", "BFUSD", "EURC", "EURI", "AEUR", "FRAX", "GHO", "SUSDE", "USDG", "USDF", "XAUT", "PAXG",
                "WBTC", "WETH", "WBETH", "STETH", "WSTETH", "CBBTC", "CBETH", "RETH", "WEETH", "EETH", "BNSOL",
                "JITOSOL", "MSOL", "LBTC", "SOLVBTC", "TBTC", "WBNB", "BTCB", "BETH", "WTRX", "WSOL",
                "USAT", "USDGO", "USDQ", "USDA", "USDB", "KAG", "KAU", "XAUM"],   # stablecoins et jetons de métaux ajoutés le 05/11/2026
    # ---- Filtre halal ----
    # Il n'existe pas de liste unique faisant autorité : ce filtre applique des critères usuels de screening (riba, maysir,
    # gharar) à partir des catégories CoinGecko + d'une liste manuelle. Adaptez-le à la référence que vous suivez.
    "HALAL_FILTER": True,
    "EXCLURE_DOUTEUX": True,                  # memecoins et projets à forte composante spéculative (classés « douteux »)
    # Catégories CoinGecko exclues (reconnues par mots-clés dans leur nom)
    "HALAL_CAT_NON": ["lending", "borrowing", "yield", "derivative", "perpetual", "leverag", "gambling", "casino",
                      "betting", "prediction", "lottery", "synthetic dollar", "interest", "tokenized stock", "stock", "etf"],
    "HALAL_CAT_DOUTEUX": ["meme"],
    "HALAL_MAX_CAT_CALLS": 18,
    "HALAL_USE_COINGECKO_CATEGORIES": True,   # False = liste manuelle uniquement (instantané)
    "HALAL_TIME_BUDGET_S": 75,                # temps maximal consacré aux catégories CoinGecko ; au-delà, on continue sans
    # Liste manuelle (complète les catégories, appliquée même si CoinGecko est indisponible)
    "HALAL_MANUEL_NON": {
        "AAVE": "prêt/emprunt à intérêt", "COMP": "prêt/emprunt à intérêt", "MORPHO": "prêt/emprunt à intérêt",
        "SYRUP": "prêt à intérêt (Maple)", "KMNO": "prêt/emprunt (Kamino)", "XVS": "prêt/emprunt (Venus)",
        "JST": "prêt/emprunt (JustLend)", "SKY": "taux d'épargne / CDP (ex-Maker)", "MKR": "taux d'épargne / CDP",
        "PENDLE": "négociation de rendements", "ENA": "dollar synthétique à rendement (Ethena)",
        "HYPE": "plateforme de dérivés à levier", "ASTER": "plateforme de dérivés à levier", "DYDX": "dérivés à levier",
        "GMX": "dérivés à levier", "DRIFT": "dérivés à levier", "RLB": "jeux d'argent", "SHFL": "jeux d'argent",
        # ajoutés pour retrouver exactement l'univers du développement (exclus alors via les catégories CoinGecko)
        "UNI": "catégorie « Yield Farming »", "CRV": "catégorie « Yield Farming »", "CVX": "catégorie « Yield Farming »",
        "RAY": "catégorie « Perpetuals »", "AWE": "catégorie « Derivatives »",
        # ajoutés le 27/10/2026 (revue du Top 200)
        "CRCLB": "action tokenisée (Circle)", "SNDKB": "action tokenisée (SanDisk)", "INTCB": "action tokenisée (Intel)",
        "MSTRB": "action tokenisée (MicroStrategy)", "SOXLB": "ETF tokenisé à levier ×3",
        "ONDO": "bons du Trésor tokenisés (intérêt)", "SNX": "actifs synthétiques / dérivés", "YFI": "agrégateur de rendement",
        "KAVA": "prêt/emprunt", "SPK": "prêt (Spark)", "HUMA": "prêt (PayFi)", "FF": "dollar synthétique à rendement (Falcon)",
        "SUN": "stablecoin / rendement / prêt (Sun.io)", "WLFI": "prêt/emprunt (World Liberty Financial)",
    },
    "HALAL_MANUEL_DOUTEUX": {
        "DOGE": "memecoin", "SHIB": "memecoin", "PEPE": "memecoin", "BONK": "memecoin", "WIF": "memecoin",
        "FLOKI": "memecoin", "TRUMP": "memecoin", "FARTCOIN": "memecoin", "PENGU": "memecoin", "SPX": "memecoin",
        "BRETT": "memecoin", "POPCAT": "memecoin", "PNUT": "memecoin", "NEIRO": "memecoin", "TURBO": "memecoin",
        "BOME": "memecoin", "MEW": "memecoin", "1000SATS": "memecoin", "PUMP": "lanceur de memecoins (pump.fun)",
        "CAKE": "loterie et prédictions intégrées (PancakeSwap)", "JUP": "plateforme incluant des perpétuels à levier",
        "APE": "catégorie « IP Meme »", "币安人生": "memecoin",
        # ajoutés le 27/10/2026 (revue du Top 200) ; les RWA (ex. PLUME) restent autorisés
        "MEME": "memecoin", "MUBARAK": "memecoin", "BANANAS31": "memecoin", "LUNC": "memecoin",
        "INJ": "plateforme de dérivés", "SUSHI": "module de prêt", "RUNE": "module de prêt (THORChain)",
        "LDO": "staking liquide à rendement", "ETHFI": "restaking liquide à rendement", "REZ": "restaking liquide à rendement",
        "EIGEN": "restaking à rendement", "NMR": "paris sur des prédictions de marché", "WIN": "issu d'une plateforme de jeux",
        "CHZ": "jetons de supporters liés aux paris sportifs",
    },
    "HALAL_AUTORISER": [],                    # symboles à réintégrer manuellement malgré le filtre
    "MAX_WORKERS": 10, "HTTP_TIMEOUT": 20,
}
OUT = CFG["OUT_DIR"]
os.makedirs(OUT, exist_ok=True)
NOW = pd.Timestamp.now(tz=UTC).floor("h")
JOURNAL_START = pd.Timestamp(CFG["JOURNAL_START"], tz=UTC)
if MODE == "backtest":
    WIN_START = pd.Timestamp(CFG["BACKTEST_START"], tz=UTC)
    DATA_START = WIN_START - pd.Timedelta(days=60)
else:
    DATA_START = (NOW - pd.Timedelta(days=CFG["LOOKBACK_DAYS"])).floor("D")
    WIN_START = DATA_START + pd.Timedelta(days=55)          # setups détectés après la mise en route de la moyenne 50 j
FETCH_END = NOW
END = NOW
# Empreinte de la règle : identique d'un jour à l'autre tant que la règle n'est pas modifiée (vérifiée par le tableau de bord)
RULE_FP = hashlib.sha256(json.dumps({"strat": "S2H", "s2": CFG["S2"], "rs": CFG["ETIQUETTE_RS_JOURS"], "mexc": CFG["MEXC"], "fee": CFG["FEE"], "slip": CFG["STOP_SLIP"]}, sort_keys=True).encode()).hexdigest()[:12]
TFD = {"1h": pd.Timedelta("1h"), "2h": pd.Timedelta("2h"), "4h": pd.Timedelta("4h"), "1d": pd.Timedelta("1D")}
TFR = {"1h": "h", "2h": "2h", "4h": "4h", "1d": "D"}
DRIVE_OK = False
if not CFG["OFFLINE_SYNTHETIC_DEMO"]:
    try:
        from google.colab import drive  # type: ignore
        if not os.path.isdir("/content/drive/MyDrive"):
            drive.mount("/content/drive")
        os.makedirs(CFG["DRIVE_DIR"], exist_ok=True)
        DRIVE_OK = True
    except Exception:
        pass
JDIR = CFG["DRIVE_DIR"] if DRIVE_OK else OUT


def log(m):
    print(f"[{datetime.now(timezone.utc):%H:%M:%S} UTC] {m}", flush=True)


log(f"Mode {MODE} · données du {DATA_START:%Y-%m-%d} au {NOW:%Y-%m-%d %H:%M} UTC · règle {RULE_FP}"
    + ("  ⚠️ MODE DÉMO SYNTHÉTIQUE" if CFG["OFFLINE_SYNTHETIC_DEMO"] else ""))

# %%
# =====================================================================
# 2. UNIVERS ET DONNÉES (Binance, bougies clôturées uniquement)
# =====================================================================
REST = ["https://data-api.binance.vision/api/v3", "https://api.binance.com/api/v3"]
MEXC_REST = "https://api.mexc.com/api/v3"
SRC = {}                                           # symbole -> "mexc" pour les cryptos prises sur MEXC (sinon Binance)
_tls = threading.local()


def http_get(url, params=None, retries=5):
    if getattr(_tls, "s", None) is None:
        _tls.s = requests.Session()
    d = 1.0
    for _ in range(retries):
        try:
            r = _tls.s.get(url, params=params, timeout=CFG["HTTP_TIMEOUT"])
        except requests.RequestException:
            time.sleep(d); d *= 2
            continue
        if r.status_code == 200:
            return r
        if r.status_code in (400, 404):
            return None
        time.sleep(min(float(r.headers.get("Retry-After", d)), 60)); d *= 2
    return None


def cg_get(url, params=None):
    """Appel CoinGecko court : 2 essais maximum, attente plafonnée à 8 s (l'API gratuite limite fortement Colab)."""
    for k in range(2):
        try:
            r = requests.get(url, params=params, timeout=10)
        except requests.RequestException:
            r = None
        if r is not None and r.status_code == 200:
            return r
        if k == 0:
            wait = 8.0
            if r is not None and r.headers.get("Retry-After"):
                try:
                    wait = min(float(r.headers["Retry-After"]), 8.0)
                except ValueError:
                    pass
            time.sleep(wait)
    return None


def bj(path, params=None):
    for b in REST:
        r = http_get(b + path, params)
        if r is not None:
            return r.json()
    return None


CG_IDS, VOL24, MCAP_RANK = {}, {}, {}
if CFG["OFFLINE_SYNTHETIC_DEMO"]:
    SYMBOLS = ["BTCUSDT", "AAVEUSDT", "DOGEUSDT"] + [f"SYN{i:02d}USDT" for i in range(1, 25)]
    SYMBOLS_MX = ["MXA01USDT", "MXB02USDT"] if CFG["MEXC"]["actif"] else []
    for _s in SYMBOLS_MX:
        SRC[_s] = "mexc"
    UNIV_SOURCE = "synthétique"
else:
    ex = bj("/exchangeInfo")
    if ex is None:
        raise RuntimeError("Binance injoignable.")
    pairs = {x["baseAsset"].upper(): x["symbol"] for x in ex["symbols"] if x["quoteAsset"] == "USDT" and x["status"] == "TRADING"}
    r = cg_get("https://api.coingecko.com/api/v3/coins/markets", {"vs_currency": "usd", "order": "market_cap_desc", "per_page": 250})
    r2 = None
    if r is not None:                                               # Top 200 après filtres : on lit aussi la 2e page
        time.sleep(2.0)
        r2 = cg_get("https://api.coingecko.com/api/v3/coins/markets", {"vs_currency": "usd", "order": "market_cap_desc", "per_page": 250, "page": 2})
    SYMBOLS, seen = [], set()
    SYMBOLS_MX = []
    CG_IDS = {}
    mpairs = {}
    if CFG["MEXC"]["actif"]:
        _mx = http_get(MEXC_REST + "/exchangeInfo")
        if _mx is not None:
            mpairs = {x["baseAsset"].upper(): x["symbol"] for x in _mx.json().get("symbols", [])
                      if x.get("quoteAsset") == "USDT" and (str(x.get("status")) in ("1", "ENABLED", "TRADING") or x.get("isSpotTradingAllowed"))}
        else:
            log("MEXC injoignable : les cryptos MEXC sont ignorées pour ce scan")
    if r is not None:
        UNIV_SOURCE = f"Top {CFG['TOP_N']} CoinGecko au {NOW:%Y-%m-%d}"
        for c in r.json() + (r2.json() if r2 is not None else []):
            b = c["symbol"].upper()
            if b not in seen and b not in CFG["EXCLUDE"] and b in pairs:
                seen.add(b)
                SYMBOLS.append(pairs[b])
                CG_IDS[c["id"]] = b
                VOL24[b] = c.get("total_volume")
                MCAP_RANK[b] = c.get("market_cap_rank")
            elif (b not in seen and b not in CFG["EXCLUDE"] and b not in pairs and b in mpairs and b.isascii()
                  and (c.get("market_cap_rank") or 9999) <= CFG["MEXC"]["rang_max"]):
                seen.add(b)                                         # crypto absente de Binance mais cotée sur MEXC
                SYMBOLS_MX.append(mpairs[b])
                SRC[mpairs[b]] = "mexc"
                CG_IDS[c["id"]] = b
                VOL24[b] = c.get("total_volume")
                MCAP_RANK[b] = c.get("market_cap_rank")
    else:
        UNIV_SOURCE = f"Top {CFG['TOP_N']} par volume Binance au {NOW:%Y-%m-%d} (CoinGecko indisponible)"
        inv = {v: k for k, v in pairs.items()}
        for t in sorted([t for t in (bj("/ticker/24hr") or []) if t["symbol"] in inv], key=lambda t: -float(t["quoteVolume"])):
            if inv[t["symbol"]] not in CFG["EXCLUDE"] and not re.search(r"(UP|DOWN|BULL|BEAR)$", inv[t["symbol"]]):
                SYMBOLS.append(t["symbol"])
                VOL24[inv[t["symbol"]]] = float(t["quoteVolume"])

# ---- Filtre halal ----
HALAL_EXCL = {}           # base -> (statut, raison)
if CFG["HALAL_FILTER"]:
    for b, why in CFG["HALAL_MANUEL_NON"].items():
        HALAL_EXCL[b] = ("non halal", why)
    if CFG["EXCLURE_DOUTEUX"]:
        for b, why in CFG["HALAL_MANUEL_DOUTEUX"].items():
            HALAL_EXCL.setdefault(b, ("douteux", why))
    if not CFG["OFFLINE_SYNTHETIC_DEMO"] and CG_IDS and CFG["HALAL_USE_COINGECKO_CATEGORIES"]:
        t_start = time.time()
        log("Filtre halal : lecture des catégories CoinGecko (75 s maximum)…")
        rc = cg_get("https://api.coingecko.com/api/v3/coins/categories/list")
        cats = rc.json() if rc is not None else []
        kw = [(k, "non halal") for k in CFG["HALAL_CAT_NON"]] + ([(k, "douteux") for k in CFG["HALAL_CAT_DOUTEUX"]] if CFG["EXCLURE_DOUTEUX"] else [])
        chosen = []
        for c in cats:
            name = c.get("name", "").lower()
            for k, st in kw:
                if k in name:
                    chosen.append((c["category_id"], c["name"], st))
                    break
        # catégories les plus génériques d'abord (noms courts) : ~2/3 « non halal », ~1/3 « douteux »
        non = sorted([c for c in chosen if c[2] == "non halal"], key=lambda x: len(x[1]))
        dou = sorted([c for c in chosen if c[2] == "douteux"], key=lambda x: len(x[1]))
        k_dou = min(len(dou), max(3, CFG["HALAL_MAX_CAT_CALLS"] // 3))
        chosen = non[:CFG["HALAL_MAX_CAT_CALLS"] - k_dou] + dou[:k_dou]
        n_calls, n_fail = 0, 0
        for cid, cname, st in chosen:
            if time.time() - t_start > CFG["HALAL_TIME_BUDGET_S"]:
                log(f"  temps maximal atteint : {len(chosen) - n_calls} catégorie(s) non consultée(s), la liste manuelle s'applique")
                break
            time.sleep(2.0)                                                # limite de l'API gratuite CoinGecko
            rr = cg_get("https://api.coingecko.com/api/v3/coins/markets",
                        {"vs_currency": "usd", "category": cid, "order": "market_cap_desc", "per_page": 250})
            n_calls += 1
            n_fail += rr is None
            print(f"  · {cname} : {'ok' if rr is not None else 'indisponible'}", flush=True)
            for c in (rr.json() if rr is not None else []):
                b = CG_IDS.get(c.get("id"))
                if b and (b not in HALAL_EXCL or (st == "non halal" and HALAL_EXCL[b][0] == "douteux")):
                    HALAL_EXCL[b] = (st, f"catégorie CoinGecko « {cname} »")
        if rc is None:
            log("  CoinGecko indisponible : seule la liste manuelle s'applique")
        log(f"Filtre halal : {n_calls - n_fail}/{len(chosen)} catégories CoinGecko lues en {time.time() - t_start:.0f} s")
    for _sym in SYMBOLS:                               # noms en caractères non latins : memecoins chinois (ex. 币安人生, 牛来)
        _b = _sym[:-4]
        if not _b.isascii() and CFG["EXCLURE_DOUTEUX"]:
            HALAL_EXCL.setdefault(_b, ("douteux", "memecoin (nom en caractères non latins)"))
    for b in CFG["HALAL_AUTORISER"]:
        HALAL_EXCL.pop(b.upper(), None)
    _base = lambda sym: sym[:-4]
    HALAL_REMOVED = [(s, *HALAL_EXCL[_base(s)]) for s in SYMBOLS if _base(s) in HALAL_EXCL]
    SYMBOLS = [s for s in SYMBOLS if _base(s) not in HALAL_EXCL]
else:
    HALAL_REMOVED = []
SYMBOLS = SYMBOLS[:CFG["TOP_N"]]
SYMBOLS_MX = [s for s in SYMBOLS_MX if s[:-4] not in (HALAL_EXCL if CFG["HALAL_FILTER"] else {})]
SYMBOLS += SYMBOLS_MX
if "BTCUSDT" not in SYMBOLS:
    SYMBOLS = ["BTCUSDT"] + SYMBOLS                   # nécessaire pour la comparaison au marché
log(f"Univers : {len(SYMBOLS)} cryptos — {UNIV_SOURCE} ; dont {len(SYMBOLS_MX)} prises sur MEXC ; {len(HALAL_REMOVED)} écartées par le filtre halal")


def synthetic_1h(sym):
    seed = int(hashlib.md5(sym.encode()).hexdigest()[:8], 16)
    rng = np.random.default_rng(seed)
    idx = pd.date_range(DATA_START.floor("D"), FETCH_END, freq="1h", inclusive="left")
    n = len(idx)
    vol = 0.008 * rng.uniform(0.6, 1.4)
    r = rng.standard_t(4, n) * vol / 1.6 + np.repeat(rng.normal(0, vol / 6, n // 48 + 1), 48)[:n]
    for k in rng.integers(100, max(101, n - 100), max(2, n // 400)):      # quelques pumps pour que la démo produise des setups
        r[k:k + 48] += rng.uniform(0.005, 0.009)
    c = 10 * np.exp(np.cumsum(r))
    o = np.r_[c[0], c[:-1]]
    return pd.DataFrame({"open": o, "high": np.maximum(o, c) * (1 + np.abs(rng.normal(0, vol / 2, n))),
                         "low": np.minimum(o, c) * (1 - np.abs(rng.normal(0, vol / 2, n))), "close": c,
                         "volume": rng.lognormal(10, 0.5, n) * (1 + np.abs(r) * 80)}, index=idx).assign(qv=lambda d: d["volume"] * d["close"])


def resample(h1, tf):
    g = h1.resample({"2h": "2h", "4h": "4h", "1d": "1D"}[tf], label="left", closed="left", origin="epoch")
    df = g.agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum", **({"qv": "sum"} if "qv" in h1 else {})})
    return df[g["close"].count() == {"2h": 2, "4h": 4, "1d": 24}[tf]]


def fetch(sym, tf="1h"):
    rows, cur = [], int(DATA_START.timestamp() * 1000)
    end_ms = int(FETCH_END.timestamp() * 1000) - 1
    step = int(TFD[tf].total_seconds() * 1000)
    mexc = SRC.get(sym) == "mexc"
    while cur <= end_ms:
        if mexc:                                    # MEXC : même format que Binance, intervalle « 60m », 500 bougies par appel
            r_ = http_get(MEXC_REST + "/klines", {"symbol": sym, "interval": CFG["MEXC"]["interval"], "startTime": cur,
                                                  "endTime": min(end_ms, cur + 500 * step), "limit": 1000})
            data = r_.json() if r_ is not None else None
            time.sleep(0.15)
        else:
            data = bj("/klines", {"symbol": sym, "interval": tf, "startTime": cur, "endTime": end_ms, "limit": 1000})
        if data is None:
            if not rows:
                return None
            break
        if not data:
            if mexc and cur + 500 * step < end_ms:          # crypto pas encore cotée à cette date : on avance
                cur += 500 * step
                continue
            break
        rows += data
        nxt = int(data[-1][0]) + step
        if nxt <= cur:
            break
        cur = nxt
        if not mexc and len(data) < 1000:
            break
    if not rows:
        return None
    df = pd.DataFrame(rows).iloc[:, :8].astype(float)
    df.columns = ["t", "open", "high", "low", "close", "volume", "ct", "qv"][:df.shape[1]]
    if "qv" not in df:
        df["qv"] = df["volume"] * df["close"]
    df.index = pd.to_datetime(df["t"].astype("int64"), unit="ms", utc=True)
    df = df[pd.to_datetime(df["ct"].astype("int64"), unit="ms", utc=True) <= NOW + pd.Timedelta("1min")]   # bougies clôturées
    return df[~df.index.duplicated()][["open", "high", "low", "close", "volume", "qv"]]      # qv = volume en USDT


DATA = {}
t_dl = time.time()
if CFG["OFFLINE_SYNTHETIC_DEMO"]:
    for s in SYMBOLS:
        h1 = synthetic_1h(s)
        DATA[s] = {"1h": h1, "2h": resample(h1, "2h"), "4h": resample(h1, "4h"), "1d": resample(h1, "1d")}
else:
    log(f"Téléchargement des bougies 1 h ({len(SYMBOLS)} cryptos)…")
    with ThreadPoolExecutor(CFG["MAX_WORKERS"]) as ex_:
        futs = {ex_.submit(fetch, s): s for s in SYMBOLS if SRC.get(s) != "mexc"}
        futs.update({ex_.submit(fetch, s): s for s in SYMBOLS if SRC.get(s) == "mexc"})
        for k, f in enumerate(as_completed(futs), 1):
            s = futs[f]
            try:
                df = f.result()
            except Exception:
                df = None
            if df is not None and len(df) > 24 * 20:
                DATA[s] = {"1h": df, "2h": resample(df, "2h"), "4h": resample(df, "4h"), "1d": resample(df, "1d")}
            if k % 25 == 0 or k == len(SYMBOLS):
                log(f"  cryptos : {k}/{len(SYMBOLS)} ({time.time() - t_dl:.0f} s)")
log(f"{len(DATA)} cryptos avec données")

# %%
# =====================================================================
# 3. OUTILS (ATR, bougies « vector »)
# =====================================================================
def atr14(df):
    pc_ = df["close"].shift(1)
    tr = pd.concat([df["high"] - df["low"], (df["high"] - pc_).abs(), (df["low"] - pc_).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean().values


def vector_codes(df):
    """Bougies « vector » (méthode PVSRA) : 2 = climax (volume ≥ 200 % de la moyenne des 10 précédentes, ou volume × amplitude
    au plus haut des 10 précédentes), 1 = montante (volume ≥ 150 %), 0 = normale. Information seulement."""
    v = df["volume"].astype(float)
    avg = v.shift(1).rolling(10, min_periods=10).mean()
    vs = v * (df["high"] - df["low"])
    mx = vs.shift(1).rolling(10, min_periods=10).max()
    code = np.where((v >= 2 * avg) | (vs >= mx), 2, np.where(v >= 1.5 * avg, 1, 0))
    return np.where(avg.isna(), 0, code).astype(int)


VEC_LABEL = {2: "climax", 1: "montante", 0: "classique"}


# %%
# =====================================================================
# 4. MARCHÉ DE RÉFÉRENCE ET SORTIES (bougies 1 h)
# =====================================================================
def first(mask):
    i = np.flatnonzero(mask)
    return int(i[0]) if len(i) else -1


_BTC = DATA["BTCUSDT"]["1h"] if "BTCUSDT" in DATA else None
_GRID = pd.date_range(DATA_START.floor("h"), FETCH_END, freq="1h", inclusive="left")
_PO = np.full((len(DATA), len(_GRID)), np.nan, dtype=np.float32)
_PC = np.full((len(DATA), len(_GRID)), np.nan, dtype=np.float32)
for _i, (_s, _d) in enumerate(DATA.items()):
    _p = _GRID.get_indexer(_d["1h"].index)
    _ok = _p >= 0
    _PO[_i, _p[_ok]] = _d["1h"]["open"].values[_ok]
    _PC[_i, _p[_ok]] = _d["1h"]["close"].values[_ok]
_PC_FF = pd.DataFrame(_PC.T).ffill(limit=6).values.T


def btc_return(t0, t1_):
    if _BTC is None:
        return None
    i0, i1 = _BTC.index.searchsorted(t0), _BTC.index.searchsorted(t1_) - 1
    if i0 >= len(_BTC) or i1 < i0:
        return None
    return float(_BTC["close"].values[i1] / _BTC["open"].values[i0] - 1)


def basket_return(t0, t1_):
    i0, i1 = _GRID.searchsorted(t0), _GRID.searchsorted(t1_) - 1
    if i0 >= len(_GRID) or i1 < i0:
        return None
    r = _PC_FF[:, i1] / _PO[:, i0] - 1
    return float(np.nanmean(r)) if np.isfinite(r).any() else None


if "BTCUSDT" in DATA:
    _bd = DATA["BTCUSDT"]["1d"]["close"]
    _sma = _bd.rolling(50, min_periods=50).mean()
    _reg = (_bd > _sma).where(_sma.notna()).shift(1)            # décision sur la clôture de la veille
else:
    _bd, _sma, _reg = pd.Series(dtype=float), pd.Series(dtype=float), pd.Series(dtype=float)


def btc_regime(t):
    v = _reg[_reg.index <= t.floor("D")] if len(_reg) else _reg
    if not len(v) or pd.isna(v.iloc[-1]):
        return "inconnu"
    return "haussier" if bool(v.iloc[-1]) else "baissier"


def close_stop_mask(T, C, lvl):
    """True sur la dernière heure d'une bougie journalière (clôture 00:00 UTC) sous `lvl`."""
    return (C < lvl) & np.asarray((T + TFD["1h"]).hour == 0)


def exit_for(f, end_h, A, tgt, fvg_bas, T, O, H, L, C):
    n = len(T)
    if L[f] < A:
        return f, min(O[f], A) * (1 - CFG["STOP_SLIP"]), "stop A"
    ka, kt = first(L[f + 1:end_h] < A), first(H[f + 1:end_h] >= tgt)
    ka, kt = (ka + 1 if ka >= 0 else -1), (kt + 1 if kt >= 0 else -1)
    kc = first(close_stop_mask(T[f:end_h], C[f:end_h], fvg_bas))
    cands = [(k, o, st) for k, o, st in [(ka, 0, "stop A"), (kt, 1, "objectif atteint"), (kc, 2, "stop FVG")] if k >= 0]
    if cands:
        k, _, st = min(cands)
        j = f + k
        if st == "stop A":
            return j, min(O[j], A) * (1 - CFG["STOP_SLIP"]), st
        if st == "objectif atteint":
            return j, max(O[j], tgt), st
        return j, C[j] * (1 - CFG["STOP_SLIP"]), st
    if end_h < n:
        return end_h - 1, C[end_h - 1], "durée max"
    return n - 1, C[-1], "en cours"


def iso(t):
    return None if t is None or pd.isna(t) else pd.Timestamp(t).strftime("%Y-%m-%dT%H:%MZ")



# %%
# =====================================================================
# 5. S2 · STOP JOURNALIER : SIGNAUX, ORDRES, TRADES ET FICHIER POUR LE TABLEAU DE BORD
# =====================================================================
S2P = CFG["S2"]
FTF = S2P["fvg_tf"]                                   # unité de temps des FVG de la règle (2 h)
RSJ = CFG["ETIQUETTE_RS_JOURS"]


def r6(x):
    return None if x is None or not np.isfinite(x) else float(f"{float(x):.6g}")


def last_price(sym):
    return float(DATA[sym]["1h"]["close"].values[-1])


def regime_now():
    return btc_regime(NOW)


def vec_text(code, body):
    lab = {2: "vector climax (volume ≥ 200 %)", 1: "vector montante (volume ≥ 150 %)", 0: "classique"}.get(int(code), "—")
    return lab + (f" · corps {body:.1f} ATR".replace(".", ",") if body is not None and np.isfinite(body) else "")


def chart(sym, t_from):
    q = DATA[sym][FTF]                               # graphique en bougies 2 h, comme les FVG de la règle
    start = max(min(t_from, NOW - pd.Timedelta(days=CFG["CHART_DAYS"])), NOW - pd.Timedelta(days=60))   # au moins 30 jours de contexte
    q = q[q.index >= start]
    return {"t0": iso(q.index[0]) if len(q) else None, "step_h": int(TFD[FTF] / pd.Timedelta("1h")),
            "o": [r6(x) for x in q["open"].values], "h": [r6(x) for x in q["high"].values],
            "l": [r6(x) for x in q["low"].values], "c": [r6(x) for x in q["close"].values],
            "v": [int(x) for x in vector_codes(DATA[sym][FTF])[-len(q):]] if len(q) else []}


def period_label(t):
    t = pd.Timestamp(t)
    if t >= JOURNAL_START:
        return "suivi réel"
    return "2024-25" if t < pd.Timestamp("2025-10-27", tz=UTC) else "2025-26"


def ctx30(sym, t, ref):
    """Contexte (information, hors note) : position du prix d'entrée par rapport au grand mouvement des 30 jours précédents."""
    h = DATA[sym]["1h"]
    w = h[(h.index >= pd.Timestamp(t) - pd.Timedelta(days=30)) & (h.index < pd.Timestamp(t))]
    if len(w) < 48 or ref is None:
        return None
    j = int(np.argmax(w["high"].values))
    hi = float(w["high"].values[j])
    lo = float(w["low"].values[: j + 1].min())
    return {"hi30": r6(hi), "hi30_time": iso(w.index[j]), "recul": r6(ref / hi - 1),
            "efface": r6((hi - ref) / (hi - lo)) if hi > lo else None, "pump30": r6(hi / lo - 1) if lo > 0 else None}


_BTC1 = DATA["BTCUSDT"]["1h"]["close"] if "BTCUSDT" in DATA else None


def btc_close_at(t):
    if _BTC1 is None:
        return np.nan
    i = _BTC1.index.searchsorted(t, side="right") - 1
    return float(_BTC1.values[i]) if i >= 0 else np.nan


def rel_strength(T, C, k):
    """Variation de la crypto et de BTC sur les RSJ jours avant le signal (bougies déjà clôturées)."""
    k7 = k - RSJ * 24
    if k7 < 0 or k < 1:
        return None, None, False
    coin = C[k - 1] / C[k7] - 1
    b0, b1 = btc_close_at(T[k7]), btc_close_at(T[k - 1])
    btc = b1 / b0 - 1 if np.isfinite(b0) and np.isfinite(b1) and b0 > 0 else np.nan
    return float(coin), (float(btc) if np.isfinite(btc) else None), bool(np.isfinite(btc) and coin > btc)


def s2_score(rec, regime, run, rr, tight, dist, intact):
    items = [
        {"key": "btc", "label": "Marché : BTC au-dessus de sa moyenne 50 j", "value": regime, "points": 30 if regime == "haussier" else 0, "max": 30, "preuve": "solide"},
        {"key": "run", "label": "Hausse déjà faite à l'entrée", "value": f"+{run * 100:.0f} %",
         "points": 25 if run >= 0.5 else 15 if run >= 0.25 else 5, "max": 25, "preuve": "indice"},
        {"key": "rr", "label": "Gain / risque à l'objectif 1",
         "value": (f"stop trop serré ({(rec['entry'] - rec['fvg_bas']) / rec['entry'] * 100:.1f} % sous l'entrée)" if tight else f"{rr:.1f}") if np.isfinite(rr) else "—",
         "points": 0 if tight or not np.isfinite(rr) else 15 if rr >= 3 else 10 if rr >= 2 else 5 if rr >= 1 else 0, "max": 15, "preuve": "logique"},
        {"key": "dist", "label": "Distance au point d'entrée", "value": dist[0], "points": dist[1], "max": 10, "preuve": "pratique"},
    ]
    vol = VOL24.get(rec["symbol"][:-4])
    items.append({"key": "liq", "label": "Liquidité (volume 24 h)", "value": f"{vol / 1e6:,.0f} M$".replace(",", " ") if vol else "inconnue",
                  "points": 10 if (vol or 0) >= 20e6 else 5 if (vol or 0) >= 5e6 else 0, "max": 10, "preuve": "pratique"})
    items.append({"key": "fvg", "label": "FVG intact", "value": "oui" if intact else "clôture journalière sous le FVG",
                  "points": 10 if intact else 0, "max": 10, "preuve": "règle"})
    rs_txt = "—" if rec["rs_coin"] is None else (f"{'oui' if rec['rs7'] else 'non'} ({rec['rs_coin'] * 100:+.1f} % contre "
                                                 f"{(rec['rs_btc'] or 0) * 100:+.1f} % pour BTC)").replace(".", ",")
    info = [{"label": f"Mieux que BTC sur {RSJ} jours", "value": rs_txt, "preuve": "à confirmer"},
            {"label": "Taille du FVG", "value": f"{rec['fvg_atr']:.2f} ATR" if rec["fvg_atr"] is not None else "—", "preuve": "non démontré"},
            {"label": "Bougie du FVG (PVSRA)", "value": vec_text({"climax": 2, "montante": 1}.get(rec["fvg_vec"], 0), rec["fvg_body"]), "preuve": "information"},
            {"label": "Rang par capitalisation", "value": str(MCAP_RANK.get(rec["symbol"][:-4], "—")), "preuve": "information"}]
    return {"total": int(sum(i["points"] for i in items)), "max": 100, "items": items, "info": info, "rr": r6(rr)}


def add_ctx(rec, t):
    c = ctx30(rec["symbol"], t, rec["entry"])
    rec["ctx"] = c
    if c:
        rec["score"]["info"] += [
            {"label": "Contexte : plus haut des 30 jours", "value": f"{c['hi30']:.6g} (entrée {c['recul'] * 100:+.0f} % sous ce plus haut)".replace(".", ","), "preuve": "information"},
            {"label": "Contexte : part du grand mouvement déjà effacée", "value": f"{c['efface'] * 100:.0f} %" if c["efface"] is not None else "—", "preuve": "information"},
        ]
    return rec


RECS = []
for s, d in DATA.items():
    h1, q4 = d["1h"], d[FTF]                          # q4 : bougies des FVG (2 h depuis le 07/11/2026)
    T, O, H, L, C = h1.index, h1["open"].values, h1["high"].values, h1["low"].values, h1["close"].values
    hi4, lo4 = q4["high"].values, q4["low"].values
    atr4 = atr14(q4)
    vec4 = vector_codes(q4)
    body4 = np.abs(q4["close"].values - q4["open"].values) / atr4
    d1 = d["1d"]
    QV = h1["qv"].values if "qv" in h1 else (h1["volume"] * h1["close"]).values
    is_mx = SRC.get(s) == "mexc"
    # Étiquette « volume réveillé » : volume des 24 h avant le signal ÷ volume journalier habituel (médiane des 30 jours précédents)
    _q = pd.Series(np.nan_to_num(QV.astype(float)), index=T)
    _dq = _q.resample("1D").sum()
    _dq[_dq <= 0] = np.nan
    USUAL = _dq.rolling(30, min_periods=20).median().shift(1).reindex(T, method="ffill").values
    Q24 = _q.rolling(24, min_periods=20).sum().values
    busy_until = None
    for i in np.where(lo4[2:] > hi4[:-2])[0] + 2:
        known = q4.index[i] + TFD[FTF]
        if known < WIN_START - pd.Timedelta(days=3) or known > NOW:
            continue
        if busy_until is not None and known < busy_until:
            continue                                               # une seule position à la fois par crypto
        bas, haut = float(hi4[i - 2]), float(lo4[i])
        k = T.searchsorted(known)
        k0 = max(0, k - S2P["lookback_h"])
        if k - k0 < 24 or k > len(T):
            continue
        a = k0 + int(np.argmin(L[k0:k]))
        A = float(L[a])
        if float(H[a:k].max()) / A - 1 < S2P["run_min"] or q4.index[i - 1] < T[a] or bas <= A:
            continue
        if is_mx and (k < 24 or float(np.nansum(QV[k - 24:k])) < CFG["MEXC"]["volume_24h_min"]):
            continue                                               # crypto MEXC trop peu échangée sur les 24 h avant le signal
        mid = (bas + haut) / 2
        e = min(len(T), k + S2P["expiry_h"])
        expires = known + pd.Timedelta(hours=S2P["expiry_h"])
        kb = first(L[k:e] < A)
        lim = k + kb + 1 if kb >= 0 else e
        kf = first(L[k:lim] <= mid)
        rs_coin, rs_btc, rs7 = rel_strength(T, C, k)
        rec = {"id": f"S2H-{s[:-4]}-{known:%Y%m%d%H}", "strat": "S2H", "mcap_rank": MCAP_RANK.get(s[:-4]), "symbol": s, "base": s[:-4],
               "A": r6(A), "A_time": iso(T[a]), "arm": iso(known), "expires": iso(expires), "fvg_tf": FTF, "fvg_bas": r6(bas),
               "fvg_haut": r6(haut), "fvg_atr": r6((haut - bas) / atr4[i - 1]) if np.isfinite(atr4[i - 1]) else None,
               "entry": r6(mid), "fib": None, "fvg_vec": VEC_LABEL[int(vec4[i - 1])],
               "fvg_body": r6(body4[i - 1]) if np.isfinite(body4[i - 1]) else None, "stop_fvg": r6(bas), "stop_A": r6(A),
               "price": r6(last_price(s)), "fill_time": None, "fill": None, "results": {}, "rank": 1,
               "rs7": rs7, "rs_coin": r6(rs_coin) if rs_coin is not None else None, "rs_btc": r6(rs_btc) if rs_btc is not None else None,
               "source": "mexc" if is_mx else "binance"}
        _vx = Q24[k - 1] / USUAL[k - 1] if (k >= 1 and np.isfinite(USUAL[k - 1]) and USUAL[k - 1] > 0) else np.nan
        rec["vol24x"] = r6(_vx) if np.isfinite(_vx) else None
        rec["vol_ok"] = bool(np.isfinite(_vx) and _vx >= CFG["ETIQUETTE_VOLUME"])
        px_now = last_price(s)
        if kf < 0:
            if kb >= 0:
                rec["order"] = "annulé : creux cassé"
            else:
                rec["order"] = "expiré" if NOW >= expires else "en attente"
            Hn = float(H[a:].max())
            jh = a + int(np.argmax(H[a:]))
            rec.update({"B": r6(Hn), "B_time": iso(T[jh]), "pump": r6(Hn / A - 1), "duree_h": int(jh - a),
                        "targets": {"1": r6(Hn), "1.618": r6(Hn + S2P["t2_ext"] * (Hn - mid))}, "provisoire": rec["order"] == "en attente"})
            reg = regime_now()
            dist = px_now / mid - 1
            dpts = (f"{dist * 100:+.1f} %", 10 if 0 <= dist <= 0.10 else 5 if 0 <= dist <= 0.25 else 0) if rec["order"] == "en attente" else (rec["order"], 0)
            intact = bool(not len(d1) or float(d1["close"].values[-1]) >= bas)
        else:
            f = k + kf
            if O[f] < bas:
                continue                                           # ouverture directement sous le FVG : pas d'entrée
            px = float(min(O[f], mid))
            Hf = float(H[a:f].max())
            jh = a + int(np.argmax(H[a:f]))
            if Hf / px - 1 < S2P["min_room"]:
                continue
            tg = {"1": Hf, "1.618": Hf + S2P["t2_ext"] * (Hf - px)}
            end_h = min(len(T), f + S2P["max_hold_h"])
            res = {}
            for tk, tgt in tg.items():
                j, ex_px, st_ = exit_for(f, end_h, A, tgt, bas, T, O, H, L, C)
                t_out = T[j] + TFD["1h"]
                res[tk] = {"status": st_, "exit_time": iso(t_out) if st_ != "en cours" else None, "exit": r6(ex_px) if st_ != "en cours" else None,
                           "ret": r6(ex_px / px - 1 - 2 * CFG["FEE"]), "btc": btc_return(T[f], t_out), "panier": basket_return(T[f], t_out),
                           "heures": float((t_out - T[f]) / pd.Timedelta("1h"))}
            r1 = res["1"]                                          # la position suit l'objectif 1 (objectif 2 : information)
            busy_until = pd.Timestamp(r1["exit_time"]) if r1["exit_time"] else NOW + pd.Timedelta(days=30)
            reg = btc_regime(T[f])
            rec.update({"order": "exécuté", "fill_time": iso(T[f]), "fill": r6(px), "B": r6(Hf), "B_time": iso(T[jh]), "pump": r6(Hf / A - 1),
                        "duree_h": int(jh - a), "targets": {k_: r6(v_) for k_, v_ in tg.items()}, "results": res, "provisoire": False})
            dpts = ("entrée exécutée", 10)
            intact = True
        risk = rec["entry"] - bas
        rr = (rec["targets"]["1"] - rec["entry"]) / risk if risk > 0 else np.nan
        tight = risk / rec["entry"] < 0.02
        rec["regime"] = reg
        rec["conforme"] = reg == "haussier"
        rec["score"] = s2_score(rec, reg, rec["pump"], rr, tight, dpts, intact)
        rec["paper"] = known >= JOURNAL_START
        rec["periode"] = period_label(rec["fill_time"] or known)
        add_ctx(rec, known)
        RECS.append(rec)
log(f"S2 · stop journalier : {len(RECS)} ordres détectés, dont {sum(1 for r in RECS if r['order'] == 'exécuté')} exécutés "
    f"({sum(1 for r in RECS if r.get('vol_ok'))} avec l'étiquette « volume réveillé »)")

LATEST = {}
for _r in RECS:                                            # dernier signal de chaque crypto (les précédents ont été remplacés)
    if _r["base"] not in LATEST or _r["arm"] > LATEST[_r["base"]]:
        LATEST[_r["base"]] = _r["arm"]


def is_live(rec):
    if rec["order"] == "en attente":
        return True
    if rec["order"] != "exécuté":                         # ordre annulé ou expiré : affiché quelques jours, et seulement le dernier
        return rec["arm"] == LATEST[rec["base"]] and pd.Timestamp(rec["arm"]) >= NOW - pd.Timedelta(days=S2P["expiry_h"] / 24 + 3)
    ends = [v.get("exit_time") for v in rec["results"].values()]
    if any(e is None for e in ends):
        return True
    return max(pd.Timestamp(e) for e in ends) >= NOW - pd.Timedelta(days=CFG["KEEP_CLOSED_DAYS"])


def all_prices():
    """Dernier prix de toutes les paires USDT (Binance, puis MEXC pour le reste) : sert au suivi de vos positions hors univers."""
    if CFG["OFFLINE_SYNTHETIC_DEMO"]:
        return {}
    out = {}
    for t in (bj("/ticker/price") or []):
        if t.get("symbol", "").endswith("USDT"):
            out[t["symbol"][:-4]] = r6(float(t["price"]))
    if CFG["MEXC"]["actif"]:
        r_ = http_get(MEXC_REST + "/ticker/price")
        for t in (r_.json() if r_ is not None else []):
            b = t.get("symbol", "")[:-4]
            if t.get("symbol", "").endswith("USDT") and b not in out and b.isascii() and t.get("price"):
                out[b] = r6(float(t["price"]))
    return out


GEN = iso(NOW)
STAMP = f"{NOW:%Y%m%d_%H%M}"
RULE_TEXT = ("Pendant une hausse d'au moins 15 % depuis le plus bas des 7 derniers jours, un FVG haussier 2 h se forme · ordre limite au milieu "
             "du FVG, valable 3 jours · objectif 1 = retour au plus haut atteint avant l'entrée (objectif 2 = ce plus haut + 0,618 × la profondeur "
             "du repli, en information) · stop : clôture journalière sous le bas du FVG, creux de départ en sécurité · trade limité à 10 jours · "
             "une position à la fois par crypto · ne compte que si BTC est au-dessus de sa moyenne 50 jours à l'entrée. "
             f"Étiquette suivie en parallèle : mieux que BTC sur {RSJ} jours. Univers : Top 200 Binance + cryptos classées jusqu'au rang "
             f"{CFG['MEXC']['rang_max']} absentes de Binance mais cotées sur MEXC (signal compté si ≥ 1 M$ échangés sur 24 h).")
UNIV = {"n": len(DATA), "source": UNIV_SOURCE, "halal_exclues": len(HALAL_REMOVED), "symbols": sorted(s_[:-4] for s_ in DATA)}

if MODE == "backtest":
    trades = []
    for rec in RECS:
        if rec["order"] != "exécuté" or rec["paper"] or pd.Timestamp(rec["fill_time"]) < WIN_START:
            continue
        for tk, v in rec["results"].items():
            if v["status"] == "en cours":
                continue
            trades.append({"strat": "S2H", "rs7": rec["rs7"], "vol_ok": rec.get("vol_ok", False), "rk": rec.get("mcap_rank"), "base": rec["base"], "periode": rec["periode"],
                           "fill_time": rec["fill_time"], "exit_time": v["exit_time"], "objectif": tk, "status": v["status"],
                           "ret": r6(v["ret"]), "btc": r6(v["btc"]) if v["btc"] is not None else None,
                           "panier": r6(v["panier"]) if v["panier"] is not None else None, "regime": rec["regime"],
                           "efface": (rec.get("ctx") or {}).get("efface"), "vec": rec.get("fvg_vec")})
    payload = {"kind": "fibo_fvg_backtest", "version": 3, "strat": "S2H", "rule_fp": RULE_FP, "rule_text": RULE_TEXT, "generated_at": GEN,
               "from": iso(WIN_START), "to": iso(min(NOW, JOURNAL_START)), "universe": UNIV, "demo": CFG["OFFLINE_SYNTHETIC_DEMO"],
               "trades": trades}
    fname = f"backtest_{STAMP}.json"
    log(f"Backtest : {len(trades)} trades terminés (objectifs 1 et 2) entre le {WIN_START:%Y-%m-%d} et le {min(NOW, JOURNAL_START):%Y-%m-%d}")
else:
    # ---- Journal du suivi réel : les niveaux d'un setup sont figés à sa première détection ----
    jpath = os.path.join(JDIR, "journal_s2h.json")        # nouveau journal : règle FVG 2 h (07/11/2026)
    J = json.load(open(jpath)) if os.path.exists(jpath) else {"rule_fp": RULE_FP, "start": CFG["JOURNAL_START"], "entries": {}}
    if J.get("rule_fp") != RULE_FP:
        if J.get("rule_fp") in CFG.get("REGLES_PRECEDENTES", []):     # changement voulu (ajout des cryptos MEXC le 04/11/2026)
            J.setdefault("historique_regles", []).append({"rule_fp": J["rule_fp"], "jusqu_au": GEN})
            J["rule_fp"] = RULE_FP
        else:
            log("⚠️ La règle a changé depuis la création du journal : le suivi réel n'est plus comparable. Remettez la règle d'origine.")
    for rec in RECS:
        if not rec["paper"]:
            continue
        frozen = J["entries"].get(rec["id"])
        if frozen is None:
            J["entries"][rec["id"]] = frozen = {"first_seen": GEN, "strat": rec["strat"], "rs7": rec["rs7"], "vol_ok": rec["vol_ok"],
                                                "levels": {k: rec[k] for k in ["A", "B", "entry", "stop_fvg", "targets", "arm"]}}
        rec["first_seen"] = frozen["first_seen"]
        rec["revise"] = bool(abs(frozen["levels"]["entry"] / rec["entry"] - 1) > 0.001)
        rec["late"] = (pd.Timestamp(frozen["first_seen"]) - pd.Timestamp(rec["arm"])) > pd.Timedelta(hours=26)
        if rec["order"] == "exécuté" and "fill_targets" not in frozen:
            frozen["fill_targets"] = rec["targets"]               # objectifs figés à l'exécution
        frozen["last"] = {k: rec[k] for k in ["order", "fill_time", "fill", "regime", "results"]}
    J["updated"] = GEN
    json.dump(J, open(jpath, "w"), indent=1)
    log(f"Journal : {len(J['entries'])} setups suivis depuis le {CFG['JOURNAL_START']} ({jpath})")

    live = [r for r in RECS if is_live(r)]

    def _active(r):
        return r["order"] == "en attente" or (r["order"] == "exécuté" and any(v["status"] == "en cours" for v in r["results"].values()))
    charts = {r["id"]: chart(r["symbol"], pd.Timestamp(r["A_time"]) - pd.Timedelta(days=3)) for r in live if _active(r)}
    btc_series = []
    if len(_bd):
        for t, c in _bd.iloc[-120:].items():
            btc_series.append([t.strftime("%Y-%m-%d"), r6(c), r6(_sma.get(t, np.nan))])
    payload = {"kind": "fibo_fvg_scan", "version": 3, "strat": "S2H", "rule_fp": RULE_FP, "rule_text": RULE_TEXT, "generated_at": GEN,
               "journal_start": CFG["JOURNAL_START"], "universe": UNIV, "demo": CFG["OFFLINE_SYNTHETIC_DEMO"],
               "btc": {"price": r6(last_price("BTCUSDT")) if "BTCUSDT" in DATA else None,
                       "sma50": r6(_sma.iloc[-1]) if len(_sma) else None, "regime": regime_now(), "series": btc_series},
               "setups": live, "watch": [], "charts": charts,
               "prices": {s_[:-4]: r6(last_price(s_)) for s_ in DATA},
               "prices_all": all_prices(),
               "sources": {s_[:-4]: "mexc" for s_ in DATA if SRC.get(s_) == "mexc"}}     # dernier prix de chaque crypto de l'univers
    fname = "latest.json" if os.environ.get("GITHUB_ACTIONS") else f"scan_{STAMP}.json"   # GitHub : un seul fichier, remplacé à chaque scan
    n_pend = sum(1 for r in live if r["order"] == "en attente")
    log(f"Scan : {len(live)} setups affichés dont {n_pend} ordres en attente · BTC {payload['btc']['regime']}")

PATH = os.path.join(OUT, fname)


def clean(o):
    if isinstance(o, dict):
        return {str(k): clean(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [clean(v) for v in o]
    if isinstance(o, (np.floating, float)):
        return float(o) if np.isfinite(o) else None
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.bool_,)):
        return bool(o)
    if isinstance(o, pd.Timestamp):
        return iso(o)
    return o


txt = json.dumps(clean(payload), ensure_ascii=False, separators=(",", ":"), allow_nan=False)
open(PATH, "w", encoding="utf-8").write(txt)
if DRIVE_OK:
    open(os.path.join(CFG["DRIVE_DIR"], fname), "w", encoding="utf-8").write(txt)
log(f"Fichier : {PATH} ({len(txt) / 1024:.0f} Ko){' · copie sur Drive' if DRIVE_OK else ''}")
try:
    from google.colab import files  # type: ignore
    files.download(PATH)
    print("\n➡️  Déposez ce fichier dans le tableau de bord (bouton « Importer un scan »).")
except Exception:
    pass

# %%
# =====================================================================
# 6. NOTIFICATIONS (exécution automatique sur GitHub, canal ntfy)
# =====================================================================
NTFY_TOPIC = os.environ.get("NTFY_TOPIC", "").strip()
NOTIF_HORS_REGLE = os.environ.get("NOTIF_HORS_REGLE", "0") == "1"      # 1 = prévenir aussi quand BTC est baissier
NOTIF_SUIVI = os.environ.get("NOTIF_SUIVI", "0") == "1"                # 1 = prévenir aussi des exécutions, sorties et changements de régime BTC


def _paris(t):
    try:
        from zoneinfo import ZoneInfo
        return pd.Timestamp(t).tz_convert(ZoneInfo("Europe/Paris")).strftime("%d/%m %H:%M")
    except Exception:
        return pd.Timestamp(t).strftime("%d/%m %H:%M UTC")


def _p(x):
    return "—" if x is None else f"{x:.6g}".replace(".", ",")


def _pct(x):
    return "—" if x is None or not np.isfinite(x) else f"{x * 100:+.1f} %".replace(".", ",")


def ntfy(title, message, tags=None, priority=3):
    if not NTFY_TOPIC:
        return
    try:
        requests.post("https://ntfy.sh/", json={"topic": NTFY_TOPIC, "title": title, "message": message,
                                               "tags": tags or [], "priority": priority}, timeout=15)
    except Exception as e:
        log(f"Notification non envoyée : {e}")


if MODE == "quotidien" and NTFY_TOPIC and not CFG["OFFLINE_SYNTHETIC_DEMO"]:
    npath = os.path.join(JDIR, "notif_state.json")
    NS = json.load(open(npath)) if os.path.exists(npath) else {"sent": [], "regime": None}
    first_run = NS.get("strat") != "S2H"                    # nouvelle règle (FVG 2 h) : on repart d'un état propre, sans rafale de messages
    if first_run:
        NS = {"sent": [], "regime": NS.get("regime")}
    sent = set(NS["sent"])
    msgs = []
    reg = regime_now()
    if NOTIF_SUIVI and NS.get("regime") and NS["regime"] != reg:
        msgs.append(("📈 BTC repasse au-dessus de sa moyenne 50 jours" if reg == "haussier" else "📉 BTC repasse sous sa moyenne 50 jours",
                     ("Les nouveaux ordres S2 comptent à nouveau pour la règle." if reg == "haussier"
                      else "Les nouveaux ordres S2 ne comptent plus (hors règle) tant que BTC reste sous sa moyenne."),
                     ["chart_with_upwards_trend" if reg == "haussier" else "chart_with_downwards_trend"], 4))
    for rec in RECS:
        if not rec["paper"]:
            continue
        ok = rec["conforme"] or NOTIF_HORS_REGLE
        hr = "" if rec["conforme"] else " (hors règle : BTC baissier)"
        rs = (" · mieux que BTC 7 j" if rec["rs7"] else "") + (" · à trader sur MEXC" if rec.get("source") == "mexc" else "")
        if rec["order"] == "en attente" and f"{rec['id']}:new" not in sent:
            sent.add(f"{rec['id']}:new")
            if ok:
                t1, t2 = rec["targets"]["1"], rec["targets"].get("1.618")
                vt = f" · volume ×{rec['vol24x']:.1f}".replace(".", ",") if rec.get("vol_ok") and rec.get("vol24x") else ""
                msgs.append((f"🟢 {rec['base']}{vt}",
                             f"Apparu le {_paris(rec['arm'])}\n"
                             f"Achat : {_p(rec['entry'])}\n"
                             f"Stop : clôture journalière sous {_p(rec['stop_fvg'])} ({_pct(rec['stop_fvg'] / rec['entry'] - 1)})\n"
                             f"Objectif 1 : {_p(t1)} ({_pct(t1 / rec['entry'] - 1)})"
                             + (f"\nObjectif 2 : {_p(t2)} ({_pct(t2 / rec['entry'] - 1)})" if t2 else ""),
                             ["green_circle"], 4))
        if rec["order"] == "exécuté" and f"{rec['id']}:fill" not in sent:
            sent.add(f"{rec['id']}:fill")
            sent.add(f"{rec['id']}:new")
            if ok and NOTIF_SUIVI:
                t1 = rec["targets"]["1"]
                msgs.append((f"✅ Ordre exécuté : {rec['base']}{hr}",
                             f"Entrée à {_p(rec['fill'])} le {_paris(rec['fill_time'])}\n"
                             f"Stop : clôture journalière sous {_p(rec['stop_fvg'])} · objectif 1 : {_p(t1)} ({_pct(t1 / rec['fill'] - 1)})\n"
                             f"Sortie au plus tard le {_paris(pd.Timestamp(rec['fill_time']) + pd.Timedelta(hours=S2P['max_hold_h']))}{rs}",
                             ["white_check_mark"], 3))
        r1 = rec["results"].get("1") if rec["order"] == "exécuté" else None
        if r1 and r1["status"] != "en cours" and f"{rec['id']}:exit" not in sent:
            sent.add(f"{rec['id']}:exit")
            if ok and NOTIF_SUIVI:
                icon = {"objectif atteint": "🎯", "stop FVG": "🛑", "stop A": "🛑"}.get(r1["status"], "⏱")
                msgs.append((f"{icon} {rec['base']} : {r1['status']}{hr}",
                             f"Sortie à {_p(r1['exit'])} le {_paris(r1['exit_time'])} · résultat {_pct(r1['ret'])} (frais inclus)"
                             + (f" · BTC {_pct(r1['btc'])}" if r1.get("btc") is not None else ""),
                             ["dart" if r1["status"] == "objectif atteint" else "octagonal_sign"], 3))
        if rec["order"] in ("expiré", "annulé : creux cassé"):
            sent.add(f"{rec['id']}:new")
    if first_run:
        ntfy("Journal S2 : règle FVG 2 h en place",
             f"Scan automatique réussi ({len(DATA)} cryptos, BTC {reg}). Vous serez prévenu des nouveaux ordres à placer "
             f"{'(y compris hors règle)' if NOTIF_HORS_REGLE else '(conformes seulement)'}"
             f"{', des exécutions et des sorties' if NOTIF_SUIVI else ''}.", ["bell"], 3)
    else:
        for t, m, tg, pr in msgs[:15]:
            ntfy(t, m, tg, pr)
        if len(msgs) > 15:
            ntfy("Journal S2", f"{len(msgs) - 15} autres événements : voir le tableau de bord.", ["bell"], 2)
    NS["sent"] = sorted(sent)[-5000:]
    NS["regime"] = reg
    NS["strat"] = "S2H"
    json.dump(NS, open(npath, "w"))
    log(f"Notifications : {0 if first_run else min(len(msgs), 15)} envoyée(s){' (premier passage : état initialisé)' if first_run else ''}")
