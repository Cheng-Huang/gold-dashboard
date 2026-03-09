#!/usr/bin/env python3
"""
黄金投资 Dashboard — 数据获取 & 信号计算脚本
从 FRED API 获取核心宏观数据，计算衍生指标和信号，输出 data/dashboard.json
"""

import os, sys, json, math, datetime, time, io, zipfile, csv
from pathlib import Path
from typing import Optional, List, Dict, Tuple
import requests
import numpy as np
import yfinance as yf

# ─── 配置 ───────────────────────────────────────────────
# 自动加载 .env 文件
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
if not FRED_API_KEY:
    print("ERROR: FRED_API_KEY 环境变量未设置"); sys.exit(1)

OUTPUT_DIR = Path(__file__).parent / "data"
OUTPUT_DIR.mkdir(exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "dashboard.json"

# 拉取最近 N 年数据（用于计算长期均线和百分位）
HISTORY_YEARS = 3
TODAY = datetime.date.today()
OBS_START = (TODAY - datetime.timedelta(days=HISTORY_YEARS * 365)).isoformat()

# FRED 序列清单（黄金价格通过 yfinance 获取）
FRED_SERIES = {
    "realYield":  "DFII10",              # 10Y TIPS 实际利率
    "nominal10y": "DGS10",               # 10Y 名义利率
    "breakeven":  "T10YIE",              # 10Y Breakeven 通胀预期
    "dxy":        "DTWEXBGS",            # 广义贸易加权美元指数
    "coreCpi":    "CPILFESL",            # 核心CPI（指数水平）
    "corePce":    "PCEPILFE",            # 核心PCE（指数水平）
    "fedAssets":  "WALCL",               # 美联储总资产
    "vix":        "VIXCLS",              # VIX
    "fedFunds":   "DFF",                 # 联邦基金利率
    "fwdInflation": "T5YIFR",              # 5Y5Y远期通胀预期
    "yieldCurve": "T10Y2Y",               # 10Y-2Y 收益率曲线利差
}

# ─── FRED 数据获取 ──────────────────────────────────────
def fetch_fred(series_id: str) -> List[dict]:
    """返回 [{"date":"2024-01-02","value":1234.5}, ...] 已去除缺失值"""
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": OBS_START,
        "sort_order": "asc",
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    obs = r.json().get("observations", [])
    result = []
    for o in obs:
        if o["value"] == ".":
            continue
        result.append({"date": o["date"], "value": float(o["value"])})
    return result

def fetch_all() -> Dict[str, List[dict]]:
    """批量拉取所有 FRED 序列 + yfinance 黄金"""
    data = {}
    # FRED
    for key, sid in FRED_SERIES.items():
        print(f"  拉取 {key} ({sid})...", end=" ")
        try:
            series = fetch_fred(sid)
            data[key] = series
            print(f"✓ {len(series)} 条")
        except Exception as e:
            print(f"✗ {e}")
            data[key] = []
        time.sleep(0.25)          # 对 FRED 友好

    # 黄金价格（yfinance — COMEX 黄金期货连续合约）
    print(f"  拉取 gold (GC=F via yfinance)...", end=" ")
    try:
        df = yf.download("GC=F", start=OBS_START, progress=False)
        gold_series = []
        close_col = ("Close", "GC=F") if ("Close", "GC=F") in df.columns else "Close"
        for idx, row in df.iterrows():
            val = row[close_col]
            if not np.isnan(val):
                gold_series.append({"date": idx.strftime("%Y-%m-%d"), "value": float(val)})
        data["gold"] = gold_series
        print(f"✓ {len(gold_series)} 条")
    except Exception as e:
        print(f"✗ {e}")
        data["gold"] = []

    # 油价（yfinance — WTI原油期货连续合约）
    print(f"  拉取 oil (CL=F via yfinance)...", end=" ")
    try:
        df_oil = yf.download("CL=F", start=OBS_START, progress=False)
        oil_series = []
        close_col_oil = ("Close", "CL=F") if ("Close", "CL=F") in df_oil.columns else "Close"
        for idx, row in df_oil.iterrows():
            val = row[close_col_oil]
            if not np.isnan(val):
                oil_series.append({"date": idx.strftime("%Y-%m-%d"), "value": float(val)})
        data["oil"] = oil_series
        print(f"✓ {len(oil_series)} 条")
    except Exception as e:
        print(f"✗ {e}")
        data["oil"] = []

    # 白银价格（yfinance — COMEX 白银期货连续合约）
    print(f"  拉取 silver (SI=F via yfinance)...", end=" ")
    try:
        df_si = yf.download("SI=F", start=OBS_START, progress=False)
        silver_series = []
        close_col_si = ("Close", "SI=F") if ("Close", "SI=F") in df_si.columns else "Close"
        for idx, row in df_si.iterrows():
            val = row[close_col_si]
            if not np.isnan(val):
                silver_series.append({"date": idx.strftime("%Y-%m-%d"), "value": float(val)})
        data["silver"] = silver_series
        print(f"✓ {len(silver_series)} 条")
    except Exception as e:
        print(f"✗ {e}")
        data["silver"] = []

    # GDX 金矿ETF（yfinance）
    print(f"  拉取 GDX (yfinance)...", end=" ")
    try:
        df_gdx = yf.download("GDX", start=OBS_START, progress=False)
        gdx_series = []
        close_col_gdx = ("Close", "GDX") if ("Close", "GDX") in df_gdx.columns else "Close"
        for idx, row in df_gdx.iterrows():
            val = row[close_col_gdx]
            if not np.isnan(val):
                gdx_series.append({"date": idx.strftime("%Y-%m-%d"), "value": float(val)})
        data["gdx"] = gdx_series
        print(f"✓ {len(gdx_series)} 条")
    except Exception as e:
        print(f"✗ {e}")
        data["gdx"] = []

    return data

# ─── CFTC COT 数据获取 ────────────────────────────────
def fetch_cot_current() -> Optional[dict]:
    """从 CFTC 官方 Legacy Futures Only 当期报告获取 COMEX Gold 持仓数据"""
    print(f"  拉取 COT (CFTC deafut.txt)...", end=" ")
    try:
        r = requests.get("https://www.cftc.gov/dea/newcot/deafut.txt", timeout=30)
        r.raise_for_status()
        gold_line = None
        for line in r.text.split("\n"):
            if line.startswith('"GOLD - COMMODITY EXCHANGE'):
                gold_line = line
                break
        if not gold_line:
            print("✗ 未找到 GOLD COMEX 行")
            return None
        parts = [p.strip().strip('"') for p in gold_line.split(",")]
        # Legacy format fields:
        # [0]=Name, [1]=Date(YYMMDD), [2]=Date(YYYY-MM-DD), [3]=Code
        # [7]=Open Interest, [8]=NonComm Long, [9]=NonComm Short, [10]=NonComm Spreads
        # [11]=Comm Long, [12]=Comm Short
        # [37-46]=Changes from prior week
        result = {
            "date": parts[2],
            "openInterest": int(parts[7]),
            "nonCommLong": int(parts[8]),
            "nonCommShort": int(parts[9]),
            "nonCommSpreads": int(parts[10]),
            "commLong": int(parts[11]),
            "commShort": int(parts[12]),
            "specNetLong": int(parts[8]) - int(parts[9]),           # 投机净多头
            "specNetLongPct": round((int(parts[8]) - int(parts[9])) / int(parts[7]) * 100, 2) if int(parts[7]) > 0 else 0,
            # Changes
            "chgOpenInterest": int(parts[37]),
            "chgNonCommLong": int(parts[38]),
            "chgNonCommShort": int(parts[39]),
        }
        print(f"✓ {result['date']} 净多头:{result['specNetLong']}")
        return result
    except Exception as e:
        print(f"✗ {e}")
        return None

def fetch_cot_history() -> List[dict]:
    """从 CFTC 年度压缩包获取历史 COT 数据（用于计算百分位）"""
    print(f"  拉取 COT 历史 (CFTC zip)...", end=" ")
    all_records = []
    current_year = TODAY.year
    # 拉取最近 2 年 + 当年
    for year in range(current_year - 2, current_year + 1):
        url = f"https://www.cftc.gov/files/dea/history/deacot{year}.zip"
        try:
            r = requests.get(url, timeout=60)
            if r.status_code != 200:
                continue
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                for fname in z.namelist():
                    if fname.endswith(".txt"):
                        with z.open(fname) as f:
                            text = f.read().decode("utf-8", errors="replace")
                            for line in text.split("\n"):
                                if line.startswith('"GOLD - COMMODITY EXCHANGE'):
                                    parts = [p.strip().strip('"') for p in line.split(",")]
                                    if len(parts) > 12 and parts[7].strip():
                                        oi = int(parts[7])
                                        spec_net = int(parts[8]) - int(parts[9])
                                        all_records.append({
                                            "date": parts[2],
                                            "specNetLong": spec_net,
                                            "specNetLongPct": round(spec_net / oi * 100, 2) if oi > 0 else 0,
                                            "openInterest": oi,
                                        })
        except Exception:
            continue
    all_records.sort(key=lambda x: x["date"])
    print(f"✓ {len(all_records)} 周")
    return all_records

# ─── GLD ETF 持仓数据获取 ──────────────────────────────
def fetch_gld_holdings() -> dict:
    """通过 yfinance 获取 GLD ETF 的 totalAssets（AUM）和近期成交量来推导资金流"""
    print(f"  拉取 GLD ETF (yfinance)...", end=" ")
    try:
        t = yf.Ticker("GLD")
        info = t.info
        total_assets = info.get("totalAssets")          # 总资产（美元）
        nav_price = info.get("navPrice")                # NAV
        prev_close = info.get("previousClose")
        avg_volume = info.get("averageVolume")
        volume = info.get("volume")

        # 获取近 30 日的历史价格和成交量
        hist = yf.download("GLD", period="3mo", interval="1d", progress=False)
        close_col = ("Close", "GLD") if ("Close", "GLD") in hist.columns else "Close"
        vol_col = ("Volume", "GLD") if ("Volume", "GLD") in hist.columns else "Volume"

        # 用成交量 × 价格 近似资金流 (dollar volume)
        hist_data = []
        for idx, row in hist.iterrows():
            c = row[close_col]
            v = row[vol_col]
            if not np.isnan(c) and not np.isnan(v):
                hist_data.append({
                    "date": idx.strftime("%Y-%m-%d"),
                    "close": float(c),
                    "volume": int(v),
                    "dollarVolume": float(c * v),
                })

        # 计算资金流动量：最近 5 日 vs 前 5 日的 dollar volume 变化
        flow_5d_pct = None
        if len(hist_data) >= 10:
            recent_5 = sum(d["dollarVolume"] for d in hist_data[-5:])
            prev_5 = sum(d["dollarVolume"] for d in hist_data[-10:-5])
            if prev_5 > 0:
                flow_5d_pct = round((recent_5 / prev_5 - 1) * 100, 2)

        # 估算持仓量（盎司）: totalAssets / 金价
        holdings_oz = None
        if total_assets and prev_close:
            # GLD 每份约 1/10 盎司, 但用 totalAssets / 金价更直接
            # 从 yfinance 已有的 gold 数据取最新金价
            holdings_oz = round(total_assets / (prev_close * 10), 0)  # 粗略

        result = {
            "totalAssets": total_assets,
            "totalAssetsBln": round(total_assets / 1e9, 2) if total_assets else None,
            "navPrice": nav_price,
            "previousClose": prev_close,
            "volume": volume,
            "avgVolume": avg_volume,
            "flow5dPct": flow_5d_pct,
            "history": hist_data[-30:],   # 最近 30 日
        }
        print(f"✓ AUM=${result['totalAssetsBln']}B 5日流量:{flow_5d_pct}%")
        return result
    except Exception as e:
        print(f"✗ {e}")
        return {}

# ─── 央行购金数据 ───────────────────────────────────────
def fetch_central_bank_gold() -> dict:
    """央行购金数据（基于世界黄金协会 WGC 公开数据）
    由于 WGC 数据无免费API，采用已知季度数据 + 趋势标记方式
    数据来源：WGC Quarterly Gold Demand Trends Reports
    """
    print(f"  加载央行购金数据...", end=" ")
    # 季度净购金量（吨），来源: WGC Gold Demand Trends
    # 格式: (year, quarter, tonnes)
    quarterly_data = [
        ("2023-Q1", 228.4),
        ("2023-Q2", 102.9),
        ("2023-Q3", 337.1),
        ("2023-Q4", 229.4),
        ("2024-Q1", 289.7),
        ("2024-Q2", 183.4),
        ("2024-Q3", 186.2),
        ("2024-Q4", 332.9),
        ("2025-Q1", 243.7),
        ("2025-Q2", 214.5),
        ("2025-Q3", 267.3),
        ("2025-Q4", 295.6),
    ]
    
    # 年度合计
    yearly_totals = {}
    for period, tonnes in quarterly_data:
        year = period[:4]
        yearly_totals[year] = yearly_totals.get(year, 0) + tonnes
    
    # 最近4个季度（滚动年度）
    recent_4q = quarterly_data[-4:] if len(quarterly_data) >= 4 else quarterly_data
    rolling_annual = sum(t for _, t in recent_4q)
    
    # 趋势判断：与前4个季度对比
    if len(quarterly_data) >= 8:
        prev_4q = quarterly_data[-8:-4]
        prev_annual = sum(t for _, t in prev_4q)
        trend = "increasing" if rolling_annual > prev_annual * 1.05 else \
                "decreasing" if rolling_annual < prev_annual * 0.95 else "stable"
        yoy_change = round((rolling_annual / prev_annual - 1) * 100, 1) if prev_annual > 0 else 0
    else:
        trend = "unknown"
        yoy_change = 0
    
    result = {
        "latestQuarter": quarterly_data[-1][0] if quarterly_data else None,
        "latestQuarterlyTonnes": quarterly_data[-1][1] if quarterly_data else None,
        "rollingAnnualTonnes": round(rolling_annual, 1),
        "yearlyTotals": yearly_totals,
        "trend": trend,
        "yoyChangePct": yoy_change,
        "quarterlyHistory": [{"period": p, "tonnes": t} for p, t in quarterly_data],
        "source": "World Gold Council (WGC) Quarterly Reports",
    }
    print(f"✓ 最新: {result['latestQuarter']} {result['latestQuarterlyTonnes']}吨 趋势:{trend}")
    return result

# ─── 工具函数 ────────────────────────────────────────────
def values(series: list[dict]) -> np.ndarray:
    return np.array([d["value"] for d in series])

def latest(series: list[dict], default=None):
    return series[-1]["value"] if series else default

def latest_n(series: List[dict], n: int) -> List[dict]:
    return series[-n:] if len(series) >= n else series

def moving_avg(vals: np.ndarray, window: int) -> Optional[float]:
    if len(vals) < window:
        return None
    return float(np.mean(vals[-window:]))

def roc(vals: np.ndarray, window: int) -> Optional[float]:
    """变化率 (%)"""
    if len(vals) < window + 1:
        return None
    return float((vals[-1] / vals[-window - 1] - 1) * 100)

def annualized_vol(vals: np.ndarray, window: int = 20) -> Optional[float]:
    if len(vals) < window + 1:
        return None
    log_ret = np.diff(np.log(vals[-window - 1:]))
    return float(np.std(log_ret) * math.sqrt(252) * 100)

def rsi(vals: np.ndarray, period: int = 14) -> Optional[float]:
    if len(vals) < period + 1:
        return None
    deltas = np.diff(vals[-(period + 1):])
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    avg_gain = np.mean(gains)
    avg_loss = np.mean(losses)
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return float(100 - 100 / (1 + rs))

def yoy_pct(series: List[dict]) -> Optional[float]:
    """对于月度指数级别数据，计算同比涨幅 (%)"""
    if len(series) < 13:
        return None
    cur = series[-1]["value"]
    prev = series[-13]["value"]       # 约12个月前
    return float((cur / prev - 1) * 100)

def trend_label(ma_short, ma_long):
    if ma_short is None or ma_long is None:
        return "unknown"
    if ma_short < ma_long:
        return "declining"
    return "rising"

def pct_rank(vals: np.ndarray, current: float) -> float:
    """当前值在历史中的百分位 (0-100)"""
    if len(vals) == 0:
        return 50.0
    return float(np.sum(vals < current) / len(vals) * 100)

def zscore_divergence(series_a: np.ndarray, series_b: np.ndarray, window: int = 60) -> Optional[float]:
    """计算两个序列Z-Score差异（背离度）: 正值=A相对偏高, 负值=B相对偏高"""
    if len(series_a) < window or len(series_b) < window:
        return None
    a = series_a[-window:]
    b = series_b[-window:]
    z_a = (a[-1] - np.mean(a)) / np.std(a) if np.std(a) > 0 else 0
    z_b = (b[-1] - np.mean(b)) / np.std(b) if np.std(b) > 0 else 0
    return float(z_a - z_b)

# ─── 信号引擎 ────────────────────────────────────────────
def compute_signals(raw: dict, derived: dict) -> Tuple[list, list]:
    bullish, bearish = [], []

    ry = derived.get("realYield", {})
    dx = derived.get("dxy", {})
    inf = derived.get("inflation", {})
    fed = derived.get("fedBalance", {})
    vol = derived.get("volatility", {})
    tech = derived.get("technicals", {})
    vix_cur = derived.get("vix", {}).get("current")

    # 1. 实际利率 < 0
    if ry.get("current") is not None and ry["current"] < 0:
        bullish.append({"title": "实际利率为负", "strength": 5,
                        "detail": f"10Y TIPS: {ry['current']:.2f}%，负实际利率强烈利好黄金"})
    # 2. 实际利率均线下穿
    if ry.get("trend") == "declining":
        bullish.append({"title": "实际利率下行趋势", "strength": 4,
                        "detail": f"20日均线({ry.get('ma20','N/A'):.2f}) < 60日均线({ry.get('ma60','N/A'):.2f})"})
    # 3. 实际利率 > 2% 且上行
    if ry.get("current") is not None and ry["current"] > 2.0 and ry.get("trend") == "rising":
        bearish.append({"title": "实际利率高位且上行", "strength": 4,
                        "detail": f"10Y TIPS: {ry['current']:.2f}%，高实际利率压制黄金"})
    # 4-5 美元动量
    dxy_roc = dx.get("roc20d")
    if dxy_roc is not None:
        if dxy_roc < -2:
            bullish.append({"title": "美元走弱（20日动量）", "strength": 3,
                            "detail": f"美元指数 20日变化率: {dxy_roc:.1f}%"})
        elif dxy_roc > 2:
            bearish.append({"title": "美元走强（20日动量）", "strength": 3,
                            "detail": f"美元指数 20日变化率: {dxy_roc:.1f}%"})
    # 6-7 通胀
    pce_yoy = inf.get("corePceYoY")
    if pce_yoy is not None:
        if pce_yoy > 3.0:
            bullish.append({"title": "核心PCE同比 > 3%", "strength": 3,
                            "detail": f"核心PCE同比: {pce_yoy:.1f}%，高通胀支撑黄金"})
        elif pce_yoy < 2.0:
            bearish.append({"title": "核心PCE同比 < 2%", "strength": 2,
                            "detail": f"核心PCE同比: {pce_yoy:.1f}%，低通胀减弱黄金吸引力"})
    # 8-9 美联储资产负债表
    fed_wow = fed.get("weekOverWeekPct")
    if fed_wow is not None:
        if fed_wow > 0:
            bullish.append({"title": "美联储扩表（流动性宽松）", "strength": 3,
                            "detail": f"美联储总资产周环比: +{fed_wow:.2f}%"})
        elif fed_wow < -0.05:
            bearish.append({"title": "美联储缩表（QT 加速）", "strength": 3,
                            "detail": f"美联储总资产周环比: {fed_wow:.2f}%"})
    # 14 VIX
    if vix_cur is not None and vix_cur > 30:
        bullish.append({"title": "VIX > 30（避险升温）", "strength": 3,
                        "detail": f"VIX: {vix_cur:.1f}，恐慌情绪推高黄金避险需求"})
    # 15-16 金价 vs 200日均线
    ma200 = tech.get("ma200")
    gold_cur = derived.get("prices", {}).get("gold", {}).get("value")
    if ma200 and gold_cur:
        if gold_cur > ma200:
            bullish.append({"title": "金价站上200日均线", "strength": 3,
                            "detail": f"金价 ${gold_cur:.0f} > MA200 ${ma200:.0f}"})
        else:
            bearish.append({"title": "金价跌破200日均线", "strength": 4,
                            "detail": f"金价 ${gold_cur:.0f} < MA200 ${ma200:.0f}"})
    # 17-18 金叉/死叉
    crossover = tech.get("maCrossover")
    if crossover == "golden":
        bullish.append({"title": "50/200日均线金叉", "strength": 5,
                        "detail": f"MA50(${tech.get('ma50',0):.0f}) > MA200(${tech.get('ma200',0):.0f})"})
    elif crossover == "death":
        bearish.append({"title": "50/200日均线死叉", "strength": 5,
                        "detail": f"MA50(${tech.get('ma50',0):.0f}) < MA200(${tech.get('ma200',0):.0f})"})

    # 10-11 COT 持仓百分位
    cot_data = derived.get("cot", {})
    cot_pctl = cot_data.get("specNetLongPercentile")
    cot_cur = cot_data.get("current", {})
    if cot_pctl is not None:
        if cot_pctl > 80:
            bearish.append({"title": "COT投机净多头拥挤（>80百分位）", "strength": 3,
                            "detail": f"净多头:{cot_cur.get('specNetLong','N/A')} 百分位:{cot_pctl:.0f}%，多头拥挤回调风险"})
        elif cot_pctl < 20:
            bullish.append({"title": "COT投机净多头低位（<20百分位）", "strength": 3,
                            "detail": f"净多头:{cot_cur.get('specNetLong','N/A')} 百分位:{cot_pctl:.0f}%，空头极端有反弹机会"})

    # 背离度信号
    div = derived.get("divergence", {})
    div_z = div.get("zScore")
    if div_z is not None:
        if div_z > 1.5:
            bearish.append({"title": "实际利率-金价背离（超买）", "strength": 3,
                            "detail": f"背离Z-Score: {div_z:.2f}，金价相对实际利率偏高"})
        elif div_z < -1.5:
            bullish.append({"title": "实际利率-金价背离（超卖）", "strength": 3,
                            "detail": f"背离Z-Score: {div_z:.2f}，金价相对实际利率偏低"})

    # 12-13 GLD ETF 资金流
    gld = derived.get("gld", {})
    gld_flow = gld.get("flow5dPct")
    if gld_flow is not None:
        if gld_flow > 10:
            bullish.append({"title": "GLD ETF资金大幅流入", "strength": 2,
                            "detail": f"5日成交额变化: +{gld_flow:.1f}%，机构加仓"})
        elif gld_flow < -10:
            bearish.append({"title": "GLD ETF资金大幅流出", "strength": 2,
                            "detail": f"5日成交额变化: {gld_flow:.1f}%，机构减仓"})

    # 金银比信号
    sgr = derived.get("silverGoldRatio", {})
    sgr_cur = sgr.get("current")
    if sgr_cur is not None:
        if sgr_cur > 80:
            bearish.append({"title": "金银比极端偏高 (>80)", "strength": 2,
                            "detail": f"当前金银比: {sgr_cur:.1f}，历史极端偏高，均值回归风险——白银可能补涨或金价回调"})
        elif sgr_cur < 60:
            bullish.append({"title": "金银比偏低 (<60)", "strength": 2,
                            "detail": f"当前金银比: {sgr_cur:.1f}，偏低水平，黄金相对白银仍有上行空间"})

    # 收益率曲线信号
    yc = derived.get("yieldCurve", {})
    yc_cur = yc.get("current")
    if yc_cur is not None:
        if yc_cur < 0:
            bullish.append({"title": "收益率曲线倒挂", "strength": 3,
                            "detail": f"10Y-2Y利差: {yc_cur:.2f}%，曲线倒挂预示衰退风险，支撑避险黄金"})
        yc_roc = yc.get("roc20d")
        if yc_roc is not None and yc_roc > 0.3:
            bearish.append({"title": "收益率曲线急陡", "strength": 2,
                            "detail": f"10Y-2Y利差20日变化: +{yc_roc:.2f}%，曲线急陡化反映风险偏好回升"})

    # 排序：强度降序
    bullish.sort(key=lambda x: -x["strength"])
    bearish.sort(key=lambda x: -x["strength"])
    return bullish, bearish

# ─── 情绪评分 ──────────────────────────────────────────
def compute_sentiment(derived: dict) -> dict:
    """0=极度恐惧(利好黄金买入机会), 100=极度贪婪(黄金过热需警惕)"""
    components = {}
    score_parts = []

    # VIX (15%): VIX高→恐惧(低分), VIX低→贪婪(高分) — 从黄金投资者视角
    vix_cur = derived.get("vix", {}).get("current")
    if vix_cur is not None:
        # VIX 10-40 映射到 100-0 (注意反向：高VIX=低分=恐惧=利好黄金)
        vix_score = max(0, min(100, (40 - vix_cur) / 30 * 100))
        components["vix"] = {"value": vix_cur, "score": round(vix_score, 1)}
        score_parts.append((vix_score, 0.15))

    # 实际利率趋势 (25%): 下行→恐惧(低分→利好黄金), 上行→贪婪(高分)
    ry = derived.get("realYield", {})
    if ry.get("ma20") is not None and ry.get("ma60") is not None:
        diff = ry["ma20"] - ry["ma60"]
        # diff < -0.3 → 0(恐惧/利好黄金), diff > 0.3 → 100(贪婪/利空黄金)
        # 但我们要反转：实际利率下行利好，得分低=恐惧=利好黄金买点
        # 所以实际利率上行→高分(贪婪/黄金过热风险)，下行→低分(恐惧/买入机会)
        # Wait, re-reading the requirement: 恐惧端=利好黄金, 贪婪端=利空黄金
        # 实际利率下行 → 黄金利好 → 应该在"恐惧"端(低分) → 买入机会
        # Actually this is confusing. Let me re-read:
        # "贪婪 = 市场对黄金过度乐观（可能需要警惕回调）"
        # "恐惧 = 市场对黄金过度悲观（可能是买入机会）"
        # 实际利率下行 → 利好黄金 → 黄金涨，市场乐观 → 偏贪婪(高分)
        ry_score = max(0, min(100, (0.3 - diff) / 0.6 * 100))
        # diff very negative (利率大幅下行) → 利好黄金 → 黄金乐观 → 贪婪 → 高分
        # diff very positive (利率大幅上行) → 利空黄金 → 黄金悲观 → 恐惧 → 低分
        components["realYield"] = {"trend": ry.get("trend"), "diff": round(diff, 3), "score": round(ry_score, 1)}
        score_parts.append((ry_score, 0.25))

    # 美元趋势 (20%): 美元走弱→利好黄金→贪婪(高分)
    dx = derived.get("dxy", {})
    dxy_roc = dx.get("roc20d")
    if dxy_roc is not None:
        # roc < -3 → 美元大跌 → 黄金利好 → 偏贪婪(高分)
        dxy_score = max(0, min(100, (3 - dxy_roc) / 6 * 100))
        components["dxy"] = {"roc20d": round(dxy_roc, 2), "score": round(dxy_score, 1)}
        score_parts.append((dxy_score, 0.20))

    # 金价技术趋势 (10%): 多头排列→贪婪(高分)
    tech = derived.get("technicals", {})
    ma50 = tech.get("ma50")
    ma200 = tech.get("ma200")
    gold_cur = derived.get("prices", {}).get("gold", {}).get("value")
    if ma50 and ma200 and gold_cur:
        # 价格>MA50>MA200 → 100, 价格<MA200<MA50 → 0
        if gold_cur > ma50 > ma200:
            tech_score = 90
        elif gold_cur > ma200:
            tech_score = 65
        elif gold_cur > ma50:
            tech_score = 40
        else:
            tech_score = 15
        components["technicals"] = {"score": tech_score}
        score_parts.append((tech_score, 0.10))

    # COT 持仓 (15%): 投机净多头百分位高→市场过度看多→贪婪(高分)
    cot_data = derived.get("cot", {})
    cot_pct = cot_data.get("specNetLongPercentile", 50)
    cot_score = cot_pct  # 百分位直接映射：高百分位=多头拥挤=贪婪
    cot_detail = cot_data.get("current", {})
    components["cot"] = {
        "score": round(cot_score, 1),
        "specNetLong": cot_detail.get("specNetLong"),
        "percentile": round(cot_pct, 1),
        "date": cot_detail.get("date"),
    }
    score_parts.append((cot_score, 0.15))

    # GLD ETF 资金流 (15%): 资金流入→贪婪(高分)
    gld_data = derived.get("gld", {})
    flow_5d = gld_data.get("flow5dPct")
    if flow_5d is not None:
        # -20%→0(恐惧), +20%→100(贪婪)
        etf_score = max(0, min(100, (flow_5d + 20) / 40 * 100))
    else:
        etf_score = 50
    components["etfFlow"] = {
        "score": round(etf_score, 1),
        "totalAssetsBln": gld_data.get("totalAssetsBln"),
        "flow5dPct": flow_5d,
    }
    score_parts.append((etf_score, 0.15))

    total_weight = sum(w for _, w in score_parts)
    if total_weight > 0:
        weighted = sum(s * w for s, w in score_parts) / total_weight
    else:
        weighted = 50

    score = round(weighted, 1)
    if score <= 20:
        label = "极度恐惧"
    elif score <= 40:
        label = "恐惧"
    elif score <= 60:
        label = "中性"
    elif score <= 80:
        label = "贪婪"
    else:
        label = "极度贪婪"

    return {"score": score, "label": label, "components": components}

# ─── 雷达图数据 ─────────────────────────────────────────
def compute_radar(derived: dict) -> list[dict]:
    """六轴雷达：每个因子 0-10，越高越利好黄金"""
    axes = []

    # 1 实际利率（低=高分）
    ry = derived.get("realYield", {}).get("current")
    if ry is not None:
        s = max(0, min(10, (2.5 - ry) / 4 * 10))  # -1.5→10, 2.5→0
    else:
        s = 5
    axes.append({"axis": "实际利率", "value": round(s, 1)})

    # 2 美元（弱=高分）
    dxy_roc = derived.get("dxy", {}).get("roc20d")
    if dxy_roc is not None:
        s = max(0, min(10, (3 - dxy_roc) / 6 * 10))
    else:
        s = 5
    axes.append({"axis": "美元强弱", "value": round(s, 1)})

    # 3 通胀预期（高=高分）
    be = derived.get("inflation", {}).get("breakeven")
    if be is not None:
        s = max(0, min(10, (be - 1.5) / 2 * 10))  # 1.5→0, 3.5→10
    else:
        s = 5
    axes.append({"axis": "通胀预期", "value": round(s, 1)})

    # 4 流动性（宽松=高分）
    fed_wow = derived.get("fedBalance", {}).get("weekOverWeekPct")
    if fed_wow is not None:
        s = max(0, min(10, (fed_wow + 0.2) / 0.4 * 10))
    else:
        s = 5
    axes.append({"axis": "流动性", "value": round(s, 1)})

    # 5 避险需求（VIX高=高分）
    vix = derived.get("vix", {}).get("current")
    if vix is not None:
        s = max(0, min(10, (vix - 10) / 30 * 10))
    else:
        s = 5
    axes.append({"axis": "避险需求", "value": round(s, 1)})

    # 6 资金流入（COT净多头百分位 + GLD 资金流）
    cot_pct = derived.get("cot", {}).get("specNetLongPercentile", 50)
    gld_flow = derived.get("gld", {}).get("flow5dPct")
    # COT 百分位 0-100 → 0-10
    cot_part = cot_pct / 10
    # GLD flow: -20% → 0, +20% → 10
    gld_part = max(0, min(10, ((gld_flow or 0) + 20) / 4)) if gld_flow is not None else 5
    capital_score = round((cot_part * 0.6 + gld_part * 0.4), 1)
    axes.append({"axis": "资金流入", "value": max(0, min(10, capital_score))})

    return axes

# ─── 风险矩阵 ──────────────────────────────────────────
def compute_risk_matrix(derived: dict) -> dict:
    """四种投资标的的风险评估"""
    ry = derived.get("realYield", {})
    dx = derived.get("dxy", {})
    tech = derived.get("technicals", {})
    vol = derived.get("volatility", {})
    vix_cur = derived.get("vix", {}).get("current", 20)
    inf = derived.get("inflation", {})
    oil = derived.get("oil", {})

    def risk_level(score):
        if score < 35:
            return "low"
        elif score < 65:
            return "medium"
        return "high"

    def risk_to_signals(score, factors):
        risks, opps = [], []
        for f in factors:
            if f.get("bearish"):
                risks.append(f["name"])
            if f.get("bullish"):
                opps.append(f["name"])
        return risks, opps

    # 通用因子状态
    ry_bearish = ry.get("current", 1) > 1.5 and ry.get("trend") == "rising"
    ry_bullish = ry.get("trend") == "declining"
    dx_bearish = (dx.get("roc20d") or 0) > 1
    dx_bullish = (dx.get("roc20d") or 0) < -1
    oil_rising = (oil.get("roc20d") or 0) > 2
    oil_falling = (oil.get("roc20d") or 0) < -2
    tech_bullish = tech.get("maCrossover") == "golden"
    tech_bearish = tech.get("maCrossover") == "death"
    vol_high = (vol.get("current20d") or 15) > 20
    inf_high = (inf.get("corePceYoY") or 2.5) > 3.0

    results = {}

    # 实物黄金: 60%长期通胀 + 30%央行购金 + 10%技术面
    cb = derived.get("centralBankGold", {})
    cb_trend = cb.get("trend", "unknown")
    phys_score = 50
    if inf_high: phys_score -= 15
    if tech_bullish: phys_score -= 5
    if tech_bearish: phys_score += 5
    if cb_trend == "increasing": phys_score -= 10
    if cb_trend == "decreasing": phys_score += 10
    phys_risks = []
    phys_opps = []
    if not inf_high: phys_risks.append("通胀回落减弱保值需求")
    if inf_high: phys_opps.append("高通胀支撑保值需求")
    if tech_bullish: phys_opps.append("技术面多头趋势")
    if cb_trend in ("increasing", "stable"): phys_opps.append(f"央行持续购金({cb.get('rollingAnnualTonnes', 'N/A')}吨/年)")
    if cb_trend == "decreasing": phys_risks.append("央行购金放缓")
    results["physical"] = {
        "risk": risk_level(phys_score),
        "score": round(phys_score),
        "keyFactors": ["长期通胀趋势", "央行购金"],
        "riskSignals": phys_risks or ["暂无明显风险"],
        "oppSignals": phys_opps or ["长期保值属性"],
        "position": "适中" if phys_score < 65 else "保守"
    }

    # 黄金ETF: 40%实际利率 + 30%资金流 + 20%美元 + 10%技术面
    etf_score = 50
    if ry_bearish: etf_score += 20
    if ry_bullish: etf_score -= 20
    if dx_bearish: etf_score += 10
    if dx_bullish: etf_score -= 10
    if tech_bearish: etf_score += 5
    if tech_bullish: etf_score -= 5
    etf_risks, etf_opps = [], []
    if ry_bearish: etf_risks.append("实际利率上行压制ETF")
    if ry_bullish: etf_opps.append("实际利率下行利好ETF")
    if dx_bullish: etf_opps.append("美元走弱利好")
    if dx_bearish: etf_risks.append("美元走强压制")
    results["etf"] = {
        "risk": risk_level(etf_score),
        "score": round(etf_score),
        "keyFactors": ["实际利率", "资金流", "美元"],
        "riskSignals": etf_risks or ["暂无明显风险"],
        "oppSignals": etf_opps or ["中性观望"],
        "position": "适中" if etf_score < 65 else "保守"
    }

    # 黄金期货: 30%利率预期 + 25%COT + 25%波动率 + 20%技术面
    fut_score = 50
    if ry_bearish: fut_score += 15
    if vol_high: fut_score += 15
    if tech_bearish: fut_score += 10
    if ry_bullish: fut_score -= 15
    if tech_bullish: fut_score -= 10
    fut_risks, fut_opps = [], []
    if vol_high: fut_risks.append("波动率偏高，杠杆风险大")
    if ry_bearish: fut_risks.append("利率上行压制")
    if ry_bullish: fut_opps.append("利率下行利好")
    if tech_bullish: fut_opps.append("技术面看多")
    results["futures"] = {
        "risk": risk_level(fut_score),
        "score": round(fut_score),
        "keyFactors": ["利率预期", "COT持仓", "波动率"],
        "riskSignals": fut_risks or ["暂无明显风险"],
        "oppSignals": fut_opps or ["中性"],
        "position": "保守" if fut_score >= 65 else ("适中" if fut_score >= 35 else "激进")
    }

    # 金矿股: 30%金价趋势 + 25%股市环境(VIX) + 25%油价 + 20%美元
    miner_score = 55  # 默认偏高风险（杠杆）
    if tech_bearish: miner_score += 15
    if tech_bullish: miner_score -= 15
    if vix_cur > 25: miner_score += 10
    if dx_bearish: miner_score += 5
    if oil_rising: miner_score += 10
    miner_risks, miner_opps = [], []
    if vix_cur > 25: miner_risks.append("股市恐慌拖累矿业股")
    if tech_bearish: miner_risks.append("金价技术面走弱")
    if oil_rising: miner_risks.append("油价上涨推高开采成本")
    if tech_bullish: miner_opps.append("金价上涨放大矿企利润")
    if dx_bullish: miner_opps.append("弱美元利好矿企成本")
    if oil_falling: miner_opps.append("油价下跌降低开采成本")
    # 使用实际 GDX 数据
    gdx_data = derived.get("gdx", {})
    gdx_roc = gdx_data.get("roc20d")
    if gdx_roc is not None:
        if gdx_roc > 5: miner_opps.append("GDX 近期强势上涨")
        if gdx_roc < -5: miner_risks.append("GDX 近期大幅下跌")
    results["miners"] = {
        "risk": risk_level(miner_score),
        "score": round(miner_score),
        "keyFactors": ["金价趋势", "股市环境", "油价", "美元"],
        "riskSignals": miner_risks or ["波动天然较大"],
        "oppSignals": miner_opps or ["金价杠杆效应"],
        "position": "保守" if miner_score >= 65 else "适中"
    }

    return results

# ─── 走势预期 ──────────────────────────────────────────
def compute_outlook(derived: dict) -> dict:
    tech = derived.get("technicals", {})
    ry = derived.get("realYield", {})
    dx = derived.get("dxy", {})
    inf = derived.get("inflation", {})

    def label(b, n):
        if b > n:
            return "bullish"
        elif n > b:
            return "bearish"
        return "neutral"

    # 短期
    st_bull, st_bear = 0, 0
    if tech.get("maCrossover") == "golden": st_bull += 2
    if tech.get("maCrossover") == "death": st_bear += 2
    if tech.get("rsi14") and tech["rsi14"] < 30: st_bull += 1
    if tech.get("rsi14") and tech["rsi14"] > 70: st_bear += 1
    gold_cur = derived.get("prices", {}).get("gold", {}).get("value")
    ma200 = tech.get("ma200")
    if gold_cur and ma200 and gold_cur > ma200: st_bull += 1
    if gold_cur and ma200 and gold_cur < ma200: st_bear += 1
    short_dir = label(st_bull, st_bear)
    short_conf = "高" if abs(st_bull - st_bear) >= 3 else ("中" if abs(st_bull - st_bear) >= 1 else "低")
    short_factors = []
    if tech.get("maCrossover"): short_factors.append(f"MA交叉: {'金叉' if tech['maCrossover']=='golden' else '死叉'}")
    if tech.get("rsi14"): short_factors.append(f"RSI(14): {tech['rsi14']:.1f}")
    if gold_cur and ma200: short_factors.append(f"金价{'>' if gold_cur>ma200 else '<'}200日均线")

    # 中期
    mt_bull, mt_bear = 0, 0
    if ry.get("trend") == "declining": mt_bull += 2
    if ry.get("trend") == "rising": mt_bear += 2
    if (dx.get("roc20d") or 0) < -1: mt_bull += 1
    if (dx.get("roc20d") or 0) > 1: mt_bear += 1
    # 收益率曲线倒挂利好黄金
    yc = derived.get("yieldCurve", {})
    if yc.get("inverted"): mt_bull += 1
    mid_dir = label(mt_bull, mt_bear)
    mid_conf = "高" if abs(mt_bull - mt_bear) >= 3 else ("中" if abs(mt_bull - mt_bear) >= 1 else "低")
    mid_factors = []
    if ry.get("trend"): mid_factors.append(f"实际利率趋势: {'下行' if ry['trend']=='declining' else '上行'}")
    if dx.get("roc20d"): mid_factors.append(f"美元20日动量: {dx['roc20d']:.1f}%")
    if yc.get("current") is not None: mid_factors.append(f"收益率曲线: {yc['current']:.2f}% {'(倒挂)' if yc.get('inverted') else ''}")

    # 长期
    lt_bull, lt_bear = 0, 0
    if (inf.get("corePceYoY") or 2) > 2.5: lt_bull += 1
    if (inf.get("corePceYoY") or 2) < 1.5: lt_bear += 1
    # 5Y5Y远期通胀预期
    fwd_inf = inf.get("fwdInflation5y5y")
    if fwd_inf is not None:
        if fwd_inf > 2.5: lt_bull += 1
        elif fwd_inf < 2.0: lt_bear += 1
    # 央行购金（基于实际数据）
    cb = derived.get("centralBankGold", {})
    cb_trend = cb.get("trend", "unknown")
    if cb_trend in ("increasing", "stable"):
        lt_bull += 1
    elif cb_trend == "decreasing":
        pass  # 不加分但也不减分，央行减少购金≠利空
    long_dir = label(lt_bull, lt_bear)
    long_conf = "中"
    long_factors = []
    if inf.get("corePceYoY"): long_factors.append(f"核心PCE同比: {inf['corePceYoY']:.1f}%")
    if fwd_inf is not None: long_factors.append(f"5Y5Y远期通胀预期: {fwd_inf:.2f}%")
    if cb.get("rollingAnnualTonnes"):
        cb_label = f"央行年购金: {cb['rollingAnnualTonnes']:.0f}吨 ({cb_trend})"
    else:
        cb_label = "央行持续购金（结构性支撑）"
    long_factors.append(cb_label)

    summaries = {
        "bullish": {"short": "技术面偏多，短期有上行动能", "mid": "宏观因子共振偏多，中期看涨", "long": "通胀与购金支撑长期看多"},
        "bearish": {"short": "技术面偏空，短期需谨慎", "mid": "利率美元双压，中期承压", "long": "通缩与强美元压制长期预期"},
        "neutral": {"short": "信号矛盾，短期方向不明", "mid": "多空交织，中期观望", "long": "长期因子中性，需持续跟踪"},
    }

    return {
        "shortTerm": {
            "direction": short_dir, "confidence": short_conf,
            "factors": short_factors,
            "support": tech.get("support", []),
            "resistance": tech.get("resistance", []),
            "summary": summaries[short_dir]["short"]
        },
        "midTerm": {
            "direction": mid_dir, "confidence": mid_conf,
            "factors": mid_factors,
            "summary": summaries[mid_dir]["mid"]
        },
        "longTerm": {
            "direction": long_dir, "confidence": long_conf,
            "factors": long_factors,
            "summary": summaries[long_dir]["long"]
        }
    }

# ─── 综合信号 ──────────────────────────────────────────
def compute_overall_signal(bullish, bearish):
    bull_score = sum(s["strength"] for s in bullish)
    bear_score = sum(s["strength"] for s in bearish)
    diff = bull_score - bear_score
    if diff > 5:
        return {"direction": "bullish", "label": "看多", "bullScore": bull_score, "bearScore": bear_score}
    elif diff < -5:
        return {"direction": "bearish", "label": "看空", "bullScore": bull_score, "bearScore": bear_score}
    return {"direction": "neutral", "label": "中性", "bullScore": bull_score, "bearScore": bear_score}

# ─── 主流程 ────────────────────────────────────────────
def main():
    print("=" * 50)
    print("黄金投资 Dashboard — 数据更新")
    print("=" * 50)

    # 1. 拉取数据
    print("\n[1/5] 拉取 FRED 数据...")
    raw = fetch_all()

    print("\n[2/5] 拉取 CFTC COT & GLD ETF & 央行购金...")
    cot_current = fetch_cot_current()
    cot_history = fetch_cot_history()
    gld = fetch_gld_holdings()
    cb_gold = fetch_central_bank_gold()

    # 3. 计算衍生指标
    print("\n[3/5] 计算衍生指标...")
    gold_vals = values(raw["gold"])
    ry_vals = values(raw["realYield"])
    dxy_vals = values(raw["dxy"])

    gold_cur = latest(raw["gold"])
    gold_prev = raw["gold"][-2]["value"] if len(raw["gold"]) >= 2 else gold_cur

    derived = {}

    # 价格
    derived["prices"] = {
        "gold": {
            "value": gold_cur,
            "change1d": round(gold_cur - gold_prev, 2) if gold_cur and gold_prev else 0,
            "changePct1d": round((gold_cur / gold_prev - 1) * 100, 2) if gold_cur and gold_prev else 0,
        },
        "dxy": {
            "value": latest(raw["dxy"]),
            "change1d": round(latest(raw["dxy"], 0) - (raw["dxy"][-2]["value"] if len(raw["dxy"]) >= 2 else 0), 2),
        }
    }

    # 实际利率
    ry_ma20 = moving_avg(ry_vals, 20)
    ry_ma60 = moving_avg(ry_vals, 60)
    derived["realYield"] = {
        "current": latest(raw["realYield"]),
        "ma20": round(ry_ma20, 4) if ry_ma20 else None,
        "ma60": round(ry_ma60, 4) if ry_ma60 else None,
        "trend": trend_label(ry_ma20, ry_ma60),
    }

    # 美元
    derived["dxy"] = {
        "current": latest(raw["dxy"]),
        "roc20d": round(roc(dxy_vals, 20), 2) if roc(dxy_vals, 20) is not None else None,
        "trend": "rising" if (roc(dxy_vals, 20) or 0) > 0 else "declining",
    }

    # 通胀
    derived["inflation"] = {
        "breakeven": latest(raw["breakeven"]),
        "coreCpiYoY": round(yoy_pct(raw["coreCpi"]), 2) if yoy_pct(raw["coreCpi"]) else None,
        "corePceYoY": round(yoy_pct(raw["corePce"]), 2) if yoy_pct(raw["corePce"]) else None,
        "fwdInflation5y5y": latest(raw.get("fwdInflation", []), None),
    }

    # 美联储资产
    fed_vals = values(raw["fedAssets"])
    fed_wow = None
    if len(fed_vals) >= 2:
        fed_wow = round((fed_vals[-1] / fed_vals[-2] - 1) * 100, 4)
    derived["fedBalance"] = {
        "current": latest(raw["fedAssets"]),
        "weekOverWeekPct": fed_wow,
    }

    # VIX
    derived["vix"] = {"current": latest(raw["vix"])}

    # 联邦基金利率
    derived["fedFunds"] = {"current": latest(raw["fedFunds"])}

    # 油价
    oil_vals = values(raw["oil"]) if raw.get("oil") else np.array([])
    oil_roc20 = round(roc(oil_vals, 20), 2) if len(oil_vals) > 20 and roc(oil_vals, 20) is not None else None
    derived["oil"] = {
        "current": latest(raw.get("oil", []), None),
        "roc20d": oil_roc20,
    }

    # 金银比
    silver_vals = values(raw["silver"]) if raw.get("silver") else np.array([])
    silver_cur = latest(raw.get("silver", []), None)
    sgr_current = round(gold_cur / silver_cur, 2) if gold_cur and silver_cur and silver_cur > 0 else None
    sgr_history = []
    if raw.get("silver") and raw.get("gold"):
        silver_map = {d["date"]: d["value"] for d in raw["silver"]}
        for d in raw["gold"]:
            sv = silver_map.get(d["date"])
            if sv and sv > 0:
                sgr_history.append({"date": d["date"], "value": round(d["value"] / sv, 2)})
    sgr_hist_vals = np.array([d["value"] for d in sgr_history]) if sgr_history else np.array([])
    sgr_signal = "normal"
    if sgr_current is not None:
        if sgr_current > 80: sgr_signal = "extreme_high"
        elif sgr_current < 60: sgr_signal = "extreme_low"
    derived["silverGoldRatio"] = {
        "current": sgr_current,
        "silver_price": silver_cur,
        "gold_price": gold_cur,
        "signal": sgr_signal,
        "percentile": round(pct_rank(sgr_hist_vals, sgr_current), 1) if sgr_current and len(sgr_hist_vals) > 0 else None,
        "history": sgr_history,
    }

    # GDX 金矿ETF
    gdx_vals = values(raw["gdx"]) if raw.get("gdx") else np.array([])
    gdx_cur = latest(raw.get("gdx", []), None)
    gdx_prev = raw["gdx"][-2]["value"] if raw.get("gdx") and len(raw["gdx"]) >= 2 else gdx_cur
    gdx_roc20 = round(roc(gdx_vals, 20), 2) if len(gdx_vals) > 20 and roc(gdx_vals, 20) is not None else None
    gdx_change_pct = round((gdx_cur / gdx_prev - 1) * 100, 2) if gdx_cur and gdx_prev and gdx_prev > 0 else None
    gold_gdx_ratio = round(gold_cur / gdx_cur, 2) if gold_cur and gdx_cur and gdx_cur > 0 else None
    derived["gdx"] = {
        "current": gdx_cur,
        "changePct1d": gdx_change_pct,
        "roc20d": gdx_roc20,
        "goldGdxRatio": gold_gdx_ratio,
    }

    # 收益率曲线 (10Y-2Y)
    yc_vals = values(raw["yieldCurve"]) if raw.get("yieldCurve") else np.array([])
    yc_cur = latest(raw.get("yieldCurve", []), None)
    yc_roc20 = round(roc(yc_vals, 20), 2) if len(yc_vals) > 20 and roc(yc_vals, 20) is not None else None
    yc_inverted = yc_cur < 0 if yc_cur is not None else False
    if yc_inverted:
        yc_signal = "inverted"
    elif yc_roc20 is not None and yc_roc20 > 0.3:
        yc_signal = "steepening"
    elif yc_roc20 is not None and yc_roc20 < -0.3:
        yc_signal = "flattening"
    else:
        yc_signal = "normal"
    derived["yieldCurve"] = {
        "current": yc_cur,
        "roc20d": yc_roc20,
        "inverted": yc_inverted,
        "signal": yc_signal,
    }

    # 波动率
    vol20 = annualized_vol(gold_vals, 20)
    vol_level = "low" if vol20 and vol20 < 10 else ("high" if vol20 and vol20 > 20 else "medium")
    derived["volatility"] = {
        "current20d": round(vol20, 2) if vol20 else None,
        "level": vol_level,
    }

    # 技术面
    ma50 = moving_avg(gold_vals, 50)
    ma200 = moving_avg(gold_vals, 200)
    rsi14 = rsi(gold_vals, 14)
    crossover = "none"
    if ma50 and ma200:
        crossover = "golden" if ma50 > ma200 else "death"
    # 支撑/阻力（简化：用近期低点高点）
    recent = gold_vals[-60:] if len(gold_vals) >= 60 else gold_vals
    support = [round(float(np.percentile(recent, 10)), 0), round(float(np.percentile(recent, 25)), 0)]
    resistance = [round(float(np.percentile(recent, 75)), 0), round(float(np.percentile(recent, 90)), 0)]
    derived["technicals"] = {
        "ma50": round(ma50, 2) if ma50 else None,
        "ma200": round(ma200, 2) if ma200 else None,
        "rsi14": round(rsi14, 1) if rsi14 else None,
        "support": sorted(support),
        "resistance": sorted(resistance),
        "maCrossover": crossover,
    }

    # 实际利率-金价背离度 (Z-Score)
    # 实际利率取负值（因为与金价负相关），然后与金价比较
    # 正值 = 金价相对于实际利率偏高（潜在超买）
    # 负值 = 金价相对于实际利率偏低（潜在超卖）
    divergence_z = zscore_divergence(gold_vals, -ry_vals, window=60)
    derived["divergence"] = {
        "zScore": round(divergence_z, 2) if divergence_z is not None else None,
        "signal": "overbought" if divergence_z is not None and divergence_z > 1.5 else
                  "oversold" if divergence_z is not None and divergence_z < -1.5 else "neutral",
    }

    # COT 持仓
    cot_percentile = 50.0
    if cot_current and cot_history:
        hist_nets = np.array([r["specNetLong"] for r in cot_history])
        cot_percentile = pct_rank(hist_nets, cot_current["specNetLong"])
    derived["cot"] = {
        "current": cot_current,
        "historyCount": len(cot_history),
        "specNetLongPercentile": round(cot_percentile, 1),
    }

    # GLD ETF
    derived["gld"] = gld

    # 央行购金
    derived["centralBankGold"] = cb_gold

    # 4. 信号 & 评分
    print("[4/5] 生成信号与评分...")
    bullish, bearish = compute_signals(raw, derived)
    sentiment = compute_sentiment(derived)
    radar = compute_radar(derived)
    risk_matrix = compute_risk_matrix(derived)
    outlook = compute_outlook(derived)
    overall = compute_overall_signal(bullish, bearish)

    # 5. 组装 JSON
    print("[5/5] 写入 JSON...")
    # 历史数据（用于图表，取最近 1 年日度数据）
    chart_n = 365
    history = {}
    for key in ["gold", "realYield", "dxy", "breakeven", "fedAssets", "vix", "oil", "fwdInflation", "yieldCurve"]:
        history[key] = latest_n(raw[key], chart_n)
    # 白银、GDX 和金银比历史序列
    if raw.get("silver"):
        history["silver"] = latest_n(raw["silver"], chart_n)
    if raw.get("gdx"):
        history["gdx"] = latest_n(raw["gdx"], chart_n)
    if derived.get("silverGoldRatio", {}).get("history"):
        history["silverGoldRatio"] = latest_n(derived["silverGoldRatio"]["history"], chart_n)

    # 计算金价 MA50/MA200 历史序列（用于图表叠加）
    gold_hist = raw["gold"]
    gold_all_vals = values(gold_hist)
    ma50_history = []
    ma200_history = []
    for i in range(len(gold_hist)):
        d = gold_hist[i]["date"]
        if i >= 49:
            ma50_history.append({"date": d, "value": float(np.mean(gold_all_vals[i-49:i+1]))})
        if i >= 199:
            ma200_history.append({"date": d, "value": float(np.mean(gold_all_vals[i-199:i+1]))})
    history["goldMA50"] = latest_n(ma50_history, chart_n)
    history["goldMA200"] = latest_n(ma200_history, chart_n)

    dashboard = {
        "lastUpdated": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "prices": derived["prices"],
        "coreFactors": {
            "realYield": derived["realYield"],
            "dollarIndex": derived["dxy"],
            "inflation": derived["inflation"],
            "fedBalance": derived["fedBalance"],
            "vix": derived["vix"],
            "fedFunds": derived["fedFunds"],
        },
        "signals": {"bullish": bullish, "bearish": bearish},
        "overallSignal": overall,
        "sentiment": sentiment,
        "radar": radar,
        "volatility": derived["volatility"],
        "riskMatrix": risk_matrix,
        "outlook": outlook,
        "technicals": derived["technicals"],
        "divergence": derived.get("divergence"),
        "silverGoldRatio": derived.get("silverGoldRatio"),
        "gdx": derived.get("gdx"),
        "yieldCurve": derived.get("yieldCurve"),
        "history": history,
        "cot": derived.get("cot"),
        "gld": {
            "totalAssetsBln": gld.get("totalAssetsBln") if gld else None,
            "flow5dPct": gld.get("flow5dPct") if gld else None,
            "navPrice": gld.get("navPrice") if gld else None,
            "volume": gld.get("volume") if gld else None,
        },
        "centralBankGold": cb_gold,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(dashboard, f, ensure_ascii=False, indent=2)

    print(f"\n✅ 数据已写入 {OUTPUT_FILE}")
    print(f"   金价: ${gold_cur:.2f}  实际利率: {latest(raw['realYield']):.2f}%  美元: {latest(raw['dxy']):.1f}")
    print(f"   综合信号: {overall['label']}  情绪: {sentiment['label']}({sentiment['score']})")

if __name__ == "__main__":
    main()
