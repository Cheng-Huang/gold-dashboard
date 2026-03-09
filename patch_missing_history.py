#!/usr/bin/env python3
"""
补丁脚本：为 dashboard.json 补充缺失的 history 序列
- silverGoldRatio（金银比）: 通过 yfinance 获取白银价格，与已有金价计算
- yieldCurve（收益率曲线 10Y-2Y）: 通过 FRED API 获取
- 其他缺失序列: oil, fwdInflation, silver, gdx, goldMA50, goldMA200
"""

import json, os, sys, datetime
from pathlib import Path
import numpy as np
import yfinance as yf

DASHBOARD = Path(__file__).parent / "data" / "dashboard.json"

# 自动加载 .env 文件
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
HISTORY_YEARS = 3
TODAY = datetime.date.today()
OBS_START = (TODAY - datetime.timedelta(days=HISTORY_YEARS * 365)).isoformat()
CHART_N = 365


def fetch_fred(series_id: str):
    import requests
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


def fetch_fred_csv(series_id: str):
    """通过 FRED 图表 CSV 端点获取数据（无需 API key）"""
    import requests
    url = "https://fred.stlouisfed.org/graph/fredgraph.csv"
    params = {"id": series_id, "cosd": OBS_START, "coed": TODAY.isoformat()}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    result = []
    for line in r.text.strip().split("\n")[1:]:  # skip header
        parts = line.split(",")
        if len(parts) == 2 and parts[1] not in (".", ""):
            result.append({"date": parts[0], "value": float(parts[1])})
    return result


def fetch_yf(ticker: str):
    df = yf.download(ticker, start=OBS_START, progress=False)
    close_col = ("Close", ticker) if ("Close", ticker) in df.columns else "Close"
    series = []
    for idx, row in df.iterrows():
        val = row[close_col]
        if not np.isnan(val):
            series.append({"date": idx.strftime("%Y-%m-%d"), "value": float(val)})
    return series


def latest_n(series, n):
    return series[-n:] if len(series) >= n else series


def main():
    with open(DASHBOARD, "r", encoding="utf-8") as f:
        data = json.load(f)

    history = data.get("history", {})
    gold_hist = history.get("gold", [])
    patched = []

    # 1. Silver + GoldSilverRatio (yfinance, 无需 API key)
    if not history.get("silverGoldRatio") or not history.get("silver"):
        print("拉取 silver (SI=F via yfinance)...", end=" ")
        try:
            silver = fetch_yf("SI=F")
            print(f"✓ {len(silver)} 条")
            history["silver"] = latest_n(silver, CHART_N)

            # 计算金银比
            silver_map = {d["date"]: d["value"] for d in silver}
            sgr_hist = []
            for d in gold_hist:
                sv = silver_map.get(d["date"])
                if sv and sv > 0:
                    sgr_hist.append({"date": d["date"], "value": round(d["value"] / sv, 2)})
            history["silverGoldRatio"] = latest_n(sgr_hist, CHART_N)
            print(f"  金银比: {len(sgr_hist)} 条")
            patched += ["silver", "silverGoldRatio"]
        except Exception as e:
            print(f"✗ {e}")

    # 2. GDX (yfinance)
    if not history.get("gdx"):
        print("拉取 GDX (yfinance)...", end=" ")
        try:
            gdx = fetch_yf("GDX")
            history["gdx"] = latest_n(gdx, CHART_N)
            print(f"✓ {len(gdx)} 条")
            patched.append("gdx")
        except Exception as e:
            print(f"✗ {e}")

    # 3. Oil (yfinance)
    if not history.get("oil"):
        print("拉取 oil (CL=F via yfinance)...", end=" ")
        try:
            oil = fetch_yf("CL=F")
            history["oil"] = latest_n(oil, CHART_N)
            print(f"✓ {len(oil)} 条")
            patched.append("oil")
        except Exception as e:
            print(f"✗ {e}")

    # 4. MA50 / MA200 (从已有 gold history 计算)
    if not history.get("goldMA50") and gold_hist:
        print("计算 goldMA50/goldMA200...", end=" ")
        # 需要完整的 gold 数据来计算MA，用已有的 history
        gold_vals = np.array([d["value"] for d in gold_hist])
        ma50_hist, ma200_hist = [], []
        for i in range(len(gold_hist)):
            d = gold_hist[i]["date"]
            if i >= 49:
                ma50_hist.append({"date": d, "value": float(np.mean(gold_vals[i - 49:i + 1]))})
            if i >= 199:
                ma200_hist.append({"date": d, "value": float(np.mean(gold_vals[i - 199:i + 1]))})
        history["goldMA50"] = latest_n(ma50_hist, CHART_N)
        history["goldMA200"] = latest_n(ma200_hist, CHART_N)
        print(f"✓ MA50={len(ma50_hist)}条, MA200={len(ma200_hist)}条")
        patched += ["goldMA50", "goldMA200"]

    # 5. FRED 系列 — 优先用 CSV 接口（无需 API key），回退到 JSON API
    fred_needed = {
        "yieldCurve": "T10Y2Y",
        "fwdInflation": "T5YIFR",
    }
    for key, sid in fred_needed.items():
        if not history.get(key):
            print(f"拉取 {key} ({sid} from FRED CSV)...", end=" ")
            try:
                series = fetch_fred_csv(sid)
                if series:
                    history[key] = latest_n(series, CHART_N)
                    print(f"✓ {len(series)} 条")
                    patched.append(key)
                elif FRED_API_KEY:
                    print("CSV 失败, 尝试 JSON API...", end=" ")
                    series = fetch_fred(sid)
                    history[key] = latest_n(series, CHART_N)
                    print(f"✓ {len(series)} 条")
                    patched.append(key)
                else:
                    print("✗ CSV 无数据且 FRED_API_KEY 未设置")
            except Exception as e:
                print(f"✗ {e}")

    if not patched:
        print("\n无需补丁，所有 history 序列已存在。")
        return

    data["history"] = history
    with open(DASHBOARD, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    print(f"\n✅ 已补丁 {len(patched)} 个序列: {', '.join(patched)}")
    print(f"   写入 {DASHBOARD}")


if __name__ == "__main__":
    main()
