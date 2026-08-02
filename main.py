"""
行情雷达 - FastAPI 后端 v2
新增分时数据接口：股票/ETF分钟线、加密货币分钟K线
"""
import time
import logging
from typing import Optional, Dict, Any

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("main")

app = FastAPI(
    title="行情雷达 API v2",
    description="基金/股票/加密货币 实时数据中转服务",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── 缓存 ───────────────────────────────────────────────────
_cache: Dict[str, Dict] = {}  # key → {data, ts}

INTRADAY_TTL = 60       # 分时数据缓存 1 分钟
HISTORY_TTL  = 1800     # 历史数据缓存 30 分钟

def _get_cache(key: str, ttl: int) -> Optional[Any]:
    if key in _cache and (time.time() - _cache[key]["ts"]) < ttl:
        return _cache[key]["data"]
    return None

def _set_cache(key: str, data: Any):
    _cache[key] = {"data": data, "ts": time.time()}

# ─── HTTP 客户端 ─────────────────────────────────────────────
_http = httpx.AsyncClient(timeout=15.0)

# ─── 腾讯分时数据解析 ─────────────────────────────────────────
TENCENT_MINUTE_URL = "https://web.ifzq.gtimg.cn/appstock/app/minute/query"

# 股票/指数代码 → 腾讯API代码映射
TENCENT_CODE_MAP = {
    "sh000001": "sh000001",   # 上证指数
    "sz399001": "sz399001",   # 深证成指
    "sz399006": "sz399006",   # 创业板指
    "HSI":      "hkHSI",      # 恒生指数
    "HSTECH":   "hkHSTECH",  # 恒生科技
    # ETF - 使用 stockHistory 中的代码
    "sh510050": "sh510050",   # 50ETF
    "sh510300": "sh510300",   # 300ETF
    "sh510500": "sh510500",   # 500ETF
    "sz159915": "sz159915",   # 创业板ETF
    "sh588000": "sh588000",   # 科创50ETF
    "sh513100": "sh513100",   # 纳指ETF
    "sh513520": "sh513520",   # 日经ETF
    "sh513180": "sh513180",   # 恒生科技ETF
}


@app.get("/api/health")
async def health():
    """健康检查"""
    return {"status": "ok", "version": "2.0.0"}


@app.get("/api/intraday/stock")
async def stock_intraday(code: str = Query(..., description="指数/ETF代码，如 sh000001")):
    """
    获取股票/指数/ETF 分时数据
    数据来源：腾讯财经 minute API
    返回：分钟级价格+成交量序列
    """
    cache_key = f"intraday_stock_{code}"
    cached = _get_cache(cache_key, INTRADAY_TTL)
    if cached is not None:
        return {"code": 0, "data": cached, "msg": "ok"}

    # 转换代码格式
    tencent_code = TENCENT_CODE_MAP.get(code, code)

    try:
        resp = await _http.get(TENCENT_MINUTE_URL, params={"code": tencent_code})
        resp.raise_for_status()
        raw = resp.json()
    except Exception as e:
        logger.error(f"腾讯分时请求失败 code={code}: {e}")
        raise HTTPException(status_code=502, detail=f"上游数据获取失败: {e}")

    if raw.get("code") != 0:
        raise HTTPException(status_code=502, detail="上游返回错误")

    # 解析数据
    try:
        stock_data = raw["data"][tencent_code]["data"]
        minute_list = stock_data.get("data", [])
        pre_close = stock_data.get("qt", {}).get(tencent_code, [None, None, None, None])

        points = []
        for item in minute_list:
            parts = item.split(" ")
            if len(parts) >= 3:
                t = parts[0]       # "0930"
                price = float(parts[1])
                vol = int(parts[2])
                # 格式化时间为 HH:MM
                t_fmt = t[:2] + ":" + t[2:] if len(t) == 4 else t
                points.append({
                    "time": t_fmt,
                    "price": price,
                    "volume": vol
                })

        # 昨收价
        prev_close = None
        if pre_close and len(pre_close) > 3:
            try:
                prev_close = float(pre_close[3])
            except (ValueError, TypeError):
                pass

        result = {
            "code": code,
            "points": points,
            "prevClose": prev_close,
            "count": len(points)
        }

        _set_cache(cache_key, result)
        return {"code": 0, "data": result, "msg": "ok"}

    except (KeyError, ValueError, IndexError) as e:
        logger.error(f"腾讯分时解析失败 code={code}: {e}")
        raise HTTPException(status_code=502, detail=f"数据解析失败: {e}")


# ─── Gate.io 加密货币分时K线 ──────────────────────────────────
GATE_KLINES_URL = "https://api.gateio.ws/api/v4/spot/candlesticks"

# 币种代码 → Gate.io 交易对
CRYPTO_SYMBOL_MAP = {
    "BTC": "BTC_USDT", "ETH": "ETH_USDT", "BNB": "BNB_USDT",
    "XRP": "XRP_USDT", "ADA": "ADA_USDT", "DOGE": "DOGE_USDT",
    "SOL": "SOL_USDT", "DOT": "DOT_USDT", "MATIC": "MATIC_USDT",
    "AVAX": "AVAX_USDT", "SHIB": "SHIB_USDT", "LTC": "LTC_USDT",
    "TRX": "TRX_USDT", "LINK": "LINK_USDT", "ATOM": "ATOM_USDT",
    "UNI": "UNI_USDT", "XLM": "XLM_USDT", "NEAR": "NEAR_USDT",
    "ALGO": "ALGO_USDT", "BCH": "BCH_USDT", "FIL": "FIL_USDT",
    "VET": "VET_USDT", "ICP": "ICP_USDT", "HBAR": "HBAR_USDT",
    "SAND": "SAND_USDT", "MANA": "MANA_USDT", "AXS": "AXS_USDT",
    "THETA": "THETA_USDT", "FTM": "FTM_USDT", "TON": "TON_USDT",
    "PEPE": "PEPE_USDT", "APT": "APT_USDT", "OP": "OP_USDT",
    "ARB": "ARB_USDT", "IMX": "IMX_USDT", "RUNE": "RUNE_USDT",
    "INJ": "INJ_USDT", "SUI": "SUI_USDT", "SEI": "SEI_USDT",
    "TIA": "TIA_USDT", "JUP": "JUP_USDT", "WIF": "WIF_USDT",
    "ENA": "ENA_USDT", "PENDLE": "PENDLE_USDT", "STX": "STX_USDT",
    "RENDER": "RENDER_USDT", "FET": "FET_USDT", "AGIX": "AGIX_USDT",
    "WLD": "WLD_USDT", "PEOPLE": "PEOPLE_USDT", "AAVE": "AAVE_USDT",
}


@app.get("/api/intraday/crypto")
async def crypto_intraday(symbol: str = Query(..., description="币种代码，如 BTC")):
    """
    获取加密货币日内分时数据
    数据来源：Gate.io 5分钟K线
    返回：5分钟级 OHLCV 序列（最近24小时）
    """
    cache_key = f"intraday_crypto_{symbol}"
    cached = _get_cache(cache_key, INTRADAY_TTL)
    if cached is not None:
        return {"code": 0, "data": cached, "msg": "ok"}

    pair = CRYPTO_SYMBOL_MAP.get(symbol.upper(), f"{symbol.upper()}_USDT")

    try:
        resp = await _http.get(GATE_KLINES_URL, params={
            "currency_pair": pair,
            "interval": "5m",
            "limit": 288  # 288 * 5min = 24h
        })
        resp.raise_for_status()
        raw = resp.json()
    except Exception as e:
        logger.error(f"Gate.io 分时请求失败 symbol={symbol}: {e}")
        raise HTTPException(status_code=502, detail=f"上游数据获取失败: {e}")

    if not raw or not isinstance(raw, list):
        raise HTTPException(status_code=502, detail="上游返回空数据")

    # 解析 K 线数据
    # Gate.io 格式: [timestamp, volume, high, low, open, close, amount, is_closed]
    points = []
    for kline in raw:
        if len(kline) < 6:
            continue
        try:
            ts = int(kline[0])
            # 转换为北京时间
            from datetime import datetime, timezone, timedelta
            utc_time = datetime.fromtimestamp(ts, tz=timezone.utc)
            bj_time = utc_time + timedelta(hours=8)
            t_fmt = bj_time.strftime("%H:%M")

            points.append({
                "time": t_fmt,
                "open": float(kline[4]),
                "high": float(kline[2]),
                "low": float(kline[3]),
                "close": float(kline[5]),
                "volume": float(kline[1]),
                "timestamp": ts
            })
        except (ValueError, TypeError) as e:
            logger.warning(f"Gate.io K线解析跳过: {e}")
            continue

    if not points:
        raise HTTPException(status_code=504, detail="无有效分时数据")

    # 前收盘价 = 第一根K线的开盘价
    prev_close = points[0]["open"]

    # 只返回最近 24h 的 close 序列（给分时线用）
    close_prices = [p["close"] for p in points]

    result = {
        "symbol": symbol.upper(),
        "pair": pair,
        "points": points,
        "prevClose": prev_close,
        "count": len(points)
    }

    _set_cache(cache_key, result)
    return {"code": 0, "data": result, "msg": "ok"}


# ─── 基金历史净值（兼容旧接口）──────────────────────────────
@app.get("/api/fund/history")
async def fund_history(code: str = Query(..., description="基金代码，如 005967")):
    """
    获取基金历史净值
    数据来源：东方财富 lsjz API
    """
    if not code or len(code) != 6:
        raise HTTPException(status_code=400, detail="基金代码格式错误，需6位数字")

    cache_key = f"fund_history_{code}"
    cached = _get_cache(cache_key, HISTORY_TTL)
    if cached is not None:
        return {"code": 0, "data": cached, "msg": "ok"}

    url = "https://api.fund.eastmoney.com/f10/lsjz"
    try:
        resp = await _http.get(url, params={
            "fundCode": code,
            "pageIndex": 1,
            "pageSize": 365
        }, headers={
            "Referer": "https://fund.eastmoney.com"
        })
        resp.raise_for_status()
        raw = resp.json()
    except Exception as e:
        logger.error(f"东方财富基金历史请求失败 code={code}: {e}")
        raise HTTPException(status_code=502, detail=f"上游数据获取失败: {e}")

    try:
        lst = raw.get("Data", {}).get("LSJZList", [])
        history = []
        for item in reversed(lst):  # 按日期升序
            nav = item.get("DWJZ")
            date = item.get("FSRQ")
            if nav and date:
                history.append({"date": date, "nav": float(nav)})

        result = {
            "code": code,
            "history": history,
            "count": len(history),
            "dateRange": f"{history[0]['date']}~{history[-1]['date']}" if history else ""
        }

        _set_cache(cache_key, result)
        return {"code": 0, "data": result, "msg": "ok"}

    except (KeyError, ValueError) as e:
        logger.error(f"基金历史解析失败 code={code}: {e}")
        raise HTTPException(status_code=502, detail=f"数据解析失败: {e}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
