"""
行情雷达 - FastAPI 后端 v2.3.0
v2.0.0: 分时数据接口
v2.1.0: 用户反馈接口、基金实时估值批量接口
v2.2.0: 基金档案/股票详细行情/股票K线接口
v2.3.0: 反馈持久化+启动加载、IP限流、CORS收紧、管理密钥加强、
         httpx生命周期管理、Sentry监控、上游解析加固、推送基础设施完善、metrics端点
"""
import os
import re
import json
import time
import asyncio
import logging
import secrets
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Query, Body, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# ─── Sentry 监控（可选，通过环境变量 SENTRY_DSN 启用）────────
SENTRY_DSN = os.environ.get("SENTRY_DSN", "")
if SENTRY_DSN:
    try:
        import sentry_sdk
        sentry_sdk.init(dsn=SENTRY_DSN, traces_sample_rate=0.1, environment="production")
        logging.getLogger("main").info("Sentry 监控已启用")
    except ImportError:
        logging.getLogger("main").warning("sentry-sdk 未安装，跳过 Sentry 监控（pip install sentry-sdk 启用）")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("main")

# ─── 配置 ───────────────────────────────────────────────────
ADMIN_KEY = os.environ.get("ADMIN_KEY", "")
if not ADMIN_KEY:
    ADMIN_KEY = secrets.token_urlsafe(16)
    logger.warning("⚠️ ADMIN_KEY 未设置环境变量，已生成随机密钥: " + ADMIN_KEY)
    logger.warning("⚠️ 请在 Render 后台设置 ADMIN_KEY 环境变量以固定密钥")

ALLOWED_ORIGINS = os.environ.get(
    "ALLOWED_ORIGINS",
    "https://market-radar-api-bgvu.onrender.com,http://localhost:3000"
).split(",")

# ─── 缓存 ───────────────────────────────────────────────────
_cache: Dict[str, Dict] = {}

INTRADAY_TTL = 60
HISTORY_TTL  = 1800
DETAIL_TTL = 21600

def _get_cache(key: str, ttl: int) -> Optional[Any]:
    if key in _cache and (time.time() - _cache[key]["ts"]) < ttl:
        return _cache[key]["data"]
    return None

def _set_cache(key: str, data: Any):
    _cache[key] = {"data": data, "ts": time.time()}

# ─── HTTP 客户端（生命周期管理）─────────────────────────────
_http: httpx.AsyncClient = None  # type: ignore

# ─── 限流 ───────────────────────────────────────────────────
_rate_limits: Dict[str, list] = {}
RATE_WINDOW = 60
RATE_MAX = 80

def _cleanup_rate_limits():
    now = time.time()
    stale = [ip for ip, ts_list in _rate_limits.items() if not ts_list or now - ts_list[-1] > RATE_WINDOW * 2]
    for ip in stale:
        del _rate_limits[ip]

# ─── 反馈持久化 ─────────────────────────────────────────────
FEEDBACK_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "feedbacks.jsonl")
_feedbacks: List[Dict[str, Any]] = []

def _load_feedbacks():
    global _feedbacks
    try:
        if os.path.exists(FEEDBACK_FILE):
            with open(FEEDBACK_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            _feedbacks.append(json.loads(line))
                        except json.JSONDecodeError:
                            continue
            logger.info(f"从文件加载了 {len(_feedbacks)} 条历史反馈")
    except Exception as e:
        logger.warning(f"加载历史反馈失败: {e}")

# ─── 上游解析工具 ───────────────────────────────────────────
def _log_upstream_error(source: str, code: str, reason: str, raw_snippet: str = ""):
    """统一记录上游解析错误，方便排查格式变更"""
    msg = f"上游解析异常 source={source} code={code} reason={reason}"
    if raw_snippet:
        msg += f" raw={raw_snippet[:200]}"
    logger.error(msg)

# ─── 应用生命周期 ───────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global _http
    _http = httpx.AsyncClient(timeout=15.0)
    _load_feedbacks()
    logger.info("行情雷达 API v2.3.0 启动完成")
    yield
    await _http.aclose()
    logger.info("行情雷达 API 已关闭")

app = FastAPI(
    title="行情雷达 API v2",
    description="基金/股票/加密货币 实时数据中转服务",
    version="2.3.0",
    lifespan=lifespan,
)

# ─── 中间件 ─────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in ALLOWED_ORIGINS],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    """IP 限流：每 IP 每分钟最多 80 次请求"""
    # Render 等代理架构下 request.client.host 是代理 IP，需从 X-Forwarded-For 取真实用户 IP
    xff = request.headers.get("x-forwarded-for")
    if xff:
        client_ip = xff.split(",")[0].strip()
    else:
        client_ip = request.client.host if request.client else "unknown"
    now = time.time()

    ts_list = _rate_limits.get(client_ip, [])
    ts_list = [t for t in ts_list if now - t < RATE_WINDOW]

    if len(ts_list) >= RATE_MAX:
        return JSONResponse(
            status_code=429,
            content={"code": -1, "msg": "请求过于频繁，请稍后再试"}
        )

    ts_list.append(now)
    _rate_limits[client_ip] = ts_list

    if len(_rate_limits) > 500:
        _cleanup_rate_limits()

    return await call_next(request)


# ─── 健康检查 + 监控 ────────────────────────────────────────
@app.get("/api/health")
async def health():
    return {"status": "ok", "version": "2.3.0"}

@app.get("/api/metrics")
async def metrics(key: str = Query(..., description="管理密钥")):
    """基本监控信息：缓存大小、反馈数、订阅数、限流IP数"""
    if key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="bad key")
    return {
        "code": 0,
        "data": {
            "version": "2.3.0",
            "cache_size": len(_cache),
            "feedback_count": len(_feedbacks),
            "wx_subscribers": sum(len(v) for v in _wx_subs.values()),
            "rate_limited_ips": len(_rate_limits),
            "http_client_closed": _http is None or _http.is_closed,
        },
        "msg": "ok",
    }


# ─── 腾讯分时数据解析 ─────────────────────────────────────────
TENCENT_MINUTE_URL = "https://web.ifzq.gtimg.cn/appstock/app/minute/query"

TENCENT_CODE_MAP = {
    "sh000001": "sh000001", "sz399001": "sz399001", "sz399006": "sz399006",
    "HSI": "hkHSI", "HSTECH": "hkHSTECH",
    "sh510050": "sh510050", "sh510300": "sh510300", "sh510500": "sh510500",
    "sz159915": "sz159915", "sh588000": "sh588000", "sh513100": "sh513100",
    "sh513520": "sh513520", "sh513180": "sh513180",
}


@app.get("/api/intraday/stock")
async def stock_intraday(code: str = Query(..., description="指数/ETF代码，如 sh000001")):
    cache_key = f"intraday_stock_{code}"
    cached = _get_cache(cache_key, INTRADAY_TTL)
    if cached is not None:
        return {"code": 0, "data": cached, "msg": "ok"}

    tencent_code = TENCENT_CODE_MAP.get(code, code)

    try:
        resp = await _http.get(TENCENT_MINUTE_URL, params={"code": tencent_code})
        resp.raise_for_status()
        raw = resp.json()
    except Exception as e:
        logger.error(f"腾讯分时请求失败 code={code}: {e}")
        raise HTTPException(status_code=502, detail=f"上游数据获取失败: {e}")

    if raw.get("code") != 0:
        _log_upstream_error("tencent_minute", code, f"返回码非0: {raw.get('code')}")
        raise HTTPException(status_code=502, detail="上游返回错误")

    try:
        stock_data = raw["data"][tencent_code]["data"]
        minute_list = stock_data.get("data", [])
        pre_close = stock_data.get("qt", {}).get(tencent_code, [None, None, None, None])

        points = []
        for item in minute_list:
            parts = item.split(" ")
            if len(parts) >= 3:
                t = parts[0]
                price = float(parts[1])
                vol = int(parts[2])
                t_fmt = t[:2] + ":" + t[2:] if len(t) == 4 else t
                points.append({"time": t_fmt, "price": price, "volume": vol})

        prev_close = None
        if pre_close and len(pre_close) > 3:
            try:
                prev_close = float(pre_close[3])
            except (ValueError, TypeError):
                pass

        result = {"code": code, "points": points, "prevClose": prev_close, "count": len(points)}
        _set_cache(cache_key, result)
        return {"code": 0, "data": result, "msg": "ok"}

    except (KeyError, ValueError, IndexError) as e:
        _log_upstream_error("tencent_minute", code, str(e), str(raw)[:200])
        raise HTTPException(status_code=502, detail=f"数据解析失败: {e}")


# ─── Gate.io 加密货币分时K线 ──────────────────────────────────
GATE_KLINES_URL = "https://api.gateio.ws/api/v4/spot/candlesticks"

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
    cache_key = f"intraday_crypto_{symbol}"
    cached = _get_cache(cache_key, INTRADAY_TTL)
    if cached is not None:
        return {"code": 0, "data": cached, "msg": "ok"}

    pair = CRYPTO_SYMBOL_MAP.get(symbol.upper(), f"{symbol.upper()}_USDT")

    try:
        resp = await _http.get(GATE_KLINES_URL, params={
            "currency_pair": pair, "interval": "5m", "limit": 288
        })
        resp.raise_for_status()
        raw = resp.json()
    except Exception as e:
        logger.error(f"Gate.io 分时请求失败 symbol={symbol}: {e}")
        raise HTTPException(status_code=502, detail=f"上游数据获取失败: {e}")

    if not raw or not isinstance(raw, list):
        raise HTTPException(status_code=502, detail="上游返回空数据")

    points = []
    for kline in raw:
        if len(kline) < 6:
            continue
        try:
            ts = int(kline[0])
            utc_time = datetime.fromtimestamp(ts, tz=timezone.utc)
            bj_time = utc_time + timedelta(hours=8)
            t_fmt = bj_time.strftime("%H:%M")
            points.append({
                "time": t_fmt, "open": float(kline[4]), "high": float(kline[2]),
                "low": float(kline[3]), "close": float(kline[5]),
                "volume": float(kline[1]), "timestamp": ts
            })
        except (ValueError, TypeError) as e:
            logger.warning(f"Gate.io K线解析跳过: {e}")
            continue

    if not points:
        raise HTTPException(status_code=504, detail="无有效分时数据")

    prev_close = points[0]["open"]
    result = {
        "symbol": symbol.upper(), "pair": pair,
        "points": points, "prevClose": prev_close, "count": len(points)
    }
    _set_cache(cache_key, result)
    return {"code": 0, "data": result, "msg": "ok"}


# ─── 基金历史净值（双数据源）──────────────────────────────
SINA_FUND_URL = "https://stock.finance.sina.com.cn/fundInfo/api/openapi.php/CaihuiFundInfoService.getNav"
EM_FUND_URL = "https://api.fund.eastmoney.com/f10/lsjz"


async def _fetch_sina_fund_history(code: str, days: int = 370) -> list:
    date_to = datetime.now().strftime("%Y-%m-%d")
    date_from = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    async def fetch_page(page: int) -> dict:
        resp = await _http.get(SINA_FUND_URL, params={
            "symbol": code, "datefrom": date_from, "dateto": date_to, "page": page
        })
        resp.raise_for_status()
        raw = resp.json()
        return raw.get("result", {}).get("data", {}) or {}

    first = await fetch_page(1)
    data_list = list(first.get("data") or [])
    total = int(first.get("total_num") or 0)
    total_pages = min((total + 20) // 21, 15)

    if total_pages > 1:
        pages = await asyncio.gather(
            *[fetch_page(p) for p in range(2, total_pages + 1)], return_exceptions=True
        )
        for r in pages:
            if isinstance(r, dict):
                data_list.extend(r.get("data") or [])

    history = []
    for item in data_list:
        nav = item.get("jjjz")
        date = (item.get("fbrq") or "")[:10]
        if nav and date:
            try:
                history.append({"date": date, "nav": float(nav)})
            except (ValueError, TypeError):
                continue

    history.sort(key=lambda x: x["date"])
    seen = set()
    dedup = []
    for h in history:
        if h["date"] not in seen:
            seen.add(h["date"])
            dedup.append(h)
    return dedup


async def _fetch_em_fund_history(code: str) -> list:
    resp = await _http.get(EM_FUND_URL, params={
        "fundCode": code, "pageIndex": 1, "pageSize": 365
    }, headers={"Referer": "https://fund.eastmoney.com"})
    resp.raise_for_status()
    raw = resp.json()
    lst = (raw or {}).get("Data", {}).get("LSJZList", []) or []
    history = []
    for item in reversed(lst):
        nav = item.get("DWJZ")
        date = item.get("FSRQ")
        if nav and date:
            try:
                history.append({"date": date, "nav": float(nav)})
            except (ValueError, TypeError):
                continue
    return history


@app.get("/api/fund/history")
async def fund_history(code: str = Query(..., description="基金代码，如 005967")):
    if not code or len(code) != 6 or not code.isdigit():
        raise HTTPException(status_code=400, detail="基金代码格式错误，需6位数字")

    cache_key = f"fund_history_{code}"
    cached = _get_cache(cache_key, HISTORY_TTL)
    if cached is not None:
        return {"code": 0, "data": cached, "msg": "ok"}

    history = []
    errors = []

    try:
        history = await _fetch_sina_fund_history(code)
        if history:
            logger.info(f"新浪源成功 code={code} count={len(history)}")
    except Exception as e:
        logger.error(f"新浪基金历史失败 code={code}: {e}")
        errors.append(f"sina: {e}")

    if not history:
        try:
            history = await _fetch_em_fund_history(code)
            if history:
                logger.info(f"东方财富源成功 code={code} count={len(history)}")
        except Exception as e:
            logger.error(f"东方财富基金历史失败 code={code}: {e}")
            errors.append(f"em: {e}")

    if not history:
        raise HTTPException(
            status_code=404,
            detail=f"暂无该基金历史数据 ({'; '.join(errors) if errors else 'empty'})"
        )

    result = {
        "code": code, "history": history, "count": len(history),
        "dateRange": f"{history[0]['date']}~{history[-1]['date']}"
    }
    _set_cache(cache_key, result)
    return {"code": 0, "data": result, "msg": "ok"}


# ═══ 微信订阅消息（涨跌提醒）══════════════════════════════════
WX_APPID = "wx7f5552fc52317b2a"
WX_SECRET = os.environ.get("WX_SECRET", "")
WX_TEMPLATE_ID = "PUqStjuTo2xby_vdpIZas_VpvZUnUHnmwQtuMwr34wA"
WX_NOTIFY_KEY = os.environ.get("WX_NOTIFY_KEY", "")

WX_FIELD_NAME = "thing1"
WX_FIELD_CHANGE = "thing2"
WX_FIELD_TIME = "time3"
WX_FIELD_PRICE = "number4"

_wx_subs: Dict[str, list] = {}
_wx_token_cache: Dict[str, Any] = {"token": None, "ts": 0}


async def _wx_access_token() -> str:
    if _wx_token_cache["token"] and time.time() - _wx_token_cache["ts"] < 7000:
        return _wx_token_cache["token"]
    r = await _http.get("https://api.weixin.qq.com/cgi-bin/token", params={
        "grant_type": "client_credential", "appid": WX_APPID, "secret": WX_SECRET
    })
    d = r.json()
    if "access_token" not in d:
        raise HTTPException(500, f"wx token error: {d.get('errmsg')}")
    _wx_token_cache["token"] = d["access_token"]
    _wx_token_cache["ts"] = time.time()
    return d["access_token"]


@app.post("/api/wx/subscribe")
async def wx_subscribe(body: dict = Body(...)):
    js_code = body.get("code", "")
    fund_code = body.get("fund_code", "")
    fund_name = (body.get("fund_name", "") or fund_code)[:20]
    if not (js_code and fund_code):
        raise HTTPException(400, "missing code or fund_code")
    if not WX_SECRET:
        raise HTTPException(500, "WX_SECRET not configured")

    r = await _http.get("https://api.weixin.qq.com/sns/jscode2session", params={
        "appid": WX_APPID, "secret": WX_SECRET,
        "js_code": js_code, "grant_type": "authorization_code"
    })
    d = r.json()
    openid = d.get("openid")
    if not openid:
        raise HTTPException(400, f"wx login failed: {d.get('errmsg', 'unknown')}")

    subs = _wx_subs.setdefault(fund_code, [])
    if not any(s["openid"] == openid for s in subs):
        subs.append({"openid": openid, "name": fund_name, "ts": int(time.time())})
    logger.info(f"wx subscribe: fund={fund_code} openid={openid[:10]}... total={len(subs)}")
    return {"code": 0, "data": {"ok": True, "subscribers": len(subs)}, "msg": "ok"}


@app.get("/api/jobs/daily_notify")
async def daily_notify(key: str = Query(...)):
    if not WX_NOTIFY_KEY or key != WX_NOTIFY_KEY:
        raise HTTPException(403, "bad key")
    if not WX_SECRET:
        raise HTTPException(500, "WX_SECRET not configured")

    sent, failed = 0, 0
    errors = []
    targets = [(fc, list(subs)) for fc, subs in _wx_subs.items() if subs]
    if not targets:
        return {"code": 0, "data": {"sent": 0, "failed": 0, "msg": "no subscribers"}, "msg": "ok"}

    try:
        token = await _wx_access_token()
    except HTTPException as e:
        logger.error(f"daily_notify: 获取 access_token 失败: {e.detail}")
        return {"code": -1, "data": {"sent": 0, "failed": 0, "errors": [str(e.detail)]}, "msg": "token error"}

    for fund_code, subs in targets:
        try:
            navs = await _fetch_sina_fund_history(fund_code, days=10)
            if len(navs) < 2:
                raise ValueError("nav data too short")
            latest, prev = navs[-1], navs[-2]
            chg = (latest["nav"] - prev["nav"]) / prev["nav"] * 100
            name = subs[0]["name"][:10]
            content = f"{name} {'涨' if chg >= 0 else '跌'}{abs(chg):.2f}%"[:20]
            date_str = latest["date"]
        except Exception as e:
            errors.append(f"{fund_code} nav: {e}")
            failed += len(subs)
            continue

        for s in subs:
            try:
                r = await _http.post(
                    f"https://api.weixin.qq.com/cgi-bin/message/subscribe/send?access_token={token}",
                    json={
                        "touser": s["openid"],
                        "template_id": WX_TEMPLATE_ID,
                        "page": f"pages/detail/detail?code={fund_code}",
                        "data": {
                            WX_FIELD_NAME: {"value": name},
                            WX_FIELD_CHANGE: {"value": ("涨" if chg >= 0 else "跌") + f"{abs(chg):.2f}%"},
                            WX_FIELD_TIME: {"value": date_str},
                            WX_FIELD_PRICE: {"value": f"{latest['nav']:.4f}"},
                        },
                    }
                )
                resp = r.json()
                if resp.get("errcode") == 0:
                    sent += 1
                else:
                    failed += 1
                    errors.append(f"{fund_code}/{s['openid'][:8]}: errcode={resp.get('errcode')} {resp.get('errmsg')}")
            except Exception as e:
                failed += 1
                errors.append(f"{fund_code} send: {e}")
        _wx_subs[fund_code] = []

    logger.info(f"daily_notify done: sent={sent} failed={failed}")
    return {"code": 0, "data": {"sent": sent, "failed": failed, "errors": errors[:10]}, "msg": "ok"}


# ═══ 用户反馈通道 ═════════════════════════════════════════════
@app.post("/api/feedback")
async def submit_feedback(body: dict = Body(...)):
    text = (body.get("text") or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="反馈内容不能为空")

    entry = {
        "text": text[:2000],
        "version": (body.get("version") or "")[:50],
        "contact": (body.get("contact") or "")[:200],
        "ts": int(time.time()),
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    _feedbacks.append(entry)

    try:
        with open(FEEDBACK_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.warning(f"feedback 文件写入失败（忽略）: {e}")

    logger.info(f"feedback received: len={len(text)} version={entry['version']} total={len(_feedbacks)}")
    return {"code": 0, "data": {"ok": True}, "msg": "ok"}


@app.get("/api/feedback/list")
async def feedback_list(key: str = Query(..., description="管理密钥")):
    if key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="bad key")
    return {
        "code": 0,
        "data": {
            "count": len(_feedbacks),
            "items": _feedbacks[-200:],
            "note": "反馈已持久化到 feedbacks.jsonl，重启后自动加载",
        },
        "msg": "ok",
    }


@app.get("/api/feedback/export")
async def feedback_export(key: str = Query(..., description="管理密钥")):
    """导出全部反馈为 JSON 文件下载"""
    if key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="bad key")
    from fastapi.responses import Response
    content = json.dumps(_feedbacks, ensure_ascii=False, indent=2)
    return Response(
        content=content,
        media_type="application/json",
        headers={"Content-Disposition": "attachment; filename=feedbacks.json"}
    )


# ═══ 基金实时净值 ════════════════════════════════════════════
FUND_GZ_URL = "https://fundgz.1234567.com.cn/js/{code}.js"
FUND_GZ_HEADERS = {"Referer": "https://fund.eastmoney.com/"}
_JSONPGZ_RE = re.compile(r"^jsonpgz\((.*)\);?\s*$", re.S)


async def _fetch_em_latest_nav(code: str) -> Optional[Dict[str, str]]:
    try:
        resp = await _http.get(EM_FUND_URL, params={
            "fundCode": code, "pageIndex": 1, "pageSize": 1
        }, headers={"Referer": "https://fund.eastmoney.com"})
        resp.raise_for_status()
        raw = resp.json()
        lst = (raw or {}).get("Data", {}).get("LSJZList", []) or []
        if not lst:
            return None
        item = lst[0]
        return {
            "nav": item.get("DWJZ") or "",
            "navDate": item.get("FSRQ") or "",
            "change": item.get("JZZZL") or "",
            "accNav": item.get("LJJZ") or "",
        }
    except Exception as e:
        logger.warning(f"东方财富最新净值兜底失败 code={code}: {e}")
        return None


async def _fetch_fund_realtime_one(code: str) -> Dict[str, Any]:
    fallback = await _fetch_em_latest_nav(code)
    if fallback and fallback.get("nav"):
        return {
            "code": code, "name": "", "nav": fallback["nav"],
            "navDate": fallback["navDate"], "change": fallback.get("change", ""),
            "accNav": fallback.get("accNav", ""), "estimate": "",
            "estimateChange": "", "estimateTime": "",
        }
    url = FUND_GZ_URL.format(code=code)
    try:
        resp = await _http.get(url, params={"rt": int(time.time() * 1000)}, headers=FUND_GZ_HEADERS)
        resp.raise_for_status()
        raw_text = resp.text.strip()
        m = _JSONPGZ_RE.match(raw_text)
        if not m:
            raise ValueError("非 jsonpgz 格式响应")
        payload = json.loads(m.group(1))
        return {
            "code": payload.get("fundcode", code), "name": payload.get("name", ""),
            "nav": payload.get("dwjz", ""), "navDate": payload.get("jzrq", ""),
            "estimate": payload.get("gsz", ""), "estimateChange": payload.get("gszzl", ""),
            "estimateTime": payload.get("gztime", ""),
        }
    except Exception as e:
        logger.warning(f"基金最新净值获取失败 code={code}: {e}")
        return {"code": code, "error": str(e)}


@app.get("/api/fund/realtime")
async def fund_realtime(codes: str = Query(..., description="基金代码，逗号分隔，最多50只")):
    code_list: List[str] = []
    seen = set()
    for c in codes.split(","):
        c = c.strip()
        if c.isdigit() and len(c) == 6 and c not in seen:
            seen.add(c)
            code_list.append(c)
    code_list = code_list[:50]

    if not code_list:
        return {"code": 0, "data": {"items": []}, "msg": "ok"}

    items = await asyncio.gather(*[_fetch_fund_realtime_one(c) for c in code_list])
    return {"code": 0, "data": {"items": list(items)}, "msg": "ok"}


# ═══ 详情页数据接口 ══════════════════════════════════════════
EM_HEADERS = {"Referer": "https://fund.eastmoney.com", "User-Agent": "Mozilla/5.0"}
EM_PZD_URL = "https://fund.eastmoney.com/pingzhongdata/{code}.js"
EM_JBGK_URL = "https://fundf10.eastmoney.com/jbgk_{code}.html"
TENCENT_QUOTE_URL = "https://qt.gtimg.cn/q={code}"
TENCENT_KLINE_URL = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"


def _js_var_str(text: str, name: str) -> str:
    m = re.search(r"var\s+" + re.escape(name) + r"\s*=\s*\"([^\"]*)\"", text)
    return m.group(1) if m else ""


def _js_var_json(text: str, name: str):
    m = re.search(r"var\s+" + re.escape(name) + r"\s*=\s*(\[.*?\]|\{.*?\})\s*;", text, re.S)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except Exception:
        return None


def _jbgk_field(html: str, label: str) -> str:
    m = re.search(label + r"</th><td[^>]*>(.*?)</td>", html, re.S)
    if not m:
        return ""
    txt = re.sub(r"<[^>]+>", "", m.group(1))
    return re.sub(r"\s+", " ", txt).strip()


@app.get("/api/fund/detail")
async def fund_detail(code: str = Query(..., description="基金代码，6位数字")):
    if not (code.isdigit() and len(code) == 6):
        raise HTTPException(400, "基金代码格式错误")
    cache_key = f"fund_detail_{code}"
    cached = _get_cache(cache_key, DETAIL_TTL)
    if cached is not None:
        return {"code": 0, "data": cached, "msg": "ok"}

    pzd_req = _http.get(EM_PZD_URL.format(code=code), headers=EM_HEADERS,
                        params={"rt": int(time.time() * 1000)})
    jbgk_req = _http.get(EM_JBGK_URL.format(code=code), headers=EM_HEADERS)
    pzd_resp, jbgk_resp = await asyncio.gather(pzd_req, jbgk_req, return_exceptions=True)

    info: Dict[str, Any] = {"code": code}

    if isinstance(pzd_resp, Exception):
        _log_upstream_error("em_pingzhongdata", code, f"请求异常: {pzd_resp}")
    elif pzd_resp.status_code == 200:
        t = pzd_resp.text
        # 格式校验：检查关键变量是否存在
        if "fS_name" not in t:
            _log_upstream_error("em_pingzhongdata", code, "响应中未找到 fS_name，页面格式可能已变更", t[:300])
        info["name"] = _js_var_str(t, "fS_name")
        info["m1"] = _js_var_str(t, "syl_1y")
        info["m3"] = _js_var_str(t, "syl_3y")
        info["m6"] = _js_var_str(t, "syl_6y")
        info["y1"] = _js_var_str(t, "syl_1n")
        info["feeOriginal"] = _js_var_str(t, "fund_sourceRate")
        info["feeDiscount"] = _js_var_str(t, "fund_Rate")
        info["minBuy"] = _js_var_str(t, "fund_minsg")
        mgr = _js_var_json(t, "Data_currentFundManager")
        if mgr and isinstance(mgr, list) and mgr:
            m0 = mgr[0]
            info["manager"] = m0.get("name", "")
            info["managerWorkTime"] = m0.get("workTime", "")
            info["managerFundSize"] = m0.get("fundSize", "")
        scale = _js_var_json(t, "Data_fluctuationScale")
        if scale and isinstance(scale, dict):
            series = scale.get("series") or []
            if series and series[-1].get("y") is not None:
                info["scaleYi"] = series[-1]["y"]
    else:
        _log_upstream_error("em_pingzhongdata", code, f"HTTP {pzd_resp.status_code}")

    if isinstance(jbgk_resp, Exception):
        _log_upstream_error("em_jbgk", code, f"请求异常: {jbgk_resp}")
    elif jbgk_resp.status_code == 200:
        html = jbgk_resp.text
        info["type"] = _jbgk_field(html, "基金类型")
        est = _jbgk_field(html, "成立日期/规模")
        info["establish"] = est.split("/")[0].strip() if est else ""
        info["assetScale"] = _jbgk_field(html, "资产规模")
        info["company"] = _jbgk_field(html, "基金管理人")
        if not info.get("manager"):
            info["manager"] = _jbgk_field(html, "基金经理人")

    if not info.get("name"):
        raise HTTPException(404, "未获取到该基金档案（上游数据源可能变更）")

    _set_cache(cache_key, info)
    return {"code": 0, "data": info, "msg": "ok"}


def _qf(fields: list, i: int) -> str:
    return fields[i].strip() if i < len(fields) else ""


@app.get("/api/stock/quote")
async def stock_quote(code: str = Query(..., description="腾讯代码，如 sh000001 / sh600519 / hkHSI")):
    if not re.fullmatch(r"[A-Za-z0-9]{2,12}", code or ""):
        raise HTTPException(400, "代码格式错误")
    cache_key = f"stock_quote_{code}"
    cached = _get_cache(cache_key, INTRADAY_TTL)
    if cached is not None:
        return {"code": 0, "data": cached, "msg": "ok"}

    try:
        resp = await _http.get(TENCENT_QUOTE_URL.format(code=code), timeout=10)
        resp.raise_for_status()
        text = resp.content.decode("gbk", errors="ignore")
    except Exception as e:
        raise HTTPException(502, f"行情源请求失败: {e}")

    m = re.search(r'v_[A-Za-z0-9]+="([^"]*)"', text)
    if not m or not m.group(1):
        _log_upstream_error("tencent_quote", code, "未匹配到行情数据", text[:200])
        raise HTTPException(404, "无该代码行情")
    f = m.group(1).split("~")
    if len(f) < 50:
        _log_upstream_error("tencent_quote", code, f"字段数不足: {len(f)}/50", m.group(1)[:200])
        raise HTTPException(404, "行情字段不足")

    data = {
        "code": code, "name": _qf(f, 1), "price": _qf(f, 3),
        "prevClose": _qf(f, 4), "open": _qf(f, 5), "time": _qf(f, 30),
        "change": _qf(f, 31), "changePct": _qf(f, 32),
        "high": _qf(f, 33), "low": _qf(f, 34),
        "volume": _qf(f, 36), "amountWan": _qf(f, 37),
        "turnover": _qf(f, 38), "pe": _qf(f, 39),
        "amplitude": _qf(f, 43), "circCapYi": _qf(f, 44),
        "totalCapYi": _qf(f, 45), "pb": _qf(f, 46),
        "volRatio": _qf(f, 49), "avgPrice": _qf(f, 51),
        "high52": _qf(f, 67), "low52": _qf(f, 68),
    }
    _set_cache(cache_key, data)
    return {"code": 0, "data": data, "msg": "ok"}


@app.get("/api/stock/kline")
async def stock_kline(code: str = Query(...), count: int = Query(320, ge=10, le=800)):
    if not re.fullmatch(r"[A-Za-z0-9]{2,12}", code or ""):
        raise HTTPException(400, "代码格式错误")
    cache_key = f"stock_kline_{code}_{count}"
    cached = _get_cache(cache_key, HISTORY_TTL)
    if cached is not None:
        return {"code": 0, "data": cached, "msg": "ok"}

    try:
        resp = await _http.get(TENCENT_KLINE_URL,
                               params={"param": f"{code},day,,,{count},qfq"}, timeout=15)
        resp.raise_for_status()
        raw = resp.json()
    except Exception as e:
        raise HTTPException(502, f"K线源请求失败: {e}")

    node = (raw.get("data") or {}).get(code) or {}
    rows = node.get("day") or node.get("qfqday") or []
    kline = []
    for r in rows:
        try:
            kline.append({
                "date": str(r[0])[:10], "open": float(r[1]), "close": float(r[2]),
                "high": float(r[3]), "low": float(r[4]),
                "volume": float(r[5]) if len(r) > 5 else 0,
            })
        except (ValueError, TypeError, IndexError):
            continue
    if not kline:
        raise HTTPException(404, "无K线数据")

    result = {"code": code, "kline": kline, "count": len(kline)}
    _set_cache(cache_key, result)
    return {"code": 0, "data": result, "msg": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
