"""
行情雷达 - FastAPI 后端 v2
新增分时数据接口：股票/ETF分钟线、加密货币分钟K线
v2.1.0: 新增用户反馈接口(/api/feedback)、基金实时估值批量接口(/api/fund/realtime)
"""
import os
import re
import json
import time
import asyncio
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List

import httpx
from fastapi import FastAPI, HTTPException, Query, Body
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
    return {"status": "ok", "version": "2.1.0"}


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


# ─── 基金历史净值（双数据源：新浪主源 + 东方财富备用）─────────
SINA_FUND_URL = "https://stock.finance.sina.com.cn/fundInfo/api/openapi.php/CaihuiFundInfoService.getNav"
EM_FUND_URL = "https://api.fund.eastmoney.com/f10/lsjz"


async def _fetch_sina_fund_history(code: str, days: int = 370) -> list:
    """
    新浪基金历史净值（海外可达性好，主源）
    返回 [{date, nav}] 按日期升序，已去重
    """
    import asyncio
    from datetime import datetime, timedelta

    date_to = datetime.now().strftime("%Y-%m-%d")
    date_from = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    async def fetch_page(page: int) -> dict:
        resp = await _http.get(SINA_FUND_URL, params={
            "symbol": code,
            "datefrom": date_from,
            "dateto": date_to,
            "page": page
        })
        resp.raise_for_status()
        raw = resp.json()
        return raw.get("result", {}).get("data", {}) or {}

    first = await fetch_page(1)
    data_list = list(first.get("data") or [])
    total = int(first.get("total_num") or 0)
    # 新浪每页约21条；限制最多翻15页防失控
    total_pages = min((total + 20) // 21, 15)

    if total_pages > 1:
        pages = await asyncio.gather(
            *[fetch_page(p) for p in range(2, total_pages + 1)],
            return_exceptions=True
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

    # 升序 + 按日期去重（分页边界有重叠记录）
    history.sort(key=lambda x: x["date"])
    seen = set()
    dedup = []
    for h in history:
        if h["date"] not in seen:
            seen.add(h["date"])
            dedup.append(h)
    return dedup


async def _fetch_em_fund_history(code: str) -> list:
    """
    东方财富基金历史净值（国内快，备用源；海外IP可能被拦）
    返回 [{date, nav}] 按日期升序
    """
    resp = await _http.get(EM_FUND_URL, params={
        "fundCode": code,
        "pageIndex": 1,
        "pageSize": 365
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
    """
    获取基金历史净值（近一年）
    数据源优先级：新浪 → 东方财富；全部失败返回 404
    """
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
        "code": code,
        "history": history,
        "count": len(history),
        "dateRange": f"{history[0]['date']}~{history[-1]['date']}"
    }

    _set_cache(cache_key, result)
    return {"code": 0, "data": result, "msg": "ok"}


# ═══ 微信订阅消息（涨跌提醒 v7.2.0）═══════════════════════════════
# AppSecret 走 Render 环境变量，绝不写入代码（GitHub 仓库公开）
WX_APPID = "wx7f5552fc52317b2a"
WX_SECRET = os.environ.get("WX_SECRET", "")
WX_TEMPLATE_ID = "PUqStjuTo2xby_vdpIZas_VpvZUnUHnmwQtuMwr34wA"
WX_NOTIFY_KEY = os.environ.get("WX_NOTIFY_KEY", "")

# ⚠️ 模板字段映射：按微信公众平台「我的模板」里的实际字段编号调整（类型/格式必须匹配）
# 模板4字段：1.基金名称 2.涨跌幅度 3.更新时间 4.当前价格
WX_FIELD_NAME = "thing1"      # 基金名称（待主人确认编号）
WX_FIELD_CHANGE = "thing2"    # 涨跌幅度（待主人确认编号）
WX_FIELD_TIME = "time3"       # 更新时间（待主人确认编号）
WX_FIELD_PRICE = "number4"    # 当前价格（待主人确认编号）

# 订阅记录 {fund_code: [{"openid", "name", "ts"}]}  内存存储，一次性订阅发完即清
_wx_subs: Dict[str, list] = {}
_wx_token_cache: Dict[str, Any] = {"token": None, "ts": 0}


async def _wx_access_token() -> str:
    """获取并缓存微信 access_token（有效期7200s，缓存7000s）"""
    if _wx_token_cache["token"] and time.time() - _wx_token_cache["ts"] < 7000:
        return _wx_token_cache["token"]
    r = await _http.get("https://api.weixin.qq.com/cgi-bin/token", params={
        "grant_type": "client_credential", "appid": WX_APPID, "secret": WX_SECRET})
    d = r.json()
    if "access_token" not in d:
        raise HTTPException(500, f"wx token error: {d.get('errmsg')}")
    _wx_token_cache["token"] = d["access_token"]
    _wx_token_cache["ts"] = time.time()
    return d["access_token"]


@app.post("/api/wx/subscribe")
async def wx_subscribe(body: dict = Body(...)):
    """前端授权后上报：wx.login 的 code + 基金信息 → 换 openid 存订阅记录"""
    js_code = body.get("code", "")
    fund_code = body.get("fund_code", "")
    fund_name = (body.get("fund_name", "") or fund_code)[:20]
    if not (js_code and fund_code):
        raise HTTPException(400, "missing code or fund_code")
    if not WX_SECRET:
        raise HTTPException(500, "WX_SECRET not configured")

    r = await _http.get("https://api.weixin.qq.com/sns/jscode2session", params={
        "appid": WX_APPID, "secret": WX_SECRET,
        "js_code": js_code, "grant_type": "authorization_code"})
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
    """每日收盘后由外部定时器触发：给所有订阅者推一次涨跌消息（一次性订阅，发完即清）"""
    if not WX_NOTIFY_KEY or key != WX_NOTIFY_KEY:
        raise HTTPException(403, "bad key")
    if not WX_SECRET:
        raise HTTPException(500, "WX_SECRET not configured")

    sent, failed = 0, 0
    errors = []
    targets = [(fc, list(subs)) for fc, subs in _wx_subs.items() if subs]
    if not targets:
        return {"code": 0, "data": {"sent": 0, "failed": 0, "msg": "no subscribers"}, "msg": "ok"}

    token = await _wx_access_token()
    for fund_code, subs in targets:
        # 拉最新两条净值算日涨跌
        try:
            navs = await _fetch_sina_fund_history(fund_code, days=10)
            if len(navs) < 2:
                raise ValueError("nav data too short")
            latest, prev = navs[-1], navs[-2]
            chg = (latest["nav"] - prev["nav"]) / prev["nav"] * 100
            name = subs[0]["name"][:10]
            # thing 类型 ≤20 字符：「易方达消费 涨2.35%」
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
                            WX_FIELD_CONTENT: {"value": content},
                            WX_FIELD_DATE: {"value": date_str},
                        },
                    })
                resp = r.json()
                if resp.get("errcode") == 0:
                    sent += 1
                else:
                    failed += 1
                    errors.append(f"{fund_code}/{s['openid'][:8]}: errcode={resp.get('errcode')} {resp.get('errmsg')}")
            except Exception as e:
                failed += 1
                errors.append(f"{fund_code} send: {e}")
        # 一次性订阅：无论成败，发送尝试后清空该基金订阅（用户下次进入可重新订阅）
        _wx_subs[fund_code] = []

    logger.info(f"daily_notify done: sent={sent} failed={failed}")
    return {"code": 0, "data": {"sent": sent, "failed": failed, "errors": errors[:10]}, "msg": "ok"}


# ═══ v2.1.0 用户反馈通道 ═══════════════════════════════════════
ADMIN_KEY = os.environ.get("ADMIN_KEY", "radar2026")
FEEDBACK_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "feedbacks.jsonl")

# 内存反馈列表（Render 免费层重启后清空，jsonl 文件为尽力持久化）
_feedbacks: List[Dict[str, Any]] = []


@app.post("/api/feedback")
async def submit_feedback(body: dict = Body(...)):
    """
    接收用户反馈 {text, version, contact}，追加到内存列表并尽力写入 feedbacks.jsonl
    """
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

    # 尽力写入文件，失败不报错
    try:
        with open(FEEDBACK_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.warning(f"feedback 文件写入失败（忽略）: {e}")

    logger.info(f"feedback received: len={len(text)} version={entry['version']} total={len(_feedbacks)}")
    return {"code": 0, "data": {"ok": True}, "msg": "ok"}


@app.get("/api/feedback/list")
async def feedback_list(key: str = Query(..., description="管理密钥")):
    """查看全部反馈，key 需匹配环境变量 ADMIN_KEY（默认 radar2026）"""
    if key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="bad key")
    return {
        "code": 0,
        "data": {
            "count": len(_feedbacks),
            "items": _feedbacks,
            "note": "Render免费层重启后内存数据会清空，历史反馈以服务器 feedbacks.jsonl 文件为准",
        },
        "msg": "ok",
    }


# ═══ v2.1.0 基金实时估值（天天基金）═══════════════════════════════
FUND_GZ_URL = "https://fundgz.1234567.com.cn/js/{code}.js"
FUND_GZ_HEADERS = {"Referer": "https://fund.eastmoney.com/"}
_JSONPGZ_RE = re.compile(r"^jsonpgz\((.*)\);?\s*$", re.S)


async def _fetch_em_latest_nav(code: str) -> Optional[Dict[str, str]]:
    """
    备用源：东方财富 f10/lsjz 拉最新一条已公布净值
    （天天基金 fundgz 估值接口已于 2026-07 被监管要求下线，此处兜底最新净值）
    """
    try:
        resp = await _http.get(EM_FUND_URL, params={
            "fundCode": code,
            "pageIndex": 1,
            "pageSize": 1
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
        }
    except Exception as e:
        logger.warning(f"东方财富最新净值兜底失败 code={code}: {e}")
        return None


async def _fetch_fund_realtime_one(code: str) -> Dict[str, Any]:
    """
    拉取单只基金最新净值。
    注意：天天基金盘中估值接口 fundgz.1234567.com.cn 已于2024年监管要求全国下线（返回404），
    故主源直接用东方财富最新净值（每日晚间更新当日净值）；
    若东财失败再尝试 fundgz（万一恢复则额外提供盘中估值字段）。
    全部失败返回 {code, error}，不影响整体
    """
    fallback = await _fetch_em_latest_nav(code)
    if fallback and fallback.get("nav"):
        return {
            "code": code,
            "name": "",
            "nav": fallback["nav"],
            "navDate": fallback["navDate"],
            "estimate": "",
            "estimateChange": "",
            "estimateTime": "",
        }
    # 东财失败，尝试天天基金（已下线，保留仅为兼容）
    url = FUND_GZ_URL.format(code=code)
    try:
        resp = await _http.get(
            url,
            params={"rt": int(time.time() * 1000)},
            headers=FUND_GZ_HEADERS,
        )
        resp.raise_for_status()
        raw_text = resp.text.strip()

        m = _JSONPGZ_RE.match(raw_text)
        if not m:
            raise ValueError("非 jsonpgz 格式响应")
        payload = json.loads(m.group(1))

        return {
            "code": payload.get("fundcode", code),
            "name": payload.get("name", ""),
            "nav": payload.get("dwjz", ""),
            "navDate": payload.get("jzrq", ""),
            "estimate": payload.get("gsz", ""),
            "estimateChange": payload.get("gszzl", ""),
            "estimateTime": payload.get("gztime", ""),
        }
    except Exception as e:
        logger.warning(f"基金最新净值获取失败 code={code}: {e}")
        return {"code": code, "error": str(e)}


@app.get("/api/fund/realtime")
async def fund_realtime(codes: str = Query(..., description="基金代码，逗号分隔，最多50只")):
    """
    批量获取基金实时估值
    数据源：天天基金 fundgz.1234567.com.cn（需 Referer: fund.eastmoney.com）
    单只失败该只返回 error 字段，不影响其他基金
    """
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
