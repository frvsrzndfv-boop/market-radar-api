/**
 * 行情雷达 - Cloudflare Workers API v2.5.0
 * 完整迁移自 FastAPI 后端，22个端点，8+数据源透传
 */

// ─── 常量 & 映射表 ───────────────────────────────────────────
const VERSION = "2.5.0";
const ADMIN_KEY = "AOQIQlKNvJDcYl90-Mb_pQ";
const WX_NOTIFY_KEY = "radar_notify_2026";
const WX_APPID = "wx7f5552fc52317b2a";
const WX_TEMPLATE_ID = "PUqStjuTo2xby_vdpIZas_VpvZUnUHnmwQtuMwr34wA";

const BROWSER_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36";
const EM_HEADERS = { "Referer": "https://fund.eastmoney.com", "User-Agent": BROWSER_UA };

// ─── 缓存 TTL（秒）───────────────────────────────────────────
const INTRADAY_TTL = 60;
const HISTORY_TTL = 1800;
const DETAIL_TTL = 21600;
const REALTIME_TTL = 60;
const VALUATION_TTL = 43200;
const FUND_RANK_TTL = 43200;
const EST_HOLD_TTL = 86400;
const EST_NAV_TTL = 43200;

// ─── 内存缓存 ───────────────────────────────────────────────
const _cache = new Map();
function getCache(key, ttl) {
  const entry = _cache.get(key);
  if (entry && (Date.now() / 1000 - entry.ts) < ttl) return entry.data;
  _cache.delete(key);
  return null;
}
function setCache(key, data) {
  _cache.set(key, { data, ts: Date.now() / 1000 });
}

// ─── 限流 ───────────────────────────────────────────────────
const _rateLimits = new Map();
const RATE_WINDOW = 60;
const RATE_MAX = 80;

function checkRateLimit(ip) {
  const now = Date.now() / 1000;
  let tsList = _rateLimits.get(ip) || [];
  tsList = tsList.filter(t => now - t < RATE_WINDOW);
  if (tsList.length >= RATE_MAX) {
    _rateLimits.set(ip, tsList);
    return false;
  }
  tsList.push(now);
  _rateLimits.set(ip, tsList);
  if (_rateLimits.size > 500) {
    for (const [k, v] of _rateLimits) {
      if (!v.length || now - v[v.length - 1] > RATE_WINDOW * 2) _rateLimits.delete(k);
    }
  }
  return true;
}

// ─── 腾讯代码映射 ──────────────────────────────────────────
const TENCENT_CODE_MAP = {
  "sh000001": "sh000001", "sz399001": "sz399001", "sz399006": "sz399006",
  "HSI": "hkHSI", "HSTECH": "hkHSTECH",
  "sh510050": "sh510050", "sh510300": "sh510300", "sh510500": "sh510500",
  "sz159915": "sz159915", "sh588000": "sh588000", "sh513100": "sh513100",
  "sh513520": "sh513520", "sh513180": "sh513180",
};

// ─── 加密货币映射 ──────────────────────────────────────────
const CRYPTO_SYMBOL_MAP = {
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
};

// ─── 全球指数映射 ──────────────────────────────────────────
const GM_INDEX_MAP = {
  // 欧洲 23
  "gmFTSE": ["FTSE", "英国富时100"], "gmGDAXI": ["GDAXI", "德国DAX30"],
  "gmFCHI": ["FCHI", "法国CAC40"], "gmSX5E": ["SX5E", "欧洲斯托克50"],
  "gmSXXP": ["SXXP", "欧洲斯托克600"], "gmAEX": ["AEX", "荷兰AEX"],
  "gmSSMI": ["SSMI", "瑞士SMI"], "gmIBEX": ["IBEX", "西班牙IBEX35"],
  "gmMIB": ["MIB", "意大利MIB"], "gmRTS": ["RTS", "俄罗斯RTS"],
  "gmWIG": ["WIG", "波兰WIG"], "gmATX": ["ATX", "奥地利ATX"],
  "gmOSEBX": ["OSEBX", "挪威OSEBX"], "gmOMXSPI": ["OMXSPI", "瑞典OMXSPI"],
  "gmOMXC20": ["OMXC20", "哥本哈根20"], "gmBFX": ["BFX", "比利时BFX"],
  "gmISEQ": ["ISEQ", "爱尔兰综合"], "gmPSI20": ["PSI20", "葡萄牙PSI20"],
  "gmASE": ["ASE", "希腊雅典ASE"], "gmPX": ["PX", "布拉格指数"],
  "gmICEXI": ["ICEXI", "冰岛ICEX"], "gmHEX": ["HEX", "芬兰赫尔辛基"],
  "gmMCX": ["MCX", "英国富时250"],
  // 亚太 13
  "gmKS11": ["KS11", "韩国KOSPI"], "gmTWII": ["TWII", "台湾加权"],
  "gmAS51": ["AS51", "澳大利亚标普200"], "gmSENSEX": ["SENSEX", "印度孟买SENSEX"],
  "gmSTI": ["STI", "新加坡海峡时报"], "gmNZ50": ["NZ50", "新西兰50"],
  "gmVNINDEX": ["VNINDEX", "越南胡志明"], "gmJKSE": ["JKSE", "印尼雅加达综合"],
  "gmSET": ["SET", "泰国SET"], "gmKLSE": ["KLSE", "马来西亚KLCI"],
  "gmPSI": ["PSI", "菲律宾马尼拉"], "gmKSE100": ["KSE100", "巴基斯坦卡拉奇"],
  "gmCSEALL": ["CSEALL", "斯里兰卡科伦坡"],
  // 美洲 3
  "gmTSX": ["TSX", "加拿大S&P/TSX"], "gmBVSP": ["BVSP", "巴西BOVESPA"],
  "gmMXX": ["MXX", "墨西哥BOLSA"],
  // 其他 5
  "gmUDI": ["UDI", "美元指数"], "gmXIN9": ["XIN9", "富时中国A50"],
  "gmHSAHP": ["HSAHP", "AH股溢价"], "gmNDX100": ["NDX100", "纳斯达克100"],
  "gmFISAULMU": ["FISAULMU", "沙特阿拉伯指数"],
};

// ─── 板块类型映射 ──────────────────────────────────────────
const SECTOR_BOARD_MAP = {
  "industry": "hy",
  "industry2": "hy2",
  "concept": "gn",
};

// ─── 估值白名单 ────────────────────────────────────────────
const VALUATION_WHITELIST = [
  ["SH000016", "上证50"], ["SH000300", "沪深300"], ["SH000905", "中证500"],
  ["SH000852", "中证1000"], ["SZ399006", "创业板指"], ["SZ399001", "深证成指"],
];

// ─── 数据源 URL ────────────────────────────────────────────
const TENCENT_MINUTE_URL = "https://web.ifzq.gtimg.cn/appstock/app/minute/query";
const GATE_KLINES_URL = "https://api.gateio.ws/api/v4/spot/candlesticks";
const SINA_FUND_URL = "https://stock.finance.sina.com.cn/fundInfo/api/openapi.php/CaihuiFundInfoService.getNav";
const EM_FUND_URL = "https://api.fund.eastmoney.com/f10/lsjz";
const FUND_GZ_URL = "https://fundgz.1234567.com.cn/js/{code}.js";
const EM_FUND_RANK_URL = "http://fund.eastmoney.com/data/rankhandler.aspx";
const EM_PZD_URL = "https://fund.eastmoney.com/pingzhongdata/{code}.js";
const EM_JBGK_URL = "https://fundf10.eastmoney.com/jbgk_{code}.html";
const TENCENT_QUOTE_URL = "https://qt.gtimg.cn/q={code}";
const TENCENT_KLINE_URL = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get";
const EM_ZDFB_URL = "https://push2ex.eastmoney.com/getTopicZDFenBu";
const DANJUAN_VALUATION_URL = "https://danjuanfunds.com/djapi/index_eva/dj";
const TENCENT_SECTOR_RANK_URL = "https://proxy.finance.qq.com/cgi/cgi-bin/rank/pt/getRank";
const TENCENT_SECTOR_STOCKS_URL = "https://proxy.finance.qq.com/cgi/cgi-bin/rank/hs/getBoardRankList";
const SOHU_HISQ_URL = "https://q.stock.sohu.com/hisHq";
const THS_LINE_URL = "https://d.10jqka.com.cn/v6/line/{code}/01/last8.js";
const FUND_EST_HOLD_URL = "https://fundmobapi.eastmoney.com/FundMNewApi/FundMNInverstPosition";

const EM_KLINE_HOSTS = [
  "https://push2his.eastmoney.com",
  "https://92.push2his.eastmoney.com",
  "https://48.push2his.eastmoney.com",
];
const EM_KLINE_PATH = "/api/qt/stock/kline/get";
const EM_GM_CLIST_HOSTS = [
  "https://push2delay.eastmoney.com",
  "https://33.push2delay.eastmoney.com",
  "https://92.push2delay.eastmoney.com",
];
const EM_GM_CLIST_PATH = "/api/qt/clist/get";
const EM_GM_UT = "fa5fd1943c7b386f172d6893dbfba10b";

const EM_ZDFB_PARAMS = {
  ut: "7eea3edcaed734bea9cbfc24409ed989",
  dpt: "wz.ztzt", Pageindex: "0", pagesize: "200", sort: "zdf:asc",
};

// ─── 交易所映射 ────────────────────────────────────────────
const EXCH_MAP = { "1": "sh", "2": "sz" };

// ─── 有状态数据（KV 持久化）─────────────────────────────────
let _wx_subs = {};
let _feedbacks = [];
let _amount_hist = {};
let _prev_amount_debug = { last_ok_at: null, errors: [] };

// ─── 基金估值采样状态 ──────────────────────────────────────
const _est_watch = new Map();
const _est_points = {};
const _est_state = { date: "" };
const _est_unsupported = {};
const EST_WATCH_MAX = 300;

// ─── 工具函数 ──────────────────────────────────────────────
function beijingNow() {
  return new Date(Date.now() + 8 * 3600 * 1000);
}
function beijingDateStr() {
  const d = beijingNow();
  return d.toISOString().slice(0, 10);
}
function beijingDateTimeStr() {
  const d = beijingNow();
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth()+1).padStart(2,'0')}-${String(d.getUTCDate()).padStart(2,'0')} ${String(d.getUTCHours()).padStart(2,'0')}:${String(d.getUTCMinutes()).padStart(2,'0')}:${String(d.getUTCSeconds()).padStart(2,'0')}`;
}
function isTradingTime() {
  const d = beijingNow();
  const day = d.getUTCDay();
  if (day === 0 || day === 6) return false;
  const hm = d.getUTCHours() * 100 + d.getUTCMinutes();
  return (hm >= 930 && hm <= 1130) || (hm >= 1300 && hm <= 1500);
}
function inTradeWindow() {
  const d = beijingNow();
  const day = d.getUTCDay();
  if (day === 0 || day === 6) return false;
  const hm = d.getUTCHours() * 100 + d.getUTCMinutes();
  return (hm >= 928 && hm <= 1131) || (hm >= 1258 && hm <= 1504);
}
function msToDate(ms) {
  try {
    const d = new Date(parseInt(ms));
    return new Date(d.getTime() + 8 * 3600 * 1000).toISOString().slice(0, 10);
  } catch { return ""; }
}
function fnum(v) {
  const n = parseFloat(v);
  return isNaN(n) ? null : n;
}
function qf(fields, i) {
  return (i < fields.length) ? fields[i].trim() : "";
}
function normQuoteTime(raw) {
  raw = (raw || "").trim();
  if (/^\d{14}$/.test(raw)) {
    return `${raw.slice(0,4)}-${raw.slice(4,6)}-${raw.slice(6,8)} ${raw.slice(8,10)}:${raw.slice(10,12)}:${raw.slice(12,14)}`;
  }
  if (/^\d{8}$/.test(raw)) {
    return `${raw.slice(0,4)}-${raw.slice(4,6)}-${raw.slice(6,8)}`;
  }
  return raw || beijingDateTimeStr();
}
function jsVarStr(text, name) {
  const m = text.match(new RegExp(`var\\s+${name}\\s*=\\s*"([^"]*)"`));
  return m ? m[1] : "";
}
function jsVarJson(text, name) {
  const m = text.match(new RegExp(`var\\s+${name}\\s*=\\s*(\\[[\\s\\S]*?\\]|\\{[\\s\\S]*?\\})\\s*;`));
  if (!m) return null;
  try { return JSON.parse(m[1]); } catch { return null; }
}
function jbgkField(html, label) {
  const m = html.match(new RegExp(`${label}</th><td[^>]*>([\\s\\S]*?)</td>`));
  if (!m) return "";
  return m[1].replace(/<[^>]+>/g, "").replace(/\s+/g, " ").trim();
}
function gmF2s(v, nd = 2) {
  const n = parseFloat(v);
  return isNaN(n) ? "" : n.toFixed(nd);
}
function dbgErr(msg) {
  const ts = beijingDateTimeStr().slice(5, 16);
  _prev_amount_debug.errors.push(`${ts} ${msg}`.slice(0, 220));
  if (_prev_amount_debug.errors.length > 6) _prev_amount_debug.errors.shift();
}

// ─── HTTP 工具 ─────────────────────────────────────────────
async function fetchJson(url, opts = {}) {
  const resp = await fetch(url, { ...opts, signal: opts.signal || AbortSignal.timeout((opts.timeoutMs || 15000)) });
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  return resp.json();
}
async function fetchText(url, opts = {}) {
  const resp = await fetch(url, { ...opts, signal: opts.signal || AbortSignal.timeout((opts.timeoutMs || 15000)) });
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  return resp.text();
}
async function fetchArrayBuffer(url, opts = {}) {
  const resp = await fetch(url, { ...opts, signal: opts.signal || AbortSignal.timeout((opts.timeoutMs || 15000)) });
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  return resp.arrayBuffer();
}
function buildUrl(base, params) {
  const u = new URL(base);
  for (const [k, v] of Object.entries(params || {})) {
    if (v !== undefined && v !== null) u.searchParams.set(k, String(v));
  }
  return u.toString();
}

// ─── CORS 预检处理 ─────────────────────────────────────────
function handleCors() {
  return new Response(null, {
    status: 204,
    headers: {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
      "Access-Control-Max-Age": "86400",
    },
  });
}
function jsonResponse(data, status = 200, extraHeaders = {}) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*", ...extraHeaders },
  });
}
function errorResponse(msg, status = 500) {
  return jsonResponse({ code: -1, msg }, status);
}

// ─── KV 状态加载/保存 ──────────────────────────────────────
async function loadState(env) {
  try {
    const [subsRaw, fbRaw, amtRaw] = await Promise.all([
      env.KV?.get("wx_subs", "json"),
      env.KV?.get("feedbacks", "json"),
      env.KV?.get("amount_hist", "json"),
    ]);
    if (subsRaw && typeof subsRaw === "object") _wx_subs = subsRaw;
    if (Array.isArray(fbRaw)) _feedbacks = fbRaw;
    if (amtRaw && typeof amtRaw === "object") _amount_hist = amtRaw;
  } catch (e) { console.warn("loadState error:", e); }
}
async function saveWxSubs(env) {
  try { await env.KV?.put("wx_subs", JSON.stringify(_wx_subs)); } catch {}
}
async function saveFeedbacks(env) {
  try { await env.KV?.put("feedbacks", JSON.stringify(_feedbacks)); } catch {}
}
async function saveAmountHist(env) {
  try { await env.KV?.put("amount_hist", JSON.stringify(_amount_hist)); } catch {}
}

// ═══════════════════════════════════════════════════════════
// 数据获取函数
// ═══════════════════════════════════════════════════════════

// ─── 腾讯分时 ──────────────────────────────────────────────
async function fetchTencentIntraday(code) {
  const tencentCode = TENCENT_CODE_MAP[code] || code;
  const url = buildUrl(TENCENT_MINUTE_URL, { code: tencentCode });
  const raw = await fetchJson(url);
  if (raw.code !== 0) throw new Error(`上游返回码非0: ${raw.code}`);

  const stockData = raw.data?.[tencentCode]?.data;
  if (!stockData) throw new Error("数据节点缺失");
  const minuteList = stockData.data || [];
  const preClose = stockData.qt?.[tencentCode] || [];

  const points = [];
  for (const item of minuteList) {
    const parts = item.split(" ");
    if (parts.length >= 3) {
      const t = parts[0];
      const tFmt = t.length === 4 ? t.slice(0, 2) + ":" + t.slice(2) : t;
      points.push({ time: tFmt, price: parseFloat(parts[1]), volume: parseInt(parts[2]) });
    }
  }
  let prevClose = null;
  if (preClose.length > 3) {
    const v = parseFloat(preClose[3]);
    if (!isNaN(v)) prevClose = v;
  }
  return { code, points, prevClose, count: points.length };
}

// ─── Gate.io 加密分时 ──────────────────────────────────────
async function fetchGateIntraday(symbol) {
  const pair = CRYPTO_SYMBOL_MAP[symbol.toUpperCase()] || `${symbol.toUpperCase()}_USDT`;
  const url = buildUrl(GATE_KLINES_URL, { currency_pair: pair, interval: "5m", limit: "288" });
  const raw = await fetchJson(url);
  if (!Array.isArray(raw) || !raw.length) throw new Error("上游返回空数据");

  const points = [];
  for (const k of raw) {
    if (k.length < 6) continue;
    const ts = parseInt(k[0]);
    const bj = new Date(ts * 1000 + 8 * 3600 * 1000);
    const tFmt = `${String(bj.getUTCHours()).padStart(2,'0')}:${String(bj.getUTCMinutes()).padStart(2,'0')}`;
    points.push({
      time: tFmt, open: parseFloat(k[4]), high: parseFloat(k[2]),
      low: parseFloat(k[3]), close: parseFloat(k[5]),
      volume: parseFloat(k[1]), timestamp: ts,
    });
  }
  if (!points.length) throw new Error("无有效分时数据");
  return { symbol: symbol.toUpperCase(), pair, points, prevClose: points[0].open, count: points.length };
}

// ─── 新浪基金历史 ──────────────────────────────────────────
async function fetchSinaFundHistory(code, days = 370) {
  const now = new Date();
  const from = new Date(now.getTime() - days * 86400000);
  const dateTo = now.toISOString().slice(0, 10);
  const dateFrom = from.toISOString().slice(0, 10);

  async function fetchPage(page) {
    const url = buildUrl(SINA_FUND_URL, { symbol: code, datefrom: dateFrom, dateto: dateTo, page });
    const raw = await fetchJson(url);
    return raw?.result?.data || {};
  }

  const first = await fetchPage(1);
  const dataList = [...(first.data || [])];
  const total = parseInt(first.total_num) || 0;
  const totalPages = Math.min(Math.ceil((total + 1) / 21), 15);

  if (totalPages > 1) {
    const rest = await Promise.allSettled(
      Array.from({ length: totalPages - 1 }, (_, i) => fetchPage(i + 2))
    );
    for (const r of rest) {
      if (r.status === "fulfilled" && r.value?.data) dataList.push(...r.value.data);
    }
  }

  const history = [];
  const seen = new Set();
  for (const item of dataList) {
    const nav = item.jjjz;
    const date = (item.fbrq || "").slice(0, 10);
    if (nav && date && !seen.has(date)) {
      const v = parseFloat(nav);
      if (!isNaN(v)) { history.push({ date, nav: v }); seen.add(date); }
    }
  }
  history.sort((a, b) => a.date.localeCompare(b.date));
  return history;
}

// ─── 东财全量历史（pingzhongdata）─────────────────────────
async function fetchEmFundHistory(code) {
  const url = buildUrl(EM_PZD_URL.replace("{code}", code), { rt: Date.now() });
  const text = await fetchText(url, { headers: EM_HEADERS });
  const trend = jsVarJson(text, "Data_netWorthTrend") || [];
  const history = [];
  for (const item of trend) {
    if (!item || typeof item !== "object") continue;
    const nav = item.y;
    const date = msToDate(item.x);
    if (nav == null || !date) continue;
    const v = parseFloat(nav);
    if (!isNaN(v)) history.push({ date, nav: v });
  }
  return history;
}

// ─── 东财最新净值 ──────────────────────────────────────────
async function fetchEmLatestNav(code) {
  try {
    const url = buildUrl(EM_FUND_URL, { fundCode: code, pageIndex: "1", pageSize: "1" });
    const raw = await fetchJson(url, { headers: { "Referer": "https://fund.eastmoney.com" } });
    const lst = raw?.Data?.LSJZList || [];
    if (!lst.length) return null;
    return { nav: lst[0].DWJZ || "", navDate: lst[0].FSRQ || "", change: lst[0].JZZZL || "", accNav: lst[0].LJJZ || "" };
  } catch { return null; }
}

// ─── 基金实时估值单只 ─────────────────────────────────────
async function fetchFundRealtimeOne(code) {
  const fallback = await fetchEmLatestNav(code);
  if (fallback?.nav) {
    return { code, name: "", nav: fallback.nav, navDate: fallback.navDate, change: fallback.change || "", accNav: fallback.accNav || "", estimate: "", estimateChange: "", estimateTime: "" };
  }
  try {
    const url = buildUrl(FUND_GZ_URL.replace("{code}", code), { rt: Date.now() });
    const text = (await fetchText(url, { headers: { "Referer": "https://fund.eastmoney.com/" } })).trim();
    const m = text.match(/^jsonpgz\(([\s\S]*)\);?\s*$/);
    if (!m) throw new Error("非jsonpgz格式");
    const payload = JSON.parse(m[1]);
    return {
      code: payload.fundcode || code, name: payload.name || "",
      nav: payload.dwjz || "", navDate: payload.jzrq || "",
      estimate: payload.gsz || "", estimateChange: payload.gszzl || "",
      estimateTime: payload.gztime || "",
    };
  } catch (e) {
    return { code, error: String(e) };
  }
}

// ─── 东财基金排行批量收益率 ────────────────────────────────
async function fetchFundReturnsBatch() {
  const now = new Date();
  const sd = new Date(now.getTime() - 400 * 86400000).toISOString().slice(0, 10);
  const ed = now.toISOString().slice(0, 10);
  const v = `0.${Date.now() % 1e16}`;
  const url = buildUrl(EM_FUND_RANK_URL, {
    op: "ph", dt: "kf", ft: "all", rs: "", gs: "0",
    sc: "1nzf", st: "desc", sd, ed,
    qdii: "", tabSubtype: ",,,,,",
    pi: "1", pn: "5000", dx: "1", v,
  });
  const text = await fetchText(url, {
    headers: { "User-Agent": BROWSER_UA, "Referer": "http://fund.eastmoney.com/data/fundranking.html" },
    timeoutMs: 20000,
  });
  const m = text.match(/datas:\[([\s\S]*?)\]/);
  const result = {};
  if (m) {
    const items = m[1].match(/"([^"]+)"/g) || [];
    for (const raw of items) {
      const fields = raw.slice(1, -1).split(",");
      if (fields.length >= 12) {
        const code = fields[0];
        const r1y = fields[11];
        if (code && r1y) {
          const v2 = parseFloat(r1y);
          if (!isNaN(v2)) result[code] = { code, r1y };
        }
      }
    }
  }
  return result;
}

// ─── 基金持仓（东财 mobile API）───────────────────────────
async function fetchEstHoldings(code) {
  const ck = `fund_est_hold_${code}`;
  const cached = getCache(ck, EST_HOLD_TTL);
  if (cached) return cached;
  const url = buildUrl(FUND_EST_HOLD_URL, {
    FCODE: code, deviceid: "spark_radar_01",
    plat: "Android", product: "EFund", version: "6.2.4",
  });
  const raw = await fetchJson(url, { headers: { "User-Agent": "Mozilla/5.0" }, timeoutMs: 10000 });
  const datas = raw?.Datas || {};
  const fundStocks = datas?.fundStocks || [];
  if (!fundStocks.length) return null;
  const disclosureDate = (raw?.Expansion || "").trim();
  const stocks = [];
  let coverage = 0;
  for (const s of fundStocks) {
    const exch = EXCH_MAP[s.TEXCH];
    const gpdm = s.GPDM || "";
    if (!exch || !gpdm) continue;
    const weight = parseFloat(s.JZBL) || 0;
    if (weight <= 0) continue;
    stocks.push({ tcode: `${exch}${gpdm}`, name: s.GPJC || "", weight });
    coverage += weight;
  }
  if (!stocks.length) return null;
  const result = { stocks, date: disclosureDate, coverage: Math.round(coverage * 100) / 100 };
  setCache(ck, result);
  return result;
}

// ─── 昨日净值 ──────────────────────────────────────────────
async function fetchEstPrevNav(code) {
  const ck = `fund_est_nav_${code}`;
  const cached = getCache(ck, EST_NAV_TTL);
  if (cached) return cached;
  const nav = await fetchEmLatestNav(code);
  if (!nav?.nav) return null;
  const v = parseFloat(nav.nav);
  if (isNaN(v)) return null;
  setCache(ck, v);
  return v;
}

// ─── 腾讯行情解码（GBK）───────────────────────────────────
async function fetchTencentQuoteText(codes) {
  const url = TENCENT_QUOTE_URL.replace("{code}", codes);
  const buf = await fetchArrayBuffer(url, { timeoutMs: 10000 });
  return new TextDecoder("gbk").decode(buf);
}

function parseTencentQuote(text, code) {
  const m = text.match(new RegExp(`v_${code}="([^"]*)"`));
  if (!m || !m[1]) return null;
  const f = m[1].split("~");
  if (f.length < 50) return null;
  return {
    code, name: qf(f, 1), price: qf(f, 3),
    prevClose: qf(f, 4), open: qf(f, 5), time: normQuoteTime(qf(f, 30)),
    change: qf(f, 31), changePct: qf(f, 32),
    high: qf(f, 33), low: qf(f, 34),
    volume: qf(f, 36), amountWan: qf(f, 37),
    turnover: qf(f, 38), pe: qf(f, 39),
    amplitude: qf(f, 43), circCapYi: qf(f, 44),
    totalCapYi: qf(f, 45), pb: qf(f, 46),
    volRatio: qf(f, 49), avgPrice: qf(f, 51),
    high52: qf(f, 67), low52: qf(f, 68),
  };
}

function parseTencentQuotesBatch(text) {
  const results = {};
  const re = /v_([A-Za-z0-9]+)="([^"]*)"/g;
  let m;
  while ((m = re.exec(text)) !== null) {
    const c = m[1], payload = m[2];
    const f = payload.split("~");
    if (f.length < 50) continue;
    results[c] = {
      code: c, name: qf(f, 1), price: qf(f, 3),
      prevClose: qf(f, 4), open: qf(f, 5), time: normQuoteTime(qf(f, 30)),
      change: qf(f, 31), changePct: qf(f, 32),
      high: qf(f, 33), low: qf(f, 34),
      volume: qf(f, 36), amountWan: qf(f, 37),
      turnover: qf(f, 38), pe: qf(f, 39),
      amplitude: qf(f, 43), circCapYi: qf(f, 44),
      totalCapYi: qf(f, 45), pb: qf(f, 46),
      volRatio: qf(f, 49), avgPrice: qf(f, 51),
      high52: qf(f, 67), low52: qf(f, 68),
    };
  }
  return results;
}

// ─── 全球指数报价 ──────────────────────────────────────────
function gmItemToQuote(gmCode, it) {
  const name = GM_INDEX_MAP[gmCode][1];
  const high = it.f15, low = it.f16, prev = it.f18;
  let amp = "";
  try {
    if (high != null && low != null && prev != null && parseFloat(prev) > 0) {
      amp = ((parseFloat(high) - parseFloat(low)) / parseFloat(prev) * 100).toFixed(2);
    }
  } catch {}
  let tstr;
  try { tstr = new Date(parseInt(it.f124) * 1000 + 8 * 3600 * 1000).toISOString().replace("T", " ").slice(0, 19); }
  catch { tstr = beijingDateTimeStr(); }
  return {
    code: gmCode, name, price: gmF2s(it.f2),
    prevClose: gmF2s(prev), open: gmF2s(it.f17), time: tstr,
    change: gmF2s(it.f4), changePct: gmF2s(it.f3),
    high: gmF2s(high), low: gmF2s(low),
    volume: "", amountWan: "", turnover: "", pe: "",
    amplitude: amp, circCapYi: "", totalCapYi: "", pb: "",
    volRatio: "", avgPrice: "", high52: "", low52: "",
  };
}

async function fetchGmQuotes() {
  const cached = getCache("gm_quotes_all", REALTIME_TTL);
  if (cached) return cached;
  const params = { pn: "1", pz: "100", po: "1", np: "1", fltt: "2", invt: "2",
    fid: "f3", fs: "m:100", fields: "f12,f14,f2,f3,f4,f15,f16,f17,f18,f124", ut: EM_GM_UT };
  const wanted = {};
  for (const [gm, [f12]] of Object.entries(GM_INDEX_MAP)) wanted[f12] = gm;

  for (const host of EM_GM_CLIST_HOSTS) {
    try {
      const url = buildUrl(host + EM_GM_CLIST_PATH, params);
      const raw = await fetchJson(url, { timeoutMs: 10000 });
      const diff = raw?.data?.diff || [];
      const out = {};
      for (const it of diff) {
        const gm = wanted[String(it.f12 || "")];
        if (gm) out[gm] = gmItemToQuote(gm, it);
      }
      if (Object.keys(out).length) {
        for (const [gm, q] of Object.entries(out)) setCache(`stock_quote_${gm}`, q);
        setCache("gm_quotes_all", out);
        return out;
      }
    } catch (e) { console.warn(`gm_quotes host fail ${host}:`, e); }
  }
  return {};
}

async function fetchGmQuoteOne(code) {
  const cached = getCache(`stock_quote_${code}`, INTRADAY_TTL);
  if (cached) return cached;
  const quotes = await fetchGmQuotes();
  if (!quotes[code]) throw new Error("全球指数行情源暂不可用");
  return quotes[code];
}

async function fetchGmKline(code, count) {
  const f12 = GM_INDEX_MAP[code][0];
  const params = { secid: `100.${f12}`, fields1: "f1,f2,f3",
    fields2: "f51,f52,f53,f54,f55,f56", klt: "101", fqt: "0",
    beg: "0", end: "20500101", lmt: String(count), ut: EM_GM_UT };
  for (const host of EM_KLINE_HOSTS) {
    try {
      const url = buildUrl(host + EM_KLINE_PATH, params);
      const raw = await fetchJson(url, { timeoutMs: 12000 });
      const klines = raw?.data?.klines;
      if (klines?.length) {
        return klines.map(row => {
          const p = String(row).split(",");
          return { date: p[0].slice(0, 10), open: parseFloat(p[1]), close: parseFloat(p[2]), high: parseFloat(p[3]), low: parseFloat(p[4]), volume: parseFloat(p[5]) || 0 };
        }).filter(k => !isNaN(k.open));
      }
    } catch (e) { console.warn(`gm_kline host fail ${host}:`, e); }
  }
  throw new Error("暂无该指数K线");
}

// ─── 昨日成交额三源容错 ────────────────────────────────────
async function fetchPrevAmountThs(dateStr) {
  const target = dateStr.replace(/-/g, "");
  if (target.length !== 8) return null;
  async function one(code) {
    try {
      const url = THS_LINE_URL.replace("{code}", code);
      const text = await fetchText(url, { headers: { "User-Agent": BROWSER_UA, "Referer": "http://q.10jqka.com.cn/" }, timeoutMs: 8000 });
      const m = text.match(/\((\{[\s\S]*\})\)/);
      const rows = (m ? JSON.parse(m[1]) : {})?.data?.split(";") || [];
      let prev = null;
      for (const row of rows) {
        const p = row.split(",");
        if (p.length >= 7 && p[0] < target && p[6]) {
          const v = parseFloat(p[6]);
          if (!isNaN(v)) prev = v;
        }
      }
      return prev;
    } catch (e) { dbgErr(`ths ${code} ${e}`); return null; }
  }
  const [shy, szy] = await Promise.all([one("hs_1A0001"), one("hs_399001")]);
  if (shy != null && szy != null) {
    _prev_amount_debug.last_ok_at = beijingDateTimeStr();
    return Math.round((shy + szy) / 1e8 * 10) / 10;
  }
  return null;
}

async function fetchPrevAmountSohu(dateStr) {
  try {
    const d0 = new Date(new Date(dateStr).getTime() - 12 * 86400000);
    const start = d0.toISOString().slice(0, 10).replace(/-/g, "");
    const end = dateStr.replace(/-/g, "");
    async function one(code) {
      try {
        const url = buildUrl(SOHU_HISQ_URL, { code, start, end });
        const arr = await fetchJson(url, { headers: { "User-Agent": BROWSER_UA }, timeoutMs: 8000 });
        const hq = (Array.isArray(arr) && arr[0])?.hq || [];
        for (const row of hq) {
          if (Array.isArray(row) && row.length >= 9 && row[0] < dateStr && row[8]) {
            const v = parseFloat(row[8]);
            if (!isNaN(v)) return v;
          }
        }
        return null;
      } catch (e) { dbgErr(`sohu ${code} ${e}`); return null; }
    }
    const [shWan, szWan] = await Promise.all([one("zs_000001"), one("zs_399001")]);
    if (shWan != null && szWan != null) return Math.round((shWan + szWan) / 10000 * 10) / 10;
    return null;
  } catch { return null; }
}

async function fetchPrevAmountEm(dateStr) {
  async function one(secid) {
    for (const host of EM_KLINE_HOSTS) {
      try {
        const url = buildUrl(host + EM_KLINE_PATH, {
          secid, ut: EM_GM_UT, fields1: "f1,f2,f3,f4,f5,f6",
          fields2: "f51,f57", klt: "101", fqt: "0", lmt: "5", end: "20500101",
        });
        const raw = await fetchJson(url, {
          headers: { "User-Agent": BROWSER_UA, "Referer": "https://quote.eastmoney.com/" },
          timeoutMs: 8000,
        });
        const klines = raw?.data?.klines || [];
        for (const row of klines) {
          const p = String(row).split(",");
          if (p.length >= 2 && p[0] < dateStr && p[1]) return parseFloat(p[1]);
        }
      } catch {}
    }
    return null;
  }
  const [shAmt, szAmt] = await Promise.all([one("1.000001"), one("0.399001")]);
  if (shAmt != null && szAmt != null) {
    _prev_amount_debug.last_ok_at = beijingDateTimeStr();
    return Math.round((shAmt + szAmt) / 1e8 * 10) / 10;
  }
  return null;
}

async function fetchPrevAmountYi(dateStr, env) {
  if (!dateStr) return null;
  const ths = await fetchPrevAmountThs(dateStr);
  if (ths != null) return ths;
  const sohu = await fetchPrevAmountSohu(dateStr);
  if (sohu != null) return sohu;
  const em = await fetchPrevAmountEm(dateStr);
  if (em != null) return em;
  return null;
}

// ─── 基金分时估值采样 ──────────────────────────────────────
function estRollover() {
  const today = beijingDateStr();
  if (_est_state.date !== today) {
    _est_state.date = today;
    for (const k of Object.keys(_est_points)) delete _est_points[k];
    for (const k of Object.keys(_est_unsupported)) delete _est_unsupported[k];
  }
}

async function estSampleOne(code) {
  if (_est_unsupported[code]) return null;
  const holdings = await fetchEstHoldings(code);
  if (!holdings?.stocks?.length) { _est_unsupported[code] = "持仓未披露"; return null; }
  const prevNav = await fetchEstPrevNav(code);
  if (!prevNav) { _est_unsupported[code] = "无昨日净值"; return null; }

  const stocks = holdings.stocks;
  const quoteCodes = stocks.map(s => s.tcode).join(",");
  const pctMap = {};
  try {
    const text = await fetchTencentQuoteText(quoteCodes);
    const re = /v_([A-Za-z0-9]+)="([^"]*)"/g;
    let m;
    while ((m = re.exec(text)) !== null) {
      const f = m[2].split("~");
      if (f.length < 33) continue;
      pctMap[m[1]] = parseFloat(f[32]) || 0;
    }
  } catch { return null; }

  let contrib = 0, totalWeight = 0;
  const nowBj = beijingNow();
  const hm = `${String(nowBj.getUTCHours()).padStart(2,'0')}:${String(nowBj.getUTCMinutes()).padStart(2,'0')}`;
  for (const s of stocks) {
    const pct = pctMap[s.tcode];
    if (pct == null) continue;
    contrib += s.weight * pct;
    totalWeight += s.weight;
  }
  if (totalWeight <= 0) { _est_unsupported[code] = "实时行情缺失"; return null; }
  const estChg = contrib / 100;
  const estNav = prevNav * (1 + estChg / 100);

  if (!_est_points[code]) _est_points[code] = [];
  const pts = _est_points[code];
  const pt = { t: hm, v: estNav.toFixed(4), p: estChg.toFixed(2) };
  if (pts.length && pts[pts.length - 1].t === hm) pts[pts.length - 1] = pt;
  else pts.push(pt);

  return {
    estimate: pt.v, estimateChange: pt.p,
    estimateTime: beijingDateTimeStr(),
    coverage: holdings.coverage, holdingsDate: holdings.date,
  };
}

// ═══════════════════════════════════════════════════════════
// 路由 & Handler
// ═══════════════════════════════════════════════════════════

async function handleRequest(request, env) {
  const url = new URL(request.url);
  const path = url.pathname;
  const method = request.method;
  const params = url.searchParams;

  // GET params helper
  const gp = (name) => params.get(name);
  // POST body helper
  const postBody = async () => { try { return await request.json(); } catch { return {}; } };

  // ─── 1. 健康检查 ───────────────────────────────────────
  if (path === "/api/health" && method === "GET") {
    return jsonResponse({ status: "ok", version: VERSION });
  }

  // ─── 2. 运营指标 ───────────────────────────────────────
  if (path === "/api/metrics" && method === "GET") {
    if (gp("key") !== ADMIN_KEY) return errorResponse("bad key", 403);
    return jsonResponse({
      code: 0, msg: "ok",
      data: {
        version: VERSION,
        cache_size: _cache.size,
        feedback_count: _feedbacks.length,
        wx_subscribers: Object.values(_wx_subs).reduce((s, v) => s + v.length, 0),
        rate_limited_ips: _rateLimits.size,
      },
    });
  }

  // ─── 3. 备份状态导出 ───────────────────────────────────
  if (path === "/api/admin/backup_state" && method === "GET") {
    if (gp("key") !== ADMIN_KEY) return errorResponse("bad key", 403);
    return jsonResponse({
      code: 0, msg: "ok",
      data: {
        version: "2.4.7",
        exported_at: beijingDateTimeStr(),
        wx_subs: _wx_subs,
        feedbacks: _feedbacks,
        amount_hist: _amount_hist,
        prev_amount_debug: _prev_amount_debug,
      },
    });
  }

  // ─── 4. 股票/指数分时 ──────────────────────────────────
  if (path === "/api/intraday/stock" && method === "GET") {
    const code = gp("code");
    if (!code) return errorResponse("missing code", 400);
    const ck = `intraday_stock_${code}`;
    const cached = getCache(ck, INTRADAY_TTL);
    if (cached) return jsonResponse({ code: 0, data: cached, msg: "ok" });
    try {
      const data = await fetchTencentIntraday(code);
      setCache(ck, data);
      return jsonResponse({ code: 0, data, msg: "ok" });
    } catch (e) { return errorResponse(`上游数据获取失败: ${e.message}`, 502); }
  }

  // ─── 5. 加密货币分时 ───────────────────────────────────
  if (path === "/api/intraday/crypto" && method === "GET") {
    const symbol = gp("symbol");
    if (!symbol) return errorResponse("missing symbol", 400);
    const ck = `intraday_crypto_${symbol}`;
    const cached = getCache(ck, INTRADAY_TTL);
    if (cached) return jsonResponse({ code: 0, data: cached, msg: "ok" });
    try {
      const data = await fetchGateIntraday(symbol);
      setCache(ck, data);
      return jsonResponse({ code: 0, data, msg: "ok" });
    } catch (e) { return errorResponse(`上游数据获取失败: ${e.message}`, 502); }
  }

  // ─── 6. 基金历史净值 ───────────────────────────────────
  if (path === "/api/fund/history" && method === "GET") {
    const code = gp("code");
    if (!code || code.length !== 6 || !/^\d{6}$/.test(code)) return errorResponse("基金代码格式错误", 400);
    const ck = `fund_history_${code}`;
    const cached = getCache(ck, HISTORY_TTL);
    if (cached) return jsonResponse({ code: 0, data: cached, msg: "ok" });
    const errors = [];
    let history = [];
    try {
      history = await fetchEmFundHistory(code);
      if (history.length) console.log(`东方财富全量历史成功 code=${code} count=${history.length}`);
    } catch (e) { errors.push(`em: ${e.message}`); }
    if (!history.length) {
      try {
        history = await fetchSinaFundHistory(code);
        if (history.length) console.log(`新浪源成功 code=${code} count=${history.length}`);
      } catch (e) { errors.push(`sina: ${e.message}`); }
    }
    if (!history.length) return errorResponse(`暂无该基金历史数据 (${errors.join("; ") || "empty"})`, 404);
    const result = { code, history, count: history.length, dateRange: `${history[0].date}~${history[history.length-1].date}` };
    setCache(ck, result);
    return jsonResponse({ code: 0, data: result, msg: "ok" });
  }

  // ─── 7. 微信涨跌订阅登记 ──────────────────────────────
  if (path === "/api/wx/subscribe" && method === "POST") {
    const body = await postBody();
    const jsCode = body.code || "";
    const fundCode = body.fund_code || "";
    const fundName = (body.fund_name || fundCode).slice(0, 20);
    if (!jsCode || !fundCode) return errorResponse("missing code or fund_code", 400);
    const wxSecret = env.WX_SECRET || "";
    if (!wxSecret) return errorResponse("WX_SECRET not configured", 500);
    try {
      const tokenUrl = buildUrl("https://api.weixin.qq.com/sns/jscode2session", {
        appid: WX_APPID, secret: wxSecret, js_code: jsCode, grant_type: "authorization_code",
      });
      const d = await fetchJson(tokenUrl);
      const openid = d.openid;
      if (!openid) return errorResponse(`wx login failed: ${d.errmsg || "unknown"}`, 400);
      if (!_wx_subs[fundCode]) _wx_subs[fundCode] = [];
      const subs = _wx_subs[fundCode];
      if (!subs.some(s => s.openid === openid)) {
        subs.push({ openid, name: fundName, ts: Math.floor(Date.now() / 1000) });
        await saveWxSubs(env);
      }
      return jsonResponse({ code: 0, data: { ok: true, subscribers: subs.length }, msg: "ok" });
    } catch (e) { return errorResponse(`微信登录失败: ${e.message}`, 502); }
  }

  // ─── 8. 每日推送任务 ──────────────────────────────────
  if (path === "/api/jobs/daily_notify" && method === "GET") {
    const key = gp("key");
    if (!WX_NOTIFY_KEY || key !== WX_NOTIFY_KEY) return errorResponse("bad key", 403);
    const wxSecret = env.WX_SECRET || "";
    if (!wxSecret) return errorResponse("WX_SECRET not configured", 500);

    const targets = Object.entries(_wx_subs).filter(([, subs]) => subs.length > 0);
    if (!targets.length) return jsonResponse({ code: 0, data: { sent: 0, failed: 0, msg: "no subscribers" }, msg: "ok" });

    let sent = 0, failed = 0;
    const errors = [];
    try {
      const tokenUrl = buildUrl("https://api.weixin.qq.com/cgi-bin/token", {
        grant_type: "client_credential", appid: WX_APPID, secret: wxSecret,
      });
      const td = await fetchJson(tokenUrl);
      if (!td.access_token) return jsonResponse({ code: -1, data: { sent: 0, failed: 0, errors: [`token error: ${td.errmsg}`] }, msg: "token error" });
      const token = td.access_token;

      for (const [fundCode, subs] of targets) {
        try {
          const navs = await fetchSinaFundHistory(fundCode, 10);
          if (navs.length < 2) throw new Error("nav data too short");
          const latest = navs[navs.length - 1], prev = navs[navs.length - 2];
          const chg = (latest.nav - prev.nav) / prev.nav * 100;
          const name = subs[0].name.slice(0, 10);
          const content = `${name} ${chg >= 0 ? "涨" : "跌"}${Math.abs(chg).toFixed(2)}%`.slice(0, 20);

          for (const s of subs) {
            try {
              const sendUrl = `https://api.weixin.qq.com/cgi-bin/message/subscribe/send?access_token=${token}`;
              const resp = await fetch(sendUrl, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                  touser: s.openid, template_id: WX_TEMPLATE_ID,
                  page: `pages/detail/detail?code=${fundCode}`,
                  data: {
                    thing1: { value: name },
                    thing2: { value: `${chg >= 0 ? "涨" : "跌"}${Math.abs(chg).toFixed(2)}%` },
                    time3: { value: latest.date },
                    number4: { value: latest.nav.toFixed(4) },
                  },
                }),
              });
              const rd = await resp.json();
              if (rd.errcode === 0) sent++;
              else { failed++; errors.push(`${fundCode}/${s.openid.slice(0,8)}: errcode=${rd.errcode} ${rd.errmsg}`); }
            } catch (e) { failed++; errors.push(`${fundCode} send: ${e.message}`); }
          }
          _wx_subs[fundCode] = [];
        } catch (e) { errors.push(`${fundCode} nav: ${e.message}`); failed += subs.length; }
      }
      await saveWxSubs(env);
    } catch (e) { return errorResponse(`daily_notify error: ${e.message}`, 500); }
    return jsonResponse({ code: 0, data: { sent, failed, errors: errors.slice(0, 10) }, msg: "ok" });
  }

  // ─── 9. 用户反馈提交 ──────────────────────────────────
  if (path === "/api/feedback" && method === "POST") {
    const body = await postBody();
    const text = (body.text || "").trim();
    if (!text) return errorResponse("反馈内容不能为空", 400);
    const entry = {
      text: text.slice(0, 2000),
      version: (body.version || "").slice(0, 50),
      contact: (body.contact || "").slice(0, 200),
      ts: Math.floor(Date.now() / 1000),
      time: beijingDateTimeStr(),
    };
    _feedbacks.push(entry);
    await saveFeedbacks(env);
    return jsonResponse({ code: 0, data: { ok: true }, msg: "ok" });
  }

  // ─── 10. 反馈列表 ─────────────────────────────────────
  if (path === "/api/feedback/list" && method === "GET") {
    if (gp("key") !== ADMIN_KEY) return errorResponse("bad key", 403);
    return jsonResponse({
      code: 0, msg: "ok",
      data: { count: _feedbacks.length, items: _feedbacks.slice(-200) },
    });
  }

  // ─── 11. 反馈导出 ─────────────────────────────────────
  if (path === "/api/feedback/export" && method === "GET") {
    if (gp("key") !== ADMIN_KEY) return errorResponse("bad key", 403);
    return new Response(JSON.stringify(_feedbacks, null, 2), {
      headers: {
        "Content-Type": "application/json",
        "Content-Disposition": "attachment; filename=feedbacks.json",
        "Access-Control-Allow-Origin": "*",
      },
    });
  }

  // ─── 12. 基金实时估值批量 ─────────────────────────────
  if (path === "/api/fund/realtime" && method === "GET") {
    const codesRaw = gp("codes") || "";
    const codeList = [];
    const seen = new Set();
    for (const c of codesRaw.split(",")) {
      const cc = c.trim();
      if (/^\d{6}$/.test(cc) && !seen.has(cc)) { seen.add(cc); codeList.push(cc); }
    }
    if (!codeList.length) return jsonResponse({ code: 0, data: { items: [] }, msg: "ok" });

    const results = {};
    const todo = [];
    for (const c of codeList.slice(0, 50)) {
      const hit = getCache(`fund_rt_${c}`, REALTIME_TTL);
      if (hit != null) results[c] = hit;
      else todo.push(c);
    }
    if (todo.length) {
      const fetched = await Promise.allSettled(todo.map(c => fetchFundRealtimeOne(c)));
      for (let i = 0; i < fetched.length; i++) {
        const r = fetched[i];
        if (r.status === "fulfilled") {
          const item = r.value;
          if (item.code) {
            results[item.code] = item;
            if (!item.error) setCache(`fund_rt_${item.code}`, item);
          }
        }
      }
    }
    const items = codeList.filter(c => results[c]).map(c => results[c]);
    return jsonResponse({ code: 0, data: { items }, msg: "ok" });
  }

  // ─── 13. 基金批量收益率 ───────────────────────────────
  if (path === "/api/fund/returns" && method === "GET") {
    const ck = "fund_returns_batch";
    const cached = getCache(ck, 3600);
    if (cached) return jsonResponse({ code: 0, data: cached, msg: "ok" });
    try {
      const result = await fetchFundReturnsBatch();
      if (!Object.keys(result).length) return errorResponse("基金排行数据解析失败", 502);
      setCache(ck, result);
      return jsonResponse({ code: 0, data: result, msg: "ok" });
    } catch (e) { return errorResponse(`基金排行数据获取失败: ${e.message}`, 502); }
  }

  // ─── 14. 基金分时估值 ─────────────────────────────────
  if (path === "/api/fund/estimate/chart" && method === "GET") {
    const code = gp("code");
    if (!code || !/^\d{6}$/.test(code)) return errorResponse("基金代码格式错误", 400);
    estRollover();
    _est_watch.delete(code);
    _est_watch.set(code, Date.now() / 1000);
    while (_est_watch.size > EST_WATCH_MAX) {
      const first = _est_watch.keys().next().value;
      _est_watch.delete(first);
    }

    const trading = inTradeWindow();
    if (_est_unsupported[code]) {
      return jsonResponse({
        code: 0, msg: "ok",
        data: {
          code, date: _est_state.date,
          points: _est_points[code] || [],
          count: (_est_points[code] || []).length,
          latest: null, trading, supported: false,
          source: "重仓股加权模型",
          note: `暂不支持估值（${_est_unsupported[code]}），仅股票型/偏股混合型基金持仓披露充分`,
        },
      });
    }

    try {
      const latest = await estSampleOne(code);
      const pts = _est_points[code] || [];
      if (!latest) {
        if (_est_unsupported[code]) {
          return jsonResponse({
            code: 0, msg: "ok",
            data: {
              code, date: _est_state.date,
              points: pts, count: pts.length,
              latest: null, trading, supported: false,
              source: "重仓股加权模型",
              note: `暂不支持估值（${_est_unsupported[code]}），仅股票型/偏股混合型基金持仓披露充分`,
            },
          });
        }
      }
      const coverage = latest?.coverage;
      const holdingsDate = latest?.holdingsDate || "";
      const coverageNote = coverage ? `，覆盖率约${coverage}%` : "";
      const note = `估值为本程序基于${holdingsDate || "上季"}披露的前十大重仓股实时测算${coverageNote}，可能与实际净值有较大偏差，仅供参考不构成投资建议，以晚间公布净值为准`;
      return jsonResponse({
        code: 0, msg: "ok",
        data: {
          code, date: _est_state.date,
          points: pts, count: pts.length,
          latest: latest ? {
            estimate: latest.estimate, estimateChange: latest.estimateChange,
            estimateTime: latest.estimateTime, coverage: latest.coverage,
            holdingsDate: latest.holdingsDate,
          } : null,
          trading, supported: true,
          source: "重仓股加权模型",
          note,
        },
      });
    } catch (e) {
      return errorResponse(`估值采样失败: ${e.message}`, 500);
    }
  }

  // ─── 15. 基金详情 ─────────────────────────────────────
  if (path === "/api/fund/detail" && method === "GET") {
    const code = gp("code");
    if (!code || !/^\d{6}$/.test(code)) return errorResponse("基金代码格式错误", 400);
    const ck = `fund_detail_${code}`;
    const cached = getCache(ck, DETAIL_TTL);
    if (cached) return jsonResponse({ code: 0, data: cached, msg: "ok" });

    const info = { code };
    try {
      const pzdUrl = buildUrl(EM_PZD_URL.replace("{code}", code), { rt: Date.now() });
      const jbgkUrl = EM_JBGK_URL.replace("{code}", code);
      const [pzdText, jbgkText] = await Promise.allSettled([
        fetchText(pzdUrl, { headers: EM_HEADERS }),
        fetchText(jbgkUrl, { headers: EM_HEADERS }),
      ]);

      if (pzdText.status === "fulfilled") {
        const t = pzdText.value;
        info.name = jsVarStr(t, "fS_name");
        info.m1 = jsVarStr(t, "syl_1y");
        info.m3 = jsVarStr(t, "syl_3y");
        info.m6 = jsVarStr(t, "syl_6y");
        info.y1 = jsVarStr(t, "syl_1n");
        info.feeOriginal = jsVarStr(t, "fund_sourceRate");
        info.feeDiscount = jsVarStr(t, "fund_Rate");
        info.minBuy = jsVarStr(t, "fund_minsg");
        const mgr = jsVarJson(t, "Data_currentFundManager");
        if (Array.isArray(mgr) && mgr.length) {
          info.manager = mgr[0].name || "";
          info.managerWorkTime = mgr[0].workTime || "";
          info.managerFundSize = mgr[0].fundSize || "";
        }
        const scale = jsVarJson(t, "Data_fluctuationScale");
        if (scale?.series?.length && scale.series[scale.series.length - 1].y != null) {
          info.scaleYi = scale.series[scale.series.length - 1].y;
        }
      }
      if (jbgkText.status === "fulfilled") {
        const html = jbgkText.value;
        info.type = jbgkField(html, "基金类型");
        const est = jbgkField(html, "成立日期/规模");
        info.establish = est ? est.split("/")[0].trim() : "";
        info.assetScale = jbgkField(html, "资产规模");
        info.company = jbgkField(html, "基金管理人");
        if (!info.manager) info.manager = jbgkField(html, "基金经理人");
      }
    } catch (e) { console.warn("fund detail error:", e); }

    if (!info.name) return errorResponse("未获取到该基金档案", 404);
    setCache(ck, info);
    return jsonResponse({ code: 0, data: info, msg: "ok" });
  }

  // ─── 16. 基金同类排名 ─────────────────────────────────
  if (path === "/api/fund/rank" && method === "GET") {
    const code = gp("code");
    if (!code || !/^\d{6}$/.test(code)) return errorResponse("基金代码格式错误", 400);
    const ck = `fund_rank_${code}`;
    const cached = getCache(ck, FUND_RANK_TTL);
    if (cached) return jsonResponse({ code: 0, data: cached, msg: "ok" });
    try {
      const pzdUrl = buildUrl(EM_PZD_URL.replace("{code}", code), { rt: Date.now() });
      const text = await fetchText(pzdUrl, { headers: EM_HEADERS, timeoutMs: 10000 });
      const name = jsVarStr(text, "fS_name");
      if (!name) return errorResponse("无该基金数据", 404);
      const rankArr = jsVarJson(text, "Data_rateInSimilarType") || [];
      const pctArr = jsVarJson(text, "Data_rateInSimilarPersent") || [];
      const latestRank = (Array.isArray(rankArr) && rankArr.length) ? rankArr[rankArr.length - 1] : {};
      const latestPct = (Array.isArray(pctArr) && pctArr.length) ? pctArr[pctArr.length - 1] : [];
      let total = null, beat = null;
      try { total = parseInt(latestRank.sc); } catch {}
      try {
        if (Array.isArray(latestPct) && latestPct.length >= 2 && latestPct[1] != null)
          beat = Math.round(parseFloat(latestPct[1]) * 100) / 100;
      } catch {}
      const data = {
        code, name,
        rank: latestRank.y, total,
        beatPercent: beat,
        rankDate: msToDate(latestRank.x || (Array.isArray(latestPct) ? latestPct[0] : 0)),
        ret1y: jsVarStr(text, "syl_1n"), ret6m: jsVarStr(text, "syl_6y"),
        ret3m: jsVarStr(text, "syl_3y"), ret1m: jsVarStr(text, "syl_1y"),
        note: "同类排名为东方财富滚动统计口径，阶段涨幅单位%，仅供参考",
        source: "东方财富",
      };
      setCache(ck, data);
      return jsonResponse({ code: 0, data, msg: "ok" });
    } catch (e) { return errorResponse(`排名数据源请求失败: ${e.message}`, 502); }
  }

  // ─── 17. 股票单只报价 ─────────────────────────────────
  if (path === "/api/stock/quote" && method === "GET") {
    const code = gp("code");
    if (!code || !/^[A-Za-z0-9]{2,12}$/.test(code)) return errorResponse("代码格式错误", 400);
    const ck = `stock_quote_${code}`;
    const cached = getCache(ck, INTRADAY_TTL);
    if (cached) return jsonResponse({ code: 0, data: cached, msg: "ok" });

    if (code.startsWith("gm")) {
      if (!GM_INDEX_MAP[code]) return errorResponse("无该指数代码", 404);
      try {
        const data = await fetchGmQuoteOne(code);
        return jsonResponse({ code: 0, data, msg: "ok" });
      } catch (e) { return errorResponse(e.message, 502); }
    }

    try {
      const text = await fetchTencentQuoteText(code);
      const data = parseTencentQuote(text, code);
      if (!data) return errorResponse("无该代码行情", 404);
      setCache(ck, data);
      return jsonResponse({ code: 0, data, msg: "ok" });
    } catch (e) { return errorResponse(`行情源请求失败: ${e.message}`, 502); }
  }

  // ─── 18. 股票批量报价 ─────────────────────────────────
  if (path === "/api/stock/quotes" && method === "GET") {
    const codesRaw = gp("codes") || "";
    const codeList = [];
    const seen = new Set();
    for (const c of codesRaw.split(",")) {
      const cc = c.trim();
      if (/^[A-Za-z0-9]{2,12}$/.test(cc) && !seen.has(cc)) { seen.add(cc); codeList.push(cc); }
    }
    if (!codeList.length) return jsonResponse({ code: 0, data: { items: [] }, msg: "ok" });

    const results = {};
    const todo = [];
    for (const c of codeList.slice(0, 50)) {
      const hit = getCache(`stock_quote_${c}`, INTRADAY_TTL);
      if (hit != null) results[c] = hit;
      else todo.push(c);
    }

    const gmTodo = todo.filter(c => c.startsWith("gm"));
    const txTodo = todo.filter(c => !c.startsWith("gm"));

    if (gmTodo.length) {
      try {
        const gmQuotes = await fetchGmQuotes();
        for (const c of gmTodo) { if (gmQuotes[c]) results[c] = gmQuotes[c]; }
      } catch {}
    }

    if (txTodo.length) {
      try {
        const text = await fetchTencentQuoteText(txTodo.join(","));
        const parsed = parseTencentQuotesBatch(text);
        for (const [c, data] of Object.entries(parsed)) {
          results[c] = data;
          setCache(`stock_quote_${c}`, data);
        }
      } catch (e) { console.warn("tencent quotes batch error:", e); }
    }

    const items = codeList.filter(c => results[c]).map(c => results[c]);
    return jsonResponse({ code: 0, data: { items }, msg: "ok" });
  }

  // ─── 19. 股票K线 ──────────────────────────────────────
  if (path === "/api/stock/kline" && method === "GET") {
    const code = gp("code");
    const count = Math.min(Math.max(parseInt(gp("count")) || 320, 10), 800);
    if (!code || !/^[A-Za-z0-9]{2,12}$/.test(code)) return errorResponse("代码格式错误", 400);
    const ck = `stock_kline_${code}_${count}`;
    const cached = getCache(ck, HISTORY_TTL);
    if (cached) return jsonResponse({ code: 0, data: cached, msg: "ok" });

    if (code.startsWith("gm")) {
      if (!GM_INDEX_MAP[code]) return errorResponse("无该指数代码", 404);
      try {
        const kline = await fetchGmKline(code, count);
        const result = { code, kline, count: kline.length };
        setCache(ck, result);
        return jsonResponse({ code: 0, data: result, msg: "ok" });
      } catch (e) { return errorResponse(e.message, 404); }
    }

    try {
      const url = buildUrl(TENCENT_KLINE_URL, { param: `${code},day,,${count},qfq` });
      // 修正: 腾讯K线 param 格式: {code},day,,,{count},qfq
      const url2 = buildUrl(TENCENT_KLINE_URL, { param: `${code},day,,,${count},qfq` });
      const raw = await fetchJson(url2, { timeoutMs: 15000 });
      const node = raw?.data?.[code] || {};
      const rows = node.day || node.qfqday || [];
      const kline = [];
      for (const r of rows) {
        try {
          kline.push({
            date: String(r[0]).slice(0, 10),
            open: parseFloat(r[1]), close: parseFloat(r[2]),
            high: parseFloat(r[3]), low: parseFloat(r[4]),
            volume: r.length > 5 ? parseFloat(r[5]) : 0,
          });
        } catch {}
      }
      if (!kline.length) return errorResponse("无K线数据", 404);
      const result = { code, kline, count: kline.length };
      setCache(ck, result);
      return jsonResponse({ code: 0, data: result, msg: "ok" });
    } catch (e) { return errorResponse(`K线源请求失败: ${e.message}`, 502); }
  }

  // ─── 20. 市场温度 ─────────────────────────────────────
  if (path === "/api/market/temperature" && method === "GET") {
    const cacheTtl = isTradingTime() ? 60 : 300;
    const ck = "market_temperature";
    const cached = getCache(ck, cacheTtl);
    if (cached) return jsonResponse({ code: 0, data: cached, msg: "ok" });

    const today = beijingDateStr().replace(/-/g, "");
    const zdfbUrl = buildUrl(EM_ZDFB_URL, { ...EM_ZDFB_PARAMS, date: today });
    try {
      const [zdfbRaw, quoteText] = await Promise.all([
        fetchJson(zdfbUrl, { headers: { "User-Agent": BROWSER_UA, "Referer": "https://quote.eastmoney.com/" }, timeoutMs: 10000 }),
        fetchTencentQuoteText("sh000001,sz399001").catch(() => ""),
      ]);

      // 涨跌家数
      const fb = zdfbRaw?.data?.fenbu || [];
      const qdate = String(zdfbRaw?.data?.qdate || "");
      let up = 0, down = 0, flat = 0, limitUp = 0, limitDown = 0;
      for (const cell of fb) {
        for (const [k, v] of Object.entries(cell)) {
          const n = parseInt(k), c = parseInt(v);
          if (isNaN(n) || isNaN(c)) continue;
          if (n >= 11) { limitUp += c; up += c; }
          else if (n <= -11) { limitDown += c; down += c; }
          else if (n > 0) up += c;
          else if (n < 0) down += c;
          else flat += c;
        }
      }
      const totalStocks = up + down + flat;
      if (!totalStocks) return errorResponse("涨跌分布数据为空", 502);

      // 成交额
      let shAmt = null, szAmt = null;
      if (quoteText) {
        const quotes = {};
        const re = /v_([A-Za-z0-9]+)="([^"]*)"/g;
        let m;
        while ((m = re.exec(quoteText)) !== null) quotes[m[1]] = m[2];
        const amtWan = (cd) => { const f = (quotes[cd] || "").split("~"); return f.length > 37 && f[37] ? parseFloat(f[37]) : null; };
        shAmt = amtWan("sh000001");
        szAmt = amtWan("sz399001");
      }

      const totalYi = (shAmt != null && szAmt != null) ? Math.round((shAmt + szAmt) / 10000 * 10) / 10 : null;
      const dateStr = qdate.length === 8 ? `${qdate.slice(0,4)}-${qdate.slice(4,6)}-${qdate.slice(6,8)}` : "";

      let prevYi = null, chgPct = null;
      if (totalYi != null && totalYi > 0 && dateStr) {
        prevYi = await fetchPrevAmountYi(dateStr, env);
        if (prevYi == null) {
          const prevDays = Object.keys(_amount_hist).filter(d => d < dateStr);
          if (prevDays.length) prevYi = _amount_hist[prevDays.sort().pop()];
        }
        if (prevYi) chgPct = Math.round((totalYi - prevYi) / prevYi * 1000) / 10;
        _amount_hist[dateStr] = totalYi;
        const keys = Object.keys(_amount_hist).sort();
        while (keys.length > 10) { delete _amount_hist[keys.shift()]; }
        await saveAmountHist(env);
      }

      const data = {
        date: dateStr, up, down, flat, limitUp, limitDown,
        upRatio: Math.round(up / totalStocks * 1000) / 10,
        amountYi: totalYi,
        shAmountYi: shAmt != null ? Math.round(shAmt / 10000 * 10) / 10 : null,
        szAmountYi: szAmt != null ? Math.round(szAmt / 10000 * 10) / 10 : null,
        prevAmountYi: prevYi,
        amountChgPct: chgPct,
        note: "涨停/跌停按涨跌幅≥10%/≤-10%统计（东财分布口径）；成交额为沪深合计，仅供参考",
        source: "东方财富/腾讯",
      };
      setCache(ck, data);
      return jsonResponse({ code: 0, data, msg: "ok" });
    } catch (e) { return errorResponse(`市场温度数据获取失败: ${e.message}`, 502); }
  }

  // ─── 21. 指数估值 ─────────────────────────────────────
  if (path === "/api/index/valuation" && method === "GET") {
    const ck = "index_valuation";
    const cached = getCache(ck, VALUATION_TTL);
    if (cached) return jsonResponse({ code: 0, data: cached, msg: "ok" });
    try {
      const raw = await fetchJson(DANJUAN_VALUATION_URL, {
        headers: { "User-Agent": BROWSER_UA, "Referer": "https://danjuanfunds.com/" },
        timeoutMs: 10000,
      });
      const itemsRaw = raw?.data?.items || [];
      if (!itemsRaw.length) return errorResponse("估值数据为空", 502);

      const nameMap = Object.fromEntries(VALUATION_WHITELIST);
      const order = Object.fromEntries(VALUATION_WHITELIST.map(([c], i) => [c, i]));
      const items = [];
      let latestTs = 0;

      for (const it of itemsRaw) {
        const c = it.index_code;
        if (!nameMap[c]) continue;
        try { latestTs = Math.max(latestTs, parseInt(it.ts) || 0); } catch {}
        const pe = it.pe || 0;
        const pb = it.pb || 0;
        items.push({
          code: c, name: nameMap[c],
          pe: pe ? Math.round(pe * 100) / 100 : null,
          pePercentile: pe ? Math.round((it.pe_percentile || 0) * 1000) / 10 : null,
          pb: pb ? Math.round(pb * 1000) / 1000 : null,
          pbPercentile: pb ? Math.round((it.pb_percentile || 0) * 1000) / 10 : null,
          roe: it.roe ? Math.round(it.roe * 10000) / 100 : null,
          dividendYield: it.yeild ? Math.round(it.yeild * 10000) / 100 : null,
          evaType: it.eva_type || "",
        });
      }
      items.sort((a, b) => (order[a.code] || 99) - (order[b.code] || 99));
      if (!items.length) return errorResponse("估值数据无白名单指数", 502);

      const data = {
        date: msToDate(latestTs),
        items,
        evaTypeMap: { low: "偏低", mid: "适中", high: "偏高" },
        note: "百分位为历史分位（蛋卷口径），估值仅供参考，不构成投资建议",
        source: "蛋卷基金",
      };
      setCache(ck, data);
      return jsonResponse({ code: 0, data, msg: "ok" });
    } catch (e) { return errorResponse(`估值数据源请求失败: ${e.message}`, 502); }
  }

  // ─── 22. 板块列表 ─────────────────────────────────────
  if (path === "/api/sector/list" && method === "GET") {
    const type = gp("type");
    const board = SECTOR_BOARD_MAP[type];
    if (!board) return errorResponse("type 仅支持 industry / industry2 / concept", 400);
    const ck = `sector_list_${type}`;
    const cached = getCache(ck, REALTIME_TTL);
    if (cached) return jsonResponse({ code: 0, data: cached, msg: "ok" }, 200, { "Cache-Control": "public, max-age=60" });

    const pages = type !== "concept" ? [[0, 200]] : [[0, 200], [200, 200]];
    async function onePage(offset, count) {
      const url = buildUrl(TENCENT_SECTOR_RANK_URL, {
        board_type: board, sort_type: "PriceRatio",
        direct: "down", offset, count,
      });
      const raw = await fetchJson(url, { headers: { "User-Agent": BROWSER_UA }, timeoutMs: 10000 });
      if (raw.code !== 0) throw new Error(`板块榜上游错误 code=${raw.code}`);
      return raw?.data?.rank_list || [];
    }

    try {
      const parts = await Promise.allSettled(pages.map(([o, c]) => onePage(o, c)));
      const rows = [];
      const seenCodes = new Set();
      for (const part of parts) {
        if (part.status !== "fulfilled") continue;
        for (const it of part.value) {
          const code = String(it.code || "");
          const name = String(it.name || "");
          if (!code || !name || seenCodes.has(code)) continue;
          seenCodes.add(code);
          const lzg = it.lzg || {};
          const zgb = String(it.zgb || "");
          let upStr = "", downStr = "";
          if (zgb.includes("/")) { [upStr, downStr] = zgb.split("/", 2); }
          rows.push({
            code, name,
            price: fnum(it.zxj), change: fnum(it.zd), changePct: fnum(it.zdf),
            turnover: fnum(it.hsl),
            leadStock: String(lzg.name || ""), leadStockCode: String(lzg.code || ""),
            leadStockChangePct: fnum(lzg.zdf),
            upCount: upStr, downCount: downStr,
          });
        }
      }
      if (!rows.length) return errorResponse("板块榜上游无数据", 502);
      const data = {
        type, items: rows, count: rows.length,
        time: beijingDateTimeStr(), source: "腾讯财经",
      };
      setCache(ck, data);
      return jsonResponse({ code: 0, data, msg: "ok" }, 200, { "Cache-Control": "public, max-age=60" });
    } catch (e) { return errorResponse(`板块榜请求失败: ${e.message}`, 502); }
  }

  // ─── 23. 板块成份股 ───────────────────────────────────
  if (path === "/api/sector/stocks" && method === "GET") {
    const code = gp("code");
    const offset = Math.min(Math.max(parseInt(gp("offset")) || 0, 0), 2000);
    const count = Math.min(Math.max(parseInt(gp("count")) || 50, 1), 100);
    if (!code || !/^pt[A-Za-z0-9]{1,16}$/.test(code)) return errorResponse("板块代码格式错误", 400);
    const ck = `sector_stocks_${code}_${offset}_${count}`;
    const cached = getCache(ck, REALTIME_TTL);
    if (cached) return jsonResponse({ code: 0, data: cached, msg: "ok" }, 200, { "Cache-Control": "public, max-age=60" });

    try {
      const url = buildUrl(TENCENT_SECTOR_STOCKS_URL, {
        board_code: code, sort_type: "PriceRatio",
        direct: "down", offset, count,
      });
      const raw = await fetchJson(url, { headers: { "User-Agent": BROWSER_UA }, timeoutMs: 10000 });
      if (raw.code !== 0) return errorResponse(`成份股上游错误 code=${raw.code}`, 502);
      const d = raw.data || {};
      const items = [];
      for (const it of (d.rank_list || [])) {
        const scode = String(it.code || "");
        const name = String(it.name || "");
        if (!scode || !name) continue;
        items.push({
          code: scode, name,
          price: fnum(it.zxj), change: fnum(it.zd), changePct: fnum(it.zdf),
          turnover: fnum(it.hsl), volRatio: fnum(it.lb),
          pe: fnum(it.pe_ttm), circCapYi: fnum(it.ltsz), totalCapYi: fnum(it.zsz),
        });
      }
      const data = {
        code, items, total: d.total || items.length,
        offset, count, time: beijingDateTimeStr(), source: "腾讯财经",
      };
      setCache(ck, data);
      return jsonResponse({ code: 0, data, msg: "ok" }, 200, { "Cache-Control": "public, max-age=60" });
    } catch (e) { return errorResponse(`成份股上游请求失败: ${e.message}`, 502); }
  }

  // ─── 404 ──────────────────────────────────────────────
  return errorResponse("Not Found", 404);
}

// ═══════════════════════════════════════════════════════════
// Worker 入口
// ═══════════════════════════════════════════════════════════
let _stateLoaded = false;

export default {
  async fetch(request, env, ctx) {
    // CORS 预检
    if (request.method === "OPTIONS") return handleCors();

    // 限流
    const ip = request.headers.get("CF-Connecting-IP") || "unknown";
    if (!checkRateLimit(ip)) {
      return jsonResponse({ code: -1, msg: "请求过于频繁，请稍后再试" }, 429);
    }

    // 懒加载 KV 状态
    if (!_stateLoaded) {
      _stateLoaded = true;
      try { await loadState(env); } catch (e) { console.warn("loadState failed:", e); }
    }

    try {
      return await handleRequest(request, env);
    } catch (e) {
      console.error("Unhandled error:", e);
      return errorResponse(`Internal error: ${e.message}`, 500);
    }
  },
};
