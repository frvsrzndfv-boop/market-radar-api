# 行情雷达 API — Cloudflare Workers 部署指引

> 完整迁移自 FastAPI 后端 v2.5.0，22 个端点、8+ 数据源透传，零服务器依赖。

---

## 📋 功能概览

| 模块 | 端点数 | 数据源 |
|------|--------|--------|
| 分时数据 | 2 | 腾讯、Gate.io |
| 基金数据 | 6 | 东财、新浪、天天基金、蛋卷 |
| 股票行情 | 3 | 腾讯、东财（全球指数）|
| 板块数据 | 2 | 腾讯财经 |
| 市场温度 | 1 | 东财 + 同花顺 + 搜狐 |
| 微信推送 | 2 | 微信 API |
| 管理接口 | 3 | 微信 API |
| 反馈系统 | 2 | KV 存储 |

---

## 🚀 快速部署（手机友好）

### 方式一：Wrangler CLI 部署（推荐）

#### 1. 安装 Node.js 和 Wrangler

```bash
# 在电脑终端执行（需先安装 Node.js 18+）
npm install -g wrangler
```

#### 2. 登录 Cloudflare

```bash
wrangler login
```

浏览器会弹出授权页面，点击确认即可。

#### 3. 创建 KV 命名空间

```bash
wrangler kv:namespace create KV
```

输出类似：
```
✨ Success!
Add the following to your configuration file in your kv_namespaces section:
{ binding = "KV", id = "abc123..." }
```

**记下输出的 `id` 值**，替换 `wrangler.toml` 中的 `REPLACE_ON_DEPLOY`。

#### 4. 设置环境变量

```bash
wrangler secret put WX_SECRET
# 粘贴微信小程序的 AppSecret

wrangler secret put ADMIN_KEY  
# 输入：AOQIQlKNvJDcYl90-Mb_pQ
```

#### 5. 部署

```bash
wrangler deploy
```

部署成功后会显示一个 `https://xxx.workers.dev` 的 URL，就是你的 API 地址。

---

### 方式二：Dashboard 直接部署（无需安装）

#### 1. 登录 Cloudflare Dashboard

打开 https://dash.cloudflare.com → 左侧菜单 **Workers & Pages**

#### 2. 创建 Worker

- 点击 **Create** → 输入名称 `market-radar-api` → 点击 **Deploy**
- 点击 **Edit code**

#### 3. 粘贴代码

- 复制 `src/worker.js` 的全部内容
- 粘贴到编辑器中替换默认代码
- 点击右上角 **Save and deploy**

#### 4. 创建 KV 命名空间

- 返回 Workers 主页 → **KV** → **Create a namespace**
- 名称填 `market_radar_kv` → 确认创建

#### 5. 绑定 KV 到 Worker

- 进入 Worker 设置页 → **Settings** → **Bindings**
- 点击 **Add binding**
- Type: **KV Namespace**
- Variable name: `KV`
- KV namespace: 选择刚才创建的 `market_radar_kv`
- 保存

#### 6. 设置环境变量

- 进入 Worker → **Settings** → **Variables**
- 添加以下变量（建议 Encrypt）：

| 变量名 | 值 | 是否加密 |
|--------|-----|---------|
| `WX_SECRET` | 你的微信小程序 AppSecret | ✅ 是 |
| `ADMIN_KEY` | `AOQIQlKNvJDcYl90-Mb_pQ` | ✅ 是 |

---

## 🔧 配置说明

### wrangler.toml 关键字段

```toml
name = "market-radar-api"           # Worker 名称
main = "src/worker.js"              # 入口文件
compatibility_date = "2026-01-01"   # 兼容日期

[[kv_namespaces]]
binding = "KV"                      # 代码中通过 env.KV 访问
id = "REPLACE_ON_DEPLOY"           # 替换为实际 KV namespace ID
```

### 环境变量说明

| 变量 | 必填 | 说明 |
|------|------|------|
| `WX_SECRET` | 是 | 微信小程序密钥，推送功能依赖 |
| `ADMIN_KEY` | 否 | 管理接口密钥，不设置则 metrics/feedback/list 等不可用 |

### 已硬编码的配置（无需修改）

```
WX_APPID = wx7f5552fc52317b2a
WX_TEMPLATE_ID = PUqStjuTo2xby_vdpIZas_VpvZUnUHnmwQtuMwr34wA
ADMIN_KEY = AOQIQlKNvJDcYl90-Mb_pQ（也可通过环境变量覆盖）
WX_NOTIFY_KEY = radar_notify_2026
```

---

## 📡 端点列表

### 公开接口（无需认证）

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/health` | GET | 健康检查 |
| `/api/intraday/stock?code=sh000001` | GET | 股票/指数分时 |
| `/api/intraday/crypto?symbol=BTC` | GET | 加密货币分时 |
| `/api/fund/history?code=005967` | GET | 基金历史净值 |
| `/api/fund/realtime?codes=005967,110011` | GET | 基金实时估值（批量） |
| `/api/fund/returns` | GET | 基金批量收益率 |
| `/api/fund/estimate/chart?code=005967` | GET | 基金分时估值 |
| `/api/fund/detail?code=005967` | GET | 基金详情 |
| `/api/fund/rank?code=005967` | GET | 基金同类排名 |
| `/api/stock/quote?code=sh000001` | GET | 股票单只报价 |
| `/api/stock/quotes?codes=sh000001,sh600519` | GET | 股票批量报价 |
| `/api/stock/kline?code=sh000001&count=320` | GET | 股票K线 |
| `/api/market/temperature` | GET | 市场温度 |
| `/api/index/valuation` | GET | 指数估值 |
| `/api/sector/list?type=industry` | GET | 板块列表 |
| `/api/sector/stocks?code=pt01801050` | GET | 板块成份股 |
| `/api/wx/subscribe` | POST | 微信订阅登记 |
| `/api/feedback` | POST | 用户反馈 |

### 管理接口（需 ADMIN_KEY）

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/metrics?key=ADMIN_KEY` | GET | 运营指标 |
| `/api/admin/backup_state?key=ADMIN_KEY` | GET | 状态导出 |
| `/api/feedback/list?key=ADMIN_KEY` | GET | 反馈列表 |
| `/api/feedback/export?key=ADMIN_KEY` | GET | 反馈导出 |

### 定时任务接口（需 WX_NOTIFY_KEY）

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/jobs/daily_notify?key=radar_notify_2026` | GET | 每日推送 |

---

## 💾 数据存储

### KV 持久化数据

| Key | 内容 | 说明 |
|-----|------|------|
| `wx_subs` | JSON 对象 | 涨跌订阅登记（openid） |
| `feedbacks` | JSON 数组 | 用户反馈列表 |
| `amount_hist` | JSON 对象 | 成交额日历史 |

### 内存缓存（重启丢失，TTL 各不同）

| 数据类型 | TTL | 说明 |
|----------|-----|------|
| 分时数据 | 60s | 股票/加密货币分时 |
| 基金净值/排行 | 60s | 实时估值 |
| 历史净值 | 30min | 基金历史 |
| 基金详情 | 6h | pingzhongdata |
| 指数估值 | 12h | 蛋卷 |
| 基金排名 | 12h | 同类排名 |
| 持仓数据 | 24h | 估值用 |

---

## ⚠️ 注意事项

1. **Workers 免费版限制**：
   - 每日 10 万次请求
   - 单次请求 CPU 时间 10ms（免费版）/ 50ms（付费版 $5/月）
   - 建议：流量大时升级到付费版

2. **GBK 编码处理**：
   - 腾讯行情接口返回 GBK 编码
   - Workers 使用 `TextDecoder('gbk')` 解码，无需额外处理

3. **KV 写入延迟**：
   - KV 写入是最终一致的，读可能有几秒延迟
   - 对实时性要求高的状态（如内存缓存）仍用 Map

4. **冷启动**：
   - Worker 首次请求时会从 KV 加载状态
   - 后续请求直接从内存读取，极快

5. **CORS**：
   - 默认允许所有来源（`Access-Control-Allow-Origin: *`）
   - 小程序环境不需要严格 CORS

6. **限流**：
   - 每 IP 每分钟最多 80 次请求
   - 超过返回 429 状态码

---

## 🔄 更新部署

### CLI 方式

```bash
# 修改代码后
wrangler deploy
```

### Dashboard 方式

1. 进入 Worker → Edit code
2. 粘贴新代码 → Save and deploy

---

## 📊 监控

### 查看日志

CLI:
```bash
wrangler tail
```

Dashboard:
- Workers → 你的 Worker → **Logs** → **Real-time logs**

### 查看指标

```bash
curl "https://你的域名.workers.dev/api/metrics?key=AOQIQlKNvJDcYl90-Mb_pQ"
```

---

## 🛠️ 故障排查

| 问题 | 原因 | 解决 |
|------|------|------|
| 微信推送失败 | WX_SECRET 未设置 | 设置环境变量 `WX_SECRET` |
| 管理接口 403 | ADMIN_KEY 不匹配 | 检查 URL 参数或环境变量 |
| KV 读写失败 | KV 未绑定 | 检查 Settings → Bindings |
| 腾讯行情乱码 | GBK 解码失败 | 检查 TextDecoder 是否支持 'gbk' |
| 东财数据为空 | 被风控 | 等待几分钟后重试，或换 IP |

---

## 📝 版本历史

- **v2.5.0** — Workers 完整重写，22 端点 + 全球指数
- v2.4.9 — 基金分时估值（重仓股加权模型）
- v2.4.0 — 市场温度 / 基金排名 / 指数估值
- v2.0.0 — 初始版本（FastAPI）

---

## 📞 技术支持

如遇到问题，请检查：
1. Worker 日志（`wrangler tail` 或 Dashboard Logs）
2. KV 绑定是否正确
3. 环境变量是否已加密并正确设置
4. 数据源是否可达（腾讯/东财/蛋卷等）

---

**最后更新**: 2026-09-18
