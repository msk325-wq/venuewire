# Kalshi Public API — Research Notes (verified 2026-07-18)

Verified against the current canonical docs at **https://docs.kalshi.com** (the old
`trading-api.readme.io` content has been superseded; docs.kalshi.com serves `.md`
versions of every page plus machine-readable specs at
`https://docs.kalshi.com/llms.txt` and `https://docs.kalshi.com/asyncapi.yaml`).

Provenance legend used below:
- **[live]** — captured via `curl` against the production API on 2026-07-18 (unauthenticated).
- **[doc]** — quoted from official docs / AsyncAPI spec (not live-captured; WSS requires auth).

Doc pages used (all fetched 2026-07-18):
- https://docs.kalshi.com/ (landing / section index)
- https://docs.kalshi.com/llms.txt (full page index)
- https://docs.kalshi.com/api-reference/market/get-markets.md
- https://docs.kalshi.com/api-reference/events/get-events.md
- https://docs.kalshi.com/api-reference/market/get-series-list.md
- https://docs.kalshi.com/getting_started/rate_limits.md
- https://docs.kalshi.com/getting_started/pagination.md
- https://docs.kalshi.com/getting_started/api_keys.md
- https://docs.kalshi.com/getting_started/quick_start_websockets
- https://docs.kalshi.com/websockets/websocket-connection.md
- https://docs.kalshi.com/websockets/market-ticker.md
- https://docs.kalshi.com/websockets/orderbook-updates.md
- https://docs.kalshi.com/asyncapi.yaml (AsyncAPI 3.0.0, "Kalshi Market Data WebSocket API" v2.0.0)

---

## 1. REST market discovery

### Base URLs [doc, both confirmed live with HTTP 200]

| Env | Host | Notes |
|---|---|---|
| Production (new, dedicated) | `https://external-api.kalshi.com/trade-api/v2` | listed first in current OpenAPI |
| Production (shared / legacy) | `https://api.elections.kalshi.com/trade-api/v2` | still fully supported |
| Demo (new) | `https://external-api.demo.kalshi.co/trade-api/v2` | |
| Demo (shared / legacy) | `https://demo-api.kalshi.co/trade-api/v2` | |

### Auth for market data: **NOT required** [live]

`GET /series`, `GET /events`, `GET /markets`, `GET /markets/{ticker}`,
`GET /markets/{ticker}/orderbook` all return 200 with no auth headers
(`security: []` in the OpenAPI-derived reference pages). Responses are served via
CloudFront with `cache-control: public, max-age=15` and an `x-kalshi-cache-hits`
header [live] — i.e. unauthenticated market data is edge-cached (~15s freshness).
No `x-ratelimit-*` headers were observed on unauthenticated responses [live].

### Endpoints

**`GET /series`** (list) — filter by `category` (e.g. `Economics`), `tags`,
`include_product_metadata`, `include_volume`, `min_updated_ts` ("use this to
efficiently poll for changes"). No auth. Response: `{ "series": [ { ticker,
frequency, title, category, tags, settlement_sources, contract_url,
contract_terms_url, fee_type, fee_multiplier, additional_prohibitions,
volume_fp, last_updated_ts } ] }`. [doc + live]

**`GET /series/{series_ticker}`** — single series, e.g.
`/series/KXFED` → 200 [live].

**`GET /events`** — params: `limit` (1–200, default 200), `cursor`,
`with_nested_markets` (bool, default false), `with_milestones` (bool),
`status` (enum: `unopened|open|closed|settled`), `series_ticker`, `tickers`
(comma-separated event tickers), `min_close_ts`, `min_updated_ts`. No auth.
Response: `{ events: [...], milestones: [...], cursor }`. Event fields [live]:
`event_ticker`, `series_ticker`, `title`, `sub_title`, `category`,
`collateral_return_type`, `mutually_exclusive`, `strike_period`,
`settlement_sources`, `last_updated_ts` (RFC3339 string), `available_on_brokers`.

**`GET /markets`** — params: `limit` (default 100, max 1000), `cursor`,
`event_ticker`, `series_ticker`, `status`
(`unopened|open|paused|closed|settled`), `tickers`, `min/max_created_ts`,
`min/max_close_ts`, `min/max_settled_ts`, `min_updated_ts`, `mve_filter`
(`only|exclude`). No auth. Docs say `series_ticker` "requires
`mve_filter=exclude`", but live probing shows `series_ticker` alone works;
`mve_filter=exclude` just excludes multivariate (combo) markets. [doc + live]

**`GET /markets/{ticker}`** and **`GET /markets/{ticker}/orderbook?depth=N`** —
single market and REST orderbook snapshot, no auth [live]. There is also
`GET /markets/orderbooks` (batch, `get-multiple-market-orderbooks.md`) and
candlestick endpoints (see llms.txt index).

### Series tickers for FED / CPI [live]

Series now come in legacy and `KX`-prefixed variants, both live:
`FED` and `KXFED` (title "Fed funds rate"), `CPI` / `KXCPI` (title "CPI"),
plus many related (`KXFEDDECISION`, `KXCPIYOY`, `KXCPICORE`, `KXCPINDEX`, ...).
Current active markets live under the KX series (e.g. event `KXFED-27APR`,
market `KXFED-27APR-T4.25`). Config should use `KXFED`, `KXCPI` (and optionally
legacy `FED`, `CPI` for old markets).

### Pagination [doc, confirmed live]

Cursor-based: pass `limit`, read `cursor` from the response, pass it back as
`?cursor=...`; stop when `cursor` is null/absent. (Pagination guide says
default limit 100, "most list endpoints accept 1–100", but `/events` allows
200 and `/markets` allows 1000 per their own reference pages.)

### Rate limits [doc: getting_started/rate_limits.md]

Token-bucket per second, tiered (event-contract endpoints):

| Tier | Read tokens/s | Write tokens/s |
|---|---|---|
| Basic | 200 | 100 |
| Advanced | 300 | 300 |
| Expert | 600 | 600 |
| Premier | 1000 | 1000 |
| Paragon | 2000 | 2000 |
| Prime | 4000 | 4000 |
| Prestige | 6000 | 8000 |

Most requests cost **10 tokens** (so Basic ≈ 20 reads/s). Batch endpoints charge
per item. Docs do not state a limit for *unauthenticated* requests; the
CloudFront 15s cache absorbs polling. Basic→Advanced is a self-serve upgrade
endpoint; higher tiers are volume-based.

### Live REST sample payloads [live, 2026-07-18]

`GET https://api.elections.kalshi.com/trade-api/v2/markets?series_ticker=KXFED&status=open&limit=2`
(one market object, abridged only by removing the second element):

```json
{
  "cursor": "CgwIj66QxwYQsOCSqgMSEUtYRkVELTI3QVBSLVQ0LjI1",
  "markets": [
    {
      "can_close_early": true,
      "close_time": "2027-04-28T17:55:00Z",
      "created_time": "2025-10-06T19:22:55.893694Z",
      "event_ticker": "KXFED-27APR",
      "expected_expiration_time": "2027-04-28T18:05:00Z",
      "expiration_time": "2027-05-05T18:05:00Z",
      "expiration_value": "",
      "floor_strike": 4.25,
      "last_price_dollars": "0.2800",
      "latest_expiration_time": "2027-05-05T18:05:00Z",
      "liquidity_dollars": "0.0000",
      "market_type": "binary",
      "no_ask_dollars": "0.7300",
      "no_bid_dollars": "0.7200",
      "no_sub_title": "Above 4.25%",
      "notional_value_dollars": "1.0000",
      "open_interest_fp": "2020.47",
      "open_time": "2025-10-13T14:00:00Z",
      "previous_price_dollars": "0.2800",
      "previous_yes_ask_dollars": "0.3000",
      "previous_yes_bid_dollars": "0.2800",
      "price_level_structure": "linear_cent",
      "price_ranges": [{ "start": "0.0000", "end": "1.0000", "step": "0.0100" }],
      "result": "",
      "rules_primary": "If the upper bound of the target federal funds rate ... resolves to Yes.",
      "rules_secondary": "This market will expire the first 2:05 PM ET ...",
      "settlement_timer_seconds": 300,
      "status": "active",
      "strike_type": "greater",
      "subtitle": "4.25%",
      "ticker": "KXFED-27APR-T4.25",
      "title": "Will the upper bound of the federal funds rate be above 4.25% following the Fed's Apr 28, 2027 meeting?",
      "updated_time": "2026-04-09T14:07:28.591885Z",
      "volume_24h_fp": "0.00",
      "volume_fp": "10155.43",
      "yes_ask_dollars": "0.2800",
      "yes_ask_size_fp": "20.00",
      "yes_bid_dollars": "0.2700",
      "yes_bid_size_fp": "31.54",
      "yes_sub_title": "Above 4.25%"
    }
  ]
}
```

Note: query filter uses `status=open` but the returned `status` field value is
`"active"` [live] — the filter enum and the response enum differ.

`GET https://api.elections.kalshi.com/trade-api/v2/markets/KXFED-27APR-T4.25/orderbook?depth=3`:

```json
{
  "orderbook_fp": {
    "no_dollars":  [["0.7000", "11.00"], ["0.7100", "25.00"], ["0.7200", "20.00"]],
    "yes_dollars": [["0.1100", "97.22"], ["0.1200", "0.47"], ["0.2700", "31.54"]]
  }
}
```

Levels are `[price_dollars_string, contract_count_fp_string]`, resting **bids**
per side (yes-bids and no-bids; a no-bid at 0.72 == a yes-ask at 0.28).

---

## 2. WebSocket

### URLs [doc: quick_start_websockets, asyncapi.yaml `servers`]

- Production (canonical in AsyncAPI): `wss://external-api-ws.kalshi.com/trade-api/ws/v2`
- Production (shared/legacy, still supported): `wss://api.elections.kalshi.com/trade-api/ws/v2`
- Demo: `wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2` (legacy: `wss://demo-api.kalshi.co/trade-api/ws/v2`)

### Auth: **REQUIRED for the connection, including public channels** [doc + live]

- websocket-connection.md: "Authentication is required to establish the
  connection; include API key headers during the WebSocket handshake." The
  `ticker`/`trade` channels are "public" only in the sense of "no additional
  channel-level authentication beyond the authenticated WebSocket connection".
- **[live]** Unauthenticated WS upgrade attempts to both
  `https://api.elections.kalshi.com/trade-api/ws/v2` and
  `https://external-api-ws.kalshi.com/trade-api/ws/v2` return **HTTP 401**.
- WS error code 9 = "Authentication required — channel requires authenticated
  connection" exists for the private channels (`orderbook_delta`, `fill`,
  `market_positions`, `communications`, `order_group_updates`).

Auth scheme (same as REST) [doc: api_keys.md + quick_start_websockets]:
- Create API key in account settings → RSA private key + Key ID (key shown once).
- Handshake headers:
  - `KALSHI-ACCESS-KEY`: key ID
  - `KALSHI-ACCESS-TIMESTAMP`: Unix time in **milliseconds**
  - `KALSHI-ACCESS-SIGNATURE`: base64 of RSA-PSS(SHA-256, MGF1-SHA256,
    salt length = digest length) signature over the string
    `"{timestamp_ms}" + "GET" + "/trade-api/ws/v2"` (path **without** query params).

### Channels [doc: asyncapi.yaml]

Market data: `ticker`, `trade`, `market_lifecycle_v2`, `multivariate_market_lifecycle`,
`multivariate`, `cfbenchmarks_value`, `pyth_value` — no extra channel-level auth.
Private: `orderbook_delta`, `fill`, `market_positions`, `communications`,
`order_group_updates`, `user_orders`.

Note: **`orderbook_delta` is listed under "Authentication required"** channels in
the AsyncAPI channel description, even though its content (aggregated book) is not
user-specific; deltas caused by *your own* orders carry extra `client_order_id` /
`subaccount` fields.

- `ticker`: market spec **optional** — "omit to receive all markets"; supports
  `market_ticker`/`market_tickers` and `market_id`/`market_ids`; fires "whenever
  any ticker field changes"; supports `send_initial_snapshot: true` to get an
  initial ticker snapshot on subscribe.
- `orderbook_delta`: market spec **required** (`market_ticker` or
  `market_tickers` only; `market_id(s)` not supported). Sends
  `orderbook_snapshot` first, then `orderbook_delta` incrementals. Supports
  `update_subscription` with actions `add_markets` / `delete_markets` /
  `get_snapshot` (get_snapshot re-sends a snapshot without changing the sub).
  Optional `use_yes_price: true` reports no-side levels in yes-leg pricing
  (see "migration plan" below).

### Command / message formats [doc: websocket-connection.md, asyncapi.yaml]

Subscribe (canonical `cmd`/`params` form; `id` is a client-chosen integer ≥ 1,
unique per session):

```json
{ "id": 1, "cmd": "subscribe",
  "params": { "channels": ["ticker"] } }
```

```json
{ "id": 2, "cmd": "subscribe",
  "params": { "channels": ["orderbook_delta"],
              "market_tickers": ["KXFED-27APR-T4.25", "KXCPI-26NOV-..."] } }
```

(The prose page orderbook-updates.md also shows an older
`{"action":"subscribe","channel":...}` shape — the AsyncAPI spec and
websocket-connection.md agree on `cmd`/`params`; use that.)

Confirmation:

```json
{ "id": 1, "type": "subscribed", "msg": { "channel": "orderbook_delta", "sid": 1 } }
```

Unsubscribe: `{ "id": 124, "cmd": "unsubscribe", "params": { "sids": [1, 2] } }`
→ `{ "id": 124, "sid": 2, "seq": 7, "type": "unsubscribed" }`

Update subscription:

```json
{ "id": 124, "cmd": "update_subscription",
  "params": { "sids": [456], "market_tickers": ["NEW-MARKET-1"], "action": "add_markets" } }
```

→ `{ "id": 123, "sid": 456, "seq": 222, "type": "ok", "msg": { "market_tickers": ["..."] } }`
(`ok.msg` contains the full market list after the update).

`{ "id": 3, "cmd": "list_subscriptions" }` lists active subs with sids.

Errors: `{ "id": ..., "type": "error", "msg": { "code": <1..28>, "msg": "..." } }`.

### Example data messages [doc: asyncapi.yaml examples — verbatim, NOT live-captured]

`ticker`:

```json
{
  "type": "ticker",
  "sid": 11,
  "msg": {
    "market_ticker": "FED-23DEC-T3.00",
    "market_id": "9b0f6b43-5b68-4f9f-9f02-9a2d1b8ac1a1",
    "price_dollars": "0.480",
    "yes_bid_dollars": "0.450",
    "yes_ask_dollars": "0.530",
    "volume_fp": "33896.00",
    "open_interest_fp": "20422.00",
    "dollar_volume": 16948,
    "dollar_open_interest": 10211,
    "yes_bid_size_fp": "300.00",
    "yes_ask_size_fp": "150.00",
    "last_trade_size_fp": "25.00",
    "ts": 1669149841,
    "ts_ms": 1669149841000,
    "time": "2022-11-22T20:44:01Z"
  }
}
```

`orderbook_snapshot`:

```json
{
  "type": "orderbook_snapshot",
  "sid": 2,
  "seq": 2,
  "msg": {
    "market_ticker": "FED-23DEC-T3.00",
    "market_id": "9b0f6b43-5b68-4f9f-9f02-9a2d1b8ac1a1",
    "yes_dollars_fp": [["0.0800", "300.00"], ["0.2200", "333.00"]],
    "no_dollars_fp":  [["0.5400", "20.00"],  ["0.5600", "146.00"]]
  }
}
```

(`yes_dollars_fp` / `no_dollars_fp` keys are **absent** when that side of the
book is empty.)

`orderbook_delta`:

```json
{
  "type": "orderbook_delta",
  "sid": 2,
  "seq": 3,
  "msg": {
    "market_ticker": "FED-23DEC-T3.00",
    "market_id": "9b0f6b43-5b68-4f9f-9f02-9a2d1b8ac1a1",
    "price_dollars": "0.960",
    "delta_fp": "-54.00",
    "side": "yes",
    "ts": "2022-11-22T20:44:01Z",
    "ts_ms": 1669149841000
  }
}
```

`trade` (public trades channel): `msg` has `trade_id`, `market_ticker`,
`yes_price_dollars`, `no_price_dollars`, `count_fp`, `taker_side`, `ts`, `ts_ms`.

### Heartbeat / keep-alive [doc: asyncapi.yaml `control_frames` channel]

"Kalshi sends Ping frames (`0x9`) every **10 seconds** with body `heartbeat` to
maintain the connection. Clients should respond with Pong frames (`0xA`).
Clients may also send Ping frames to which Kalshi will respond with Pong."
No JSON-level heartbeat message; it is WebSocket protocol-level. (The venuewire
staleness watchdog can key off server pings: expect one every ~10s.)

### Sequence numbers [doc: asyncapi.yaml]

- Field: **`seq`** (top-level, sibling of `type`/`sid`), integer ≥ 1.
  Description: "Sequential number that should be checked if you want to
  guarantee you received all the messages. Used for snapshot/delta consistency."
- **Scope: per subscription (per `sid`)**, starting at the initial
  `orderbook_snapshot` and incrementing on each delta.
- Present/required on: `orderbook_snapshot`, `orderbook_delta`,
  `unsubscribed`, `ok`, and cfbenchmarks/pyth messages.
- **NOT present on `ticker` or `trade` payloads** (schema `required` is only
  `type`, `sid`, `msg`; no `seq` property exists) — gap detection is only
  possible on the orderbook channel.
- On a detected gap: no explicit doc guidance; recovery options are
  `update_subscription` with `action: "get_snapshot"` (re-sends a snapshot
  without touching the sub) or full resubscribe. Terminal errors requiring
  resubscribe: codes 10 (channel error), 17 (internal error),
  **25 (subscription buffer overflow — outbound buffer exceeded)**.

### WS limits / batching [doc]

Documented qualitatively, no published numbers:
- Error 26: "Subscription market limit exceeded — adding markets would exceed
  the per-subscription market limit".
- Error 27: "Too many requests — the subscription exceeded its command rate limit".
- Error 25: buffer overflow if the client can't keep up (terminal).
- Batching guidance: subscribe with `market_tickers` arrays (one subscription,
  many markets) and grow via `update_subscription`/`add_markets` rather than
  many single-market subscriptions. The `communications` channel additionally
  supports sharding (`shard_factor` 1–100 / `shard_key`) but that is not
  offered for ticker/orderbook.

---

## 3. Price units & schema summary

**Kalshi has migrated from integer cents to dollar-string pricing.** Live REST
responses contain **no** integer-cent fields (`yes_bid`, `yes_ask`, `last_price`
etc. are gone); everything is:

- Prices: decimal **dollar strings**, 4 dp in REST (`"0.2700"`), 3–4 dp in WS
  examples (`"0.480"`), field names suffixed `_dollars`. Tick structure is given
  by `price_level_structure: "linear_cent"` + `price_ranges` (step `"0.0100"`),
  i.e. still 1¢ ticks for these markets, but parse as decimals — sub-cent
  structures are clearly anticipated.
- Sizes / volumes / OI: fixed-point **contract-count strings with 2 decimals**,
  suffixed `_fp` (`"31.54"` contracts — fractional contracts exist, see live
  `open_interest_fp: "2020.47"`). WS `dollar_volume` / `dollar_open_interest`
  are plain integers (whole dollars).
- Orderbook: REST wraps in `orderbook_fp.{yes_dollars,no_dollars}`; WS uses
  `msg.{yes_dollars_fp,no_dollars_fp}`; both are arrays of
  `[price_dollars_string, count_fp_string]`. Both sides are resting bids for
  their leg unless `use_yes_price: true` (WS) converts no-side to yes-leg
  pricing. **Migration plan in asyncapi.yaml: the `use_yes_price` default will
  flip to `true` in a future release and the flag later removed** — build
  no→yes conversion handling now.
- Timestamps:
  - REST: RFC3339 strings with sub-second precision (`created_time`,
    `updated_time`, `open_time`, `close_time`, `last_updated_ts`).
  - WS: `ts_ms` (int64 Unix ms) is canonical; `ts` (seconds int on ticker,
    RFC3339 string on orderbook_delta — inconsistent!) and `time` (RFC3339)
    are **deprecated**. Use `ts_ms` only.

Recommended internal representation for venuewire: parse dollar strings into
integer **tenth-of-cent (or micro-dollar) fixed point**, not f64; parse `_fp`
counts as integer hundredths of a contract.

---

## 4. Surprises vs venuewire spec §5.2

1. **WSS market data REQUIRES auth — spec's "public ticker without auth" assumption is wrong.**
   Live-confirmed: unauthenticated upgrade → HTTP 401 on both WS hosts. Even the
   "public" `ticker`/`trade` channels need an authenticated connection.
   Implement RSA-PSS API-key signing (key via env vars) as the spec's fallback
   clause anticipated: headers `KALSHI-ACCESS-KEY` / `KALSHI-ACCESS-TIMESTAMP`
   (ms) / `KALSHI-ACCESS-SIGNATURE` = base64(RSA-PSS-SHA256(ts + "GET" +
   "/trade-api/ws/v2")). Note: RSA-PSS, **not** HMAC and not Ed25519.
2. **New canonical hosts.** `external-api.kalshi.com` (REST) and
   `external-api-ws.kalshi.com` (WS) are now listed first; the
   `api.elections.kalshi.com` hosts remain supported. Make hosts configurable.
3. **Cents are gone from the API surface.** All prices are dollar strings
   (`*_dollars`), all sizes fixed-point strings (`*_fp`), including fractional
   contracts (e.g. `open_interest_fp: "2020.47"`). Any schema assuming integer
   cents / integer contract counts must be revised.
4. **No sequence numbers on the ticker channel.** `seq` exists only on
   `orderbook_delta`/`orderbook_snapshot` (per-`sid`). Spec §5.1's gap
   detection can only be implemented for the orderbook subscription; for ticker,
   fall back to the staleness watchdog + server pings (every 10s).
5. **Gap recovery has a cheap primitive**: `update_subscription` with
   `action: "get_snapshot"` re-delivers an `orderbook_snapshot` without
   resubscribing — prefer this over the REST-refresh path in §5.1 (REST
   orderbook is also CloudFront-cached up to 15s, so it is *stale by design*
   for gap recovery).
6. **Series universe renamed**: active Fed/CPI markets live under `KX`-prefixed
   series (`KXFED`, `KXCPI`, `KXFEDDECISION`, ...); legacy `FED`/`CPI` series
   still resolve but new events are `KXFED-*`. Config defaults should use the KX tickers.
7. **`use_yes_price` migration**: today no-side orderbook levels are in no-leg
   pricing; Kalshi has announced the default will flip to yes-leg pricing and
   the flag will be removed. Set `use_yes_price` explicitly and handle both.
8. Minor: `/markets?status=open` filter returns markets whose `status` field is
   `"active"` (filter enum ≠ response enum); docs claim `series_ticker`
   requires `mve_filter=exclude` but it works without (that flag only excludes
   multivariate markets); orderbook_delta is formally an auth-required channel
   despite carrying non-user-specific aggregated book data.
