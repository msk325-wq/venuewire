# Polymarket Public API Research (for venuewire connector, spec §5.1 / §5.3)

Date of research: 2026-07-18.
Method: live official docs (docs.polymarket.com) cross-checked with live curl probes and a
short live WebSocket capture. Every doc-sourced claim cites the page URL; every live-captured
payload is marked **[live-captured]**, doc examples are marked **[doc-sourced]**.

Doc pages used:
- https://docs.polymarket.com/quickstart/reference/endpoints (base URLs; points to https://docs.polymarket.com/llms.txt for full index)
- https://docs.polymarket.com/developers/gamma-markets-api/get-markets (Gamma markets/events, filters, pagination)
- https://docs.polymarket.com/developers/CLOB/websocket/wss-overview (WSS URLs, auth, heartbeat)
- https://docs.polymarket.com/developers/CLOB/websocket/market-channel (market channel subscribe format, event types)
- https://docs.polymarket.com/api-reference/markets/get-simplified-markets (CLOB REST simplified markets, cursor pagination)

Base URLs (per https://docs.polymarket.com/quickstart/reference/endpoints):

| Service | Base URL | Auth for market data |
|---|---|---|
| Gamma API (discovery/metadata) | `https://gamma-api.polymarket.com` | none |
| CLOB API (books, prices, market defs) | `https://clob.polymarket.com` | none for reads |
| Data API (positions/holders, not needed for v1) | `https://data-api.polymarket.com` | none |
| CLOB WSS market channel | `wss://ws-subscriptions-clob.polymarket.com/ws/market` | none |

---

## 1. Market discovery

### 1.1 Gamma API (recommended primary source for metadata)

Doc: https://docs.polymarket.com/developers/gamma-markets-api/get-markets

Endpoints:
- `GET https://gamma-api.polymarket.com/markets` (and `/markets/slug/{slug}`)
- `GET https://gamma-api.polymarket.com/events` (and `/events/slug/{slug}`) — events contain their `markets` array
- `GET https://gamma-api.polymarket.com/tags`

Query params (doc-sourced, verified live):
- Pagination: `limit`, `offset` (offset-based, not cursor)
- Filtering: `active=true`, `closed=false`, `slug`, `tag_id`, `related_tags`, `exclude_tag_id`
- Sorting: `order` (one of `volume24hr`, `volume`, `liquidity`, `start_date`, `end_date`, `competitive`, `closed_time`), `ascending=true|false` (default descending)

Live probe **[live-captured]**:

```
curl "https://gamma-api.polymarket.com/markets?limit=1&active=true&closed=false&order=volume24hr&ascending=false"
```

Key fields on a Gamma **market** object (exact names, live-verified):

| Field | Example / notes |
|---|---|
| `id` | `"2063129"` (Gamma-internal numeric id as string) |
| `question` | market title, e.g. `"Will Belete Molla be the next Prime Minister of Ethiopia?"` |
| `conditionId` | `"0x7c97f7315ac9a2e0..."` — the CLOB market key (66-char hex) |
| `clobTokenIds` | **JSON-encoded string**, not an array: `"[\"85367286745...\", \"40069008842...\"]"` — order matches `outcomes` |
| `outcomes` | **JSON-encoded string**: `"[\"Yes\", \"No\"]"` (sports markets use team names, e.g. `"[\"T1\", \"Karmine Corp\"]"`) |
| `outcomePrices` | **JSON-encoded string**: `"[\"0.0035\", \"0.9965\"]"` — decimal strings in 0–1 |
| `description` | free-text resolution rules |
| `endDate` / `endDateIso` | `"2026-06-01T00:00:00Z"` / `"2026-06-01"` (close time) |
| `startDate`, `createdAt`, `updatedAt` | ISO-8601 UTC |
| `active`, `closed`, `archived`, `acceptingOrders` | booleans |
| `enableOrderBook` | true ⇒ tradable on CLOB (filter on this) |
| `orderPriceMinTickSize` | e.g. `0.001` |
| `orderMinSize` | e.g. `5` |
| `negRisk`, `negRiskMarketID` | multi-outcome (neg-risk) grouping |
| `bestBid`, `bestAsk`, `lastTradePrice`, `spread` | numbers (snapshot only; use WSS for live) |
| `volume24hr`, `liquidity`, `volumeNum` | activity metrics for universe pruning |
| `events` | array of parent event objects (`title`, `slug`, `ticker`, `endDate`) |

**Category**: there is NO usable `category` field on Gamma market objects (present but `null`/absent
in live responses). Category comes from **tags**: filter with `tag_id` on `/markets` or `/events`,
and read the `tags` array on **event** objects, e.g. **[live-captured]**
`"tags": [{"id":"64","slug":"esports"}, {"id":"1","slug":"sports"}, ...]`.

Live event probe **[live-captured]**:

```
curl "https://gamma-api.polymarket.com/events?limit=1&active=true&closed=false&order=volume24hr&ascending=false"
```

Event object top-level fields include: `id`, `ticker`, `slug`, `title`, `description`, `startDate`,
`endDate`, `active`, `closed`, `archived`, `liquidity`, `volume`, `tags[]`, `markets[]`,
`negRisk`, `series`. Each entry in `markets[]` is a full market object as above.

Practical discovery flow for venuewire: page `/events?active=true&closed=false&limit=100&offset=N`
(optionally `tag_id=` for the configured universe), then per market parse `clobTokenIds` +
`outcomes` (both need a second `serde_json::from_str` on the string), keep markets with
`enableOrderBook=true && acceptingOrders=true`.

### 1.2 CLOB REST equivalents

Doc: https://docs.polymarket.com/api-reference/markets/get-simplified-markets

- `GET https://clob.polymarket.com/markets/{condition_id}` — single market **[live-verified]**.
  Snake_case fields: `condition_id`, `question_id`, `question`, `description`, `market_slug`,
  `end_date_iso` (`"2026-06-01T00:00:00Z"`), `active`, `closed`, `archived`, `accepting_orders`,
  `minimum_order_size`, `minimum_tick_size`, `neg_risk`, `tags` (plain string array, e.g.
  `["Politics","Elections","Global Elections","Ethiopia","Main Election"]`), and
  `tokens: [{"token_id": "...", "outcome": "Yes", "price": 0.0035, "winner": false}, {"token_id": "...", "outcome": "No", "price": 0.9965, "winner": false}]`.
- `GET https://clob.polymarket.com/simplified-markets?next_cursor=` — bulk listing,
  **cursor pagination** (`next_cursor` base64, `"LTE="` = end; live response returned
  `limit:1000, count:1000, next_cursor:"MTAwMA=="`) **[live-captured]**. NOTE: it returns ALL
  markets including `closed:true` ones with no active/closed filter params — Gamma is much better
  for filtered discovery; use CLOB REST mainly for book/price snapshots.
- `GET https://clob.polymarket.com/book?token_id={token_id}` — REST book snapshot **[live-captured]**,
  same schema as the WSS `book` event plus `min_order_size`, `tick_size`, `neg_risk`,
  `last_trade_price`. This is the snapshot-refresh endpoint for §5.1 gap recovery.
- `GET https://clob.polymarket.com/midpoint?token_id=...` → `{"mid":"0.0035"}` **[live-captured]**.
- `POST https://clob.polymarket.com/prices` with body
  `[{"token_id":"...","side":"BUY"}, ...]` → `{ "<token_id>": {"BUY":"0.996"}, ... }` — batch
  best-price snapshot, no auth **[live-captured]**.

---

## 2. CLOB WebSocket (market channel)

Docs: https://docs.polymarket.com/developers/CLOB/websocket/wss-overview and
https://docs.polymarket.com/developers/CLOB/websocket/market-channel

- URL: `wss://ws-subscriptions-clob.polymarket.com/ws/market`
- **Auth: none** for the market channel (only the `user` channel needs `apiKey`/`secret`/`passphrase`).
  Verified live: anonymous connect + subscribe streamed data immediately.
- Subscription is keyed by **token ids (asset ids)**, not condition ids. Subscribe message
  (must be sent immediately after connect; "The server may close connections that don't
  subscribe within a timeout period" — wss-overview):

```json
{"assets_ids": ["<token_id_1>", "<token_id_2>"], "type": "market"}
```

Optional `"custom_feature_enabled": true` additionally enables `best_bid_ask`, `new_market`,
and `market_resolved` events (market-channel doc).

- **Heartbeat**: client sends the literal text frame `PING` every 10 seconds; server replies
  literal `PONG` (wss-overview doc; verified live — `PING` → `PONG`). Payloads otherwise arrive
  as JSON, sometimes wrapped in a JSON array of events.
- On subscribe the server immediately pushes a full `book` snapshot per subscribed asset
  **[live-verified]** — so WSS alone seeds initial book state (REST `/book` still needed for
  gap recovery mid-session).
- Event types (market-channel doc): `book` (snapshot on subscribe and after trades),
  `price_change` (new/cancelled orders), `tick_size_change`, `last_trade_price`, and with
  custom feature flag: `best_bid_ask`, `new_market`, `market_resolved`.
- **Gap detection: there are NO sequence numbers.** Each `book` and each `price_changes[]`
  entry carries a `hash` (book-state hash) plus a ms-epoch `timestamp`. Strategy: apply
  deltas, and use timestamp monotonicity + staleness watchdog; on doubt, re-fetch REST
  `/book` and compare `hash`. `price_change` also carries `best_bid`/`best_ask` per asset,
  which lets a top-of-book consumer self-heal without full book reconstruction.
- **Limits**: no documented cap on `assets_ids` per subscription or connections
  (market-channel doc is silent). Community practice is to shard large universes across
  connections; venuewire should make batch size configurable and shard defensively.

Example live subscribe → first messages **[live-captured 2026-07-18]**:

```json
// sent
{"assets_ids": ["85367...089640", "40069...239948"], "type": "market"}

// received (book snapshot, truncated levels)
{"market": "0x7c97f7315ac9a2e0eabe9b2b9caa8369feff95180483b5168a5274b69762690c",
 "asset_id": "85367286745806857961178482075931972831841231758328346969840810630055458089640",
 "timestamp": "1784392420471",
 "hash": "1243065dd822da900bfa02f607e50a8b416ccc5e",
 "bids": [{"price": "0.001", "size": "21689.85"}, {"price": "0.002", "size": "14481.32"}, {"price": "0.003", "size": "10000"}],
 "asks": [{"price": "0.999", "size": "92.71"}, {"price": "0.998", "size": "18.2"}, ...],
 "event_type": "book"}

// received (price_change)
{"market": "0x7c97f7315ac9a2e0eabe9b2b9caa8369feff95180483b5168a5274b69762690c",
 "price_changes": [
   {"asset_id": "40069...239948", "price": "0.37", "size": "850", "side": "BUY",
    "hash": "abcad7e58703565112ec4e45694d46a14d1a631c", "best_bid": "0.996", "best_ask": "0.997"},
   {"asset_id": "85367...089640", "price": "0.63", "size": "850", "side": "SELL",
    "hash": "bdd0f19c10b07334d1f25012b7d1dc89255a7178", "best_bid": "0.003", "best_ask": "0.004"}],
 "timestamp": "1784392423403", "event_type": "price_change"}
```

Doc example of `book` **[doc-sourced, market-channel page]** for comparison (note doc shows
prices like `".48"` without leading zero; live data used `"0.48"`-style — parser must accept both):

```json
{"event_type": "book",
 "asset_id": "65818619657568813474341868652308942079804919287380422192892211131408793125422",
 "market": "0xbd31dc8a20211944f6b70f31557f1001557b59905b7738480ca09bd4532f84af",
 "bids": [{"price": ".48", "size": "30"}, {"price": ".49", "size": "20"}, {"price": ".50", "size": "15"}],
 "asks": [{"price": ".52", "size": "25"}, {"price": ".53", "size": "60"}, {"price": ".54", "size": "10"}],
 "timestamp": "123456789000",
 "hash": "0x0...."}
```

---

## 3. Price units and schema

- **Prices are decimal strings in [0, 1]** everywhere on CLOB WSS/REST (`"0.996"`, `".48"`),
  i.e. probability-of-outcome in USDC per share; sizes are decimal-string share counts.
  Gamma REST mixes types: `outcomePrices` is a JSON-encoded string of decimal strings, while
  `bestBid`/`lastTradePrice`/`orderPriceMinTickSize` are JSON numbers, and CLOB REST
  `tokens[].price` is a JSON number. Recommend parsing all prices via a tolerant
  string-or-number Decimal deserializer.
- Tick size per market: `minimum_tick_size` / `orderPriceMinTickSize` (commonly `0.01` or
  `0.001`); `tick_size_change` WSS events can change it intra-session.
- **Book level ordering (live-observed)**: `bids` ascend and `asks` descend, i.e. **best bid =
  last element of `bids`, best ask = last element of `asks`** (arrays are sorted away-from-touch
  first). Do not assume best-first.
- **Timestamps**: WSS and CLOB `/book` use string **milliseconds since epoch**
  (`"timestamp": "1784392423403"`) → maps directly to spec's `venue_ts DateTime64(3)`.
  Gamma/CLOB market metadata uses ISO-8601 UTC strings (`"2026-06-01T00:00:00Z"`).
- **YES/NO ↔ market mapping**: one market = one `conditionId`, with exactly two ERC-1155
  outcome tokens in `tokens[]` / `clobTokenIds`. Each token has its own independent order book
  and its own WSS stream keyed by `token_id`; complementarity holds (YES price ≈ 1 − NO price:
  live `0.0035` vs `0.9965`). Ordering in `clobTokenIds` matches `outcomes` — for binary
  markets that is `["Yes","No"]`, so index 0 = YES token. WSS events echo the parent
  `"market": <condition_id>` so a connector can subscribe to only the YES token and still
  attribute events, but subscribing to both and normalizing (NO bid at p ⇒ YES ask at 1−p)
  gives a fuller picture. Multi-outcome events are N separate binary markets grouped by
  `negRisk`/`negRiskMarketID` (outcomes then are entity names, not Yes/No).

---

## 4. Surprises vs venuewire spec (§5.1 / §5.3)

1. **No sequence numbers anywhere on the market channel.** Spec §5.1 says "Sequence/gap
   detection where the venue provides sequence numbers" — Polymarket provides only a book
   `hash` and ms timestamps. Gap detection must be: staleness watchdog + periodic REST
   `/book` hash reconciliation; `GapDetected{expected, got}` cannot be seq-based for this venue
   (consider carrying hashes or timestamps instead).
2. **`clobTokenIds`, `outcomes`, `outcomePrices` on Gamma are JSON-encoded strings**, not
   arrays — double-decode required. Easy to miss and will fail naive serde typing.
3. **Outcomes are not always YES/NO.** Sports/multi-candidate markets have outcome names like
   `"T1"`/`"Karmine Corp"`; §5.3's implicit "YES/NO outcome tokens" only holds for binary
   markets. Normalization should treat token[0] as the "yes-leg" only when
   `outcomes == ["Yes","No"]`, else store outcome labels.
4. **`category` field is effectively absent** on live Gamma market objects; category filtering
   must go through `tag_id` / event `tags`. The matcher's "same category" gate (§6) should key
   on tag slugs.
5. **Token ids are ~78-digit decimal strings** (uint256). They exceed u64/u128 display forms in
   some tooling — keep them as `String` in `InstrumentId` mapping.
6. **WSS pushes a full book snapshot on subscribe**, and heartbeat is a non-standard literal
   text `PING`/`PONG` (client-initiated every 10s), not WebSocket protocol ping frames — the
   staleness watchdog must send text `PING` and expect text `PONG`, and the JSON parser must
   skip the `PONG` frame. Messages may also arrive as a JSON array of events.
7. **Book arrays are sorted worst-to-best** (best at the end) — opposite of most venues.
8. **CLOB `/simplified-markets` has no active/closed filters** and returns closed markets;
   discovery should use Gamma (offset pagination) and reserve CLOB REST for `/book`,
   `/midpoint`, and batch `POST /prices` snapshots.
9. Subscription/connection limits are **undocumented**; make WSS shard size configurable
   rather than assuming a documented cap.
