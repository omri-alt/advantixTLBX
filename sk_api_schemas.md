# SourceKnowledge (SK) API Schemas (from existing automation)

This document is inferred from the GET requests and field usage in `sk_legacy_snapshot.py`.
It is designed for migration prep (Keitaro tracking-link rollout) and can be refined with live samples.

## Auth

- Header: `X-API-KEY: <KEYSK>` (set `KEYSK` in `.env`; legacy name `keySK` is also read)
- Header: `accept: application/json`
- In this repo, `config.SOURCEKNOWLEDGE_API_KEY` mirrors `KEYSK` / `keySK` (with `.env` fallback parsing like other keys).

## 1) Advertisers

### GET `/affiliate/v2/advertisers?page={n}`

Observed list response:

```json
{
  "items": [
    {
      "id": 0,
      "name": "string",
      "businessUrl": "string",
      "categoryId": 0,
      "categoryName": "string"
    }
  ]
}
```

### GET `/affiliate/v2/advertisers/{id}`

Observed object response (minimum known fields):

```json
{
  "id": 0,
  "name": "string",
  "businessUrl": "string",
  "categoryId": 0
}
```

## 2) Campaigns

### GET `/affiliate/v2/campaigns?page={n}`
### GET `/affiliate/v2/campaigns?advertiserId={id}`

Observed list response:

```json
{
  "items": [
    {
      "id": 0,
      "name": "string",
      "active": true,
      "start": "2026-03-01T00:00:00Z",
      "end": "2026-12-31T23:59:59Z",
      "updated": "2026-03-30T12:00:00Z",
      "advertiser": {
        "id": 0,
        "name": "string"
      }
    }
  ]
}
```

### GET `/affiliate/v2/campaigns/{id}`

Observed object response (minimum known fields):

```json
{
  "id": 0,
  "name": "string",
  "active": true,
  "start": "2026-03-01T00:00:00Z",
  "end": "2026-12-31T23:59:59Z",
  "updated": "2026-03-30T12:00:00Z",
  "advertiser": {
    "id": 0,
    "name": "string"
  },
  "cpc": 0.0,
  "dailyBudget": 25.0,
  "trackingUrl": "https://...",
  "allowDeepLink": true,
  "geoTargeting": ["DE"],
  "partnerChannels": ["1", "2", "3", "5", "6", "8", "9", "12", "13", "14", "15", "16"]
}
```

**Note:** campaign GET does **not** return attached allow/block control lists. Manage lists via `/control-lists` (below).

### POST `/affiliate/v2/campaigns/{id}` — pause / activate

Body is a partial update (not full PUT):

```json
{ "active": false }
```

```json
{ "active": true }
```

- Implemented as `pause_campaign` / `activate_campaign` in `integrations/autoserver/sk.py`.
- Some archived campaigns reject status changes: `{"error":"You cannot change the status of campaign with id \"…\"."}` (and PUT may return Access Denied).

### PUT `/affiliate/v2/campaigns/{id}`

Full campaign update (tracking URL, targeting, etc.). Unknown extra JSON keys are often accepted with HTTP 200 but **ignored** (e.g. stuffing `allowListId` on the campaign body does nothing).

## 3) Control lists (allow / block)

UI name: **Allow & Block Lists**. API resource: **`control-lists`**.

### GET `/affiliate/v2/control-lists`

Paginated account lists (`items`, `itemsCount`, `hasMore`).

```json
{
  "itemsCount": 30,
  "items": [
    {
      "id": 59583,
      "name": "YadJuneWL",
      "itemsCount": null,
      "type": "allow",
      "global": false,
      "updated": "2026-08-03 07:06:40"
    }
  ],
  "hasMore": false
}
```

- `type`: `"allow"` | `"block"`
- `global: true` — e.g. the account global block list

### GET `/affiliate/v2/control-lists/{id}`

Full list detail (live-verified):

```json
{
  "id": 59583,
  "name": "YadJuneWL",
  "itemsCount": 74,
  "type": "allow",
  "global": false,
  "updated": "2026-08-03 07:06:40",
  "campaigns": [
    { "id": 388915, "name": "shopapothekeYADWL-AT-all" }
  ],
  "subIds": ["s1bf84bf08ddb9e4", "s2a818b0646682f5"]
}
```

- **Allow list:** only listed `subIds` may buy on associated campaigns.
- **Block list:** all traffic except listed `subIds`.

### PUT `/affiliate/v2/control-lists/{id}`

Replace list membership. Working body shape (live-verified):

```json
{
  "name": "YadJuneWL",
  "type": "allow",
  "global": false,
  "campaigns": [388915, 388210],
  "subIds": ["s1bf84bf08ddb9e4"]
}
```

- `campaigns` may be a list of **integer IDs** (preferred for updates) or objects `{id, name}` as returned by GET.
- **Constraint:** a campaign may belong to **only one allow list**. Adding a campaign that is already on another allow list returns HTTP 400:
  `{"error":"Some of the specified campaign are invalid or associated to another list!"}`
  Fix: PUT the other list first **without** that campaign id, then PUT this list **with** it included.
- PATCH is not allowed (405). Nested `/control-lists/{id}/campaigns` routes were 404 as of 2026-08.

### Operational lists used in this account (examples)

| id | name | type | role |
| -- | ---- | ---- | ---- |
| 59583 | YadJuneWL | allow | Yadore / YADWL converting-source allow list |
| 58846 | MayNewWL | allow | earlier allow pool (campaigns may need moving off before YadJuneWL) |
| 48365 | Global block list | block | global |

## 4) Campaign Publisher Stats

### GET `/affiliate/v2/stats/by-campaign?from={YYYY-MM-DD}&to={YYYY-MM-DD}&page={n}`

Account-wide stats per campaign for a date range (paginated, typically 1000 items/page).
Used by the SK console exploration cost widget: one paginated call **per UTC day** (`from=to`),
then filter `items[].id` to `SKtrackExploration` campaign IDs and sum `spend`.

Observed list response:

```json
{
  "itemsCount": 1000,
  "items": [
    {
      "id": 12345,
      "name": "brand-uk-KLFIX-c12345",
      "spend": 1.23,
      "revenue": 0.0,
      "clicks": 10,
      "impressions": 100,
      "conversions": 0,
      "advertiser": { "id": 128119, "name": "brand-UK-KLFLEX" }
    }
  ],
  "hasMore": true,
  "page": 1
}
```

Optional query: `advertiserId` (integer) to filter by advertiser.

### GET `/affiliate/v2/stats/campaigns/{campaignId}/by-publisher?from={YYYY-MM-DD}&to={YYYY-MM-DD}`
### GET `/affiliate/v2/stats/campaigns/{campaignId}/by-publisher` with params `from,to,subid`

Observed response:

```json
{
  "itemsCount": 0,
  "items": [
    {
      "subId": "s3ed3a7177c013e2",
      "winRate": 0.0,
      "bidFactor": 1.0
    }
  ]
}
```

Possible error response observed in code:

```json
{
  "error": "Too Many Requests"
}
```

## 5) Minimal typed models (for migration scripts)

```python
from typing import TypedDict, NotRequired, List

class SKAdvertiser(TypedDict):
    id: int
    name: str
    businessUrl: NotRequired[str]
    categoryId: NotRequired[int]
    categoryName: NotRequired[str]

class SKCampaignAdvertiser(TypedDict):
    id: int
    name: str

class SKCampaign(TypedDict):
    id: int
    name: str
    active: bool
    start: str
    end: str
    updated: str
    advertiser: SKCampaignAdvertiser
    cpc: NotRequired[float]
    trackingUrl: NotRequired[str]

class SKPublisherStat(TypedDict):
    subId: str
    winRate: float
    bidFactor: float

class SKListResponseAdvertisers(TypedDict):
    items: List[SKAdvertiser]

class SKListResponseCampaigns(TypedDict):
    items: List[SKCampaign]

class SKStatsResponse(TypedDict):
    itemsCount: int
    items: List[SKPublisherStat]
```

## Notes

- Tracking-link update work should use full campaign payload from `GET /campaigns/{id}` before `PUT`.
- `trackingUrl` exists on campaign-level payload and is already read/updated in legacy code.
- Rate-limit handling is needed (`429` / `"Too Many Requests"`), currently done by simple 60s retries.
- By-publisher `from`/`to` windows are limited (~3 calendar months); older ranges may return empty.
- Do not confuse **control-list allow** (SK-side traffic restriction) with **`SKtrackExploration.wl`** (sheet JSON of converting subIds protected by the hourly optimizer).
