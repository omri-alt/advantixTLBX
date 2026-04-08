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
  "trackingUrl": "https://..."
}
```

## 3) Campaign Publisher Stats

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

## 4) Minimal typed models (for migration scripts)

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

## Notes for upcoming Keitaro migration

- Tracking-link update work should use full campaign payload from `GET /campaigns/{id}` before `PUT`.
- `trackingUrl` exists on campaign-level payload and is already read/updated in legacy code.
- Rate-limit handling is needed (`429` / `"Too Many Requests"`), currently done by simple 60s retries.
