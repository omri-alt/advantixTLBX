# Daily workflow (2 Kelkoo accounts)

End-to-end flow: feed → reports → offers sheet → Keitaro → monetization check.

---

## 1. Get updated feed for each account (Google Apps Script)

In the Google Sheet, open **Extensions > Apps Script** and ensure the project has:

- **Script properties** (File > Project settings > Script properties):
  - `keyKL_1` = Kelkoo API key for account 1  
  - `keyKL_2` = Kelkoo API key for account 2  

Then in the sheet:

1. **Kelkoo Tools > Import Static Merchants (Account 1)**  
   → Creates/overwrites sheet `YYYY-MM-DD_fixim_1` with static merchants for account 1.

2. **Kelkoo Tools > Import Static Merchants (Account 2)**  
   → Creates/overwrites sheet `YYYY-MM-DD_fixim_2` for account 2.

---

## 2. Check reports and choose merchants (Google Apps Script)

1. **Kelkoo Tools > Audit Performance (Account 1)**  
   → Colors `YYYY-MM-DD_fixim_1` by visibility and performance (green = no traffic yet, etc.). Avoid red (visibility FALSE).

2. **Kelkoo Tools > Audit Performance (Account 2)**  
   → Same for `YYYY-MM-DD_fixim_2`.

3. **Kelkoo Tools > Open Product Search Sidebar**  
   - Choose **Account 1** or **Account 2**.  
   - Click **Auto-Fill Green Merchants** to fill merchant IDs from the green rows of that day’s fixim sheet.  
   - Adjust IDs if needed, then **Launch Workflow** → creates `YYYY-MM-DD_offers_1` or `_2` with up to 100 products per geo.

---

## 3. Create offers list per account

Done in step 2 when you click **Launch Workflow** in the sidebar:

- Account 1 → sheet `YYYY-MM-DD_offers_1`  
- Account 2 → sheet `YYYY-MM-DD_offers_2`  

Columns: Country, Merchant ID, Product Title, Store Link, Audit Status, Timestamp. Same structure the Python script expects (A = country, D = Store Link).

---

## 4. Sync offers to Keitaro and update campaign (Python)

From project root (e.g. `c:\Users\Acer\KLblend`), run for **today’s** offers sheets. Replace `YYYY-MM-DD` with the actual date (e.g. `2026-03-10`).

**Account 1 (default):**

```powershell
cd "c:\Users\Acer\KLblend"
python .\update_offers_from_sheet.py --sheet "YYYY-MM-DD_offers_1"
```

**Account 2 (uses `KELKOO_ACCOUNT_ID_2` from .env):**

```powershell
python .\update_offers_from_sheet.py --sheet "YYYY-MM-DD_offers_2" --account 2
```

Optional:

- `--max-offers 5` to use 5 offers per geo instead of 10.

This script:

- Reads only geos present in the sheet; first 10 (or `--max-offers`) Store Links per geo.
- Creates/attaches/updates Keitaro offers and archives excess.
- Writes **live** and **offerUpload timestamp** to columns G and H for updated rows.

---

## 5. Check monetization during the day (Google Apps Script)

1. Open the **offers** sheet you care about (`YYYY-MM-DD_offers_1` or `_2`).
2. **Kelkoo Tools > Check Live Monetization**  
   → For each row with **live** in the “live” column, checks if the product is still monetized and fills **Monetization Status** and **Last Checked** (columns I, J). Account is inferred from the sheet name (`_offers_2` → account 2).

---

## 6. When an offer stops being active (later)

Planned options (not implemented yet):

- Replace with another offer from the same system, or  
- Integrate an API to stop the traffic source.

---

## Copy-paste summary (PowerShell, adjust date)

```powershell
cd "c:\Users\Acer\KLblend"

# Account 1
python .\update_offers_from_sheet.py --sheet "2026-03-10_offers_1"

# Account 2 (set KELKOO_ACCOUNT_ID_2 in .env first)
python .\update_offers_from_sheet.py --sheet "2026-03-10_offers_2" --account 2
```

---

## Sheet naming (2 accounts)

| Account | Feed/merchants sheet | Offers sheet |
|--------|----------------------|--------------|
| 1      | `YYYY-MM-DD_fixim_1` | `YYYY-MM-DD_offers_1` |
| 2      | `YYYY-MM-DD_fixim_2` | `YYYY-MM-DD_offers_2` |

Python uses the **offers** sheet name and `--account` to pick the correct Kelkoo account ID for offer URLs.
