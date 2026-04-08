# Kelkoo Tools – Apps Script (2 accounts)

Copy these files into your Google Sheet’s Apps Script project (Extensions > Apps Script).  
Sheet naming: `{date}_fixim_1`, `{date}_fixim_2`, `{date}_offers_1`, `{date}_offers_2`.

## Files to add in the script project

| File in this folder   | Add as in Apps Script |
|------------------------|------------------------|
| `Code.gs`              | Code.gs                |
| `ToOpen.gs`            | ToOpen.gs              |
| `PerformanceAuditor.gs`| PerformanceAuditor.gs |
| `ProductFinder.gs`     | ProductFinder.gs       |
| `MonetizationChecker.gs`| MonetizationChecker.gs |
| `Sidebar.html`         | Sidebar.html (HTML file)|

You can merge `Code.gs` and `ToOpen.gs` into one `Code.gs` if you prefer a single script file; keep the same function names.

## Script properties (required)

In the script editor: **Project Settings** (gear) > **Script properties**:

- `keyKL_1` = Kelkoo API key for account 1  
- `keyKL_2` = Kelkoo API key for account 2  

## Menu after install

After saving and reopening the sheet, menu **Kelkoo Tools**:

- Import Static Merchants (Account 1 / Account 2)
- Open Product Search Sidebar
- Audit Performance (Account 1 / Account 2)
- Check Live Monetization

Sidebar: choose Account 1 or 2, then Auto-Fill Green Merchants and Launch Workflow to build the offers sheet for that account.
