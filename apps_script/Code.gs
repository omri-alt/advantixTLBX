/**
 * Kelkoo Tools – single-file bundle (2 accounts).
 *
 * Sidebar.html expects:
 *  - getGreenMerchantsForToday(account)
 *
 * ToOpen.gs expects:
 *  - importKelkooStaticMerchants(account)
 */

function getApiKey(account) {
  const key = account === 2 ? 'keyKL_2' : 'keyKL_1';
  const apiKey = PropertiesService.getScriptProperties().getProperty(key);
  if (!apiKey) throw new Error('Set Script Property "' + key + '" (File > Project settings > Script properties).');
  return apiKey;
}

function getFiximSheetName(account) {
  const dateStr = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd');
  return dateStr + '_fixim_' + (account === 2 ? '2' : '1');
}

/**
 * Fetch Kelkoo Static merchants for one account.
 * Writes: {date}_fixim_{1|2}
 */
function importKelkooStaticMerchants(account) {
  account = account === 2 ? 2 : 1;
  const API_KEY = getApiKey(account);

  // Must match the countries used by your daily logic.
  const geos = [
    'fi', 'fr', 'gr', 'de', 'hu', 'id', 'ie', 'in', 'it', 'mx', 'nl', 'no',
    'nz', 'pl', 'pt', 'ro', 'se', 'sk', 'uk', 'us'
  ];

  const headers_KL = {
    'Authorization': 'Bearer ' + API_KEY,
    'Content-Type': 'application/json'
  };

  let allMerchants = [];
  let allPossibleKeys = new Set();
  allPossibleKeys.add('geo_origin');

  geos.forEach(function (geo) {
    console.log('Fetching data for: ' + geo + '...');
    const url = 'https://api.kelkoogroup.net/publisher/shopping/v2/feeds/merchants?country=' + geo + '&format=JSON';
    const options = { method: 'get', headers: headers_KL, muteHttpExceptions: true };
    try {
      const response = UrlFetchApp.fetch(url, options);
      if (response.getResponseCode() === 200) {
        const rawData = JSON.parse(response.getContentText());
        if (Array.isArray(rawData)) {
          rawData.forEach(function (merch) {
            if (merch.merchantTier === 'Static') {
              merch['geo_origin'] = geo;
              Object.keys(merch).forEach(function (k) { allPossibleKeys.add(k); });
              allMerchants.push(merch);
            }
          });
        }
      }
    } catch (e) {
      console.error('Request failed for ' + geo + ': ' + e.message);
    }
  });

  if (allMerchants.length > 0) {
    const sheetName = getFiximSheetName(account);
    writeToSheet(allMerchants, Array.from(allPossibleKeys), sheetName);
    SpreadsheetApp.getUi().alert('Account ' + account + ': Saved ' + allMerchants.length + ' static merchants to ' + sheetName);
  } else {
    SpreadsheetApp.getUi().alert('Account ' + account + ': No static merchants found.');
  }
}

function writeToSheet(data, headers, sheetName) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName(sheetName);
  if (sheet) {
    sheet.clear();
  } else {
    sheet = ss.insertSheet(sheetName);
  }

  const rows = [headers];
  data.forEach(function (item) {
    rows.push(headers.map(function (h) { return item[h] === undefined ? '' : item[h]; }));
  });

  if (rows.length > 0 && headers.length > 0) {
    sheet.getRange(1, 1, rows.length, headers.length).setValues(rows);
    // Keep the same default color that PerformanceAuditor uses for the “base green” state.
    sheet.getRange(1, 1, 1, headers.length).setFontWeight('bold').setBackground('#d9ead3');
    sheet.setFrozenRows(1);
    sheet.autoResizeColumns(1, headers.length);
  }
}

function _findHeaderIndex(headers, target) {
  const t = (target || '').toString().trim().toLowerCase();
  for (let i = 0; i < headers.length; i++) {
    const h = headers[i];
    if (h === null || h === undefined) continue;
    if (h.toString().trim().toLowerCase() === t) return i;
  }
  return -1;
}

/**
 * Scan today's fixim sheet for merchants suitable to auto-fill.
 *
 * Fix included:
 *  - Accept both green and yellow rows (Python selector also allows “yellow”).
 *  - CPC rule matches Python:
 *      - for `us`: require BOTH desktop & mobile CPC >= 0.02
 *      - for other geos (e.g. `nl`): require at least ONE channel >= 0.02
 */
function getGreenMerchantsForToday(account) {
  account = account === 2 ? 2 : 1;
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheetName = getFiximSheetName(account);
  const sheet = ss.getSheetByName(sheetName);
  if (!sheet) return {};

  const range = sheet.getDataRange();
  const values = range.getValues();
  const backgrounds = range.getBackgrounds();
  const headers = values[0];

  const geoIdx = _findHeaderIndex(headers, 'geo_origin');
  const idIdx = _findHeaderIndex(headers, 'id');
  const cpcIdx = _findHeaderIndex(headers, 'merchantEstimatedCpc');
  const mobileCpcIdx = _findHeaderIndex(headers, 'merchantMobileEstimatedCpc');

  // If required columns missing, fail open with empty mapping (sidebar won't set anything).
  if (geoIdx === -1 || idIdx === -1 || cpcIdx === -1 || mobileCpcIdx === -1) return {};

  const greenColor = '#d9ead3';
  const yellowColor = '#fff2cc';
  const CPC_FLOOR = 0.02;

  let autoMapping = {};
  for (let i = 1; i < values.length; i++) {
    const geo = values[i][geoIdx] ? values[i][geoIdx].toString().toLowerCase() : '';
    const mId = values[i][idIdx];
    const desktopCpc = parseFloat(values[i][cpcIdx]) || 0;
    const mobileCpc = parseFloat(values[i][mobileCpcIdx]) || 0;
    const rowColor = backgrounds[i][0];

    if (!geo || geo.length < 2) continue;
    if (autoMapping[geo] || (rowColor !== greenColor && rowColor !== yellowColor)) continue;

    const passes = (geo === 'us')
      ? (desktopCpc >= CPC_FLOOR && mobileCpc >= CPC_FLOOR)
      : (Math.max(desktopCpc, mobileCpc) >= CPC_FLOOR);

    if (passes) autoMapping[geo] = mId;
  }

  return autoMapping;
}

