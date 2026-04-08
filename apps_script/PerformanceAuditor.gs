/**
 * Performance Auditor – colors fixim sheet by visibility and report stats.
 * Sheet: {date}_fixim_1 or _2. Script Properties: keyKL_1, keyKL_2.
 */
function auditCampaignPerformance(account) {
  account = account === 2 ? 2 : 1;
  const API_KEY = PropertiesService.getScriptProperties().getProperty(account === 2 ? 'keyKL_2' : 'keyKL_1');
  if (!API_KEY) {
    SpreadsheetApp.getUi().alert('Set Script Property keyKL_' + account + ' first.');
    return;
  }

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const today = new Date();
  const startOfMonth = new Date(today.getFullYear(), today.getMonth(), 1);
  const yesterday = new Date(today);
  yesterday.setDate(yesterday.getDate() - 1);
  const startStr = Utilities.formatDate(startOfMonth, 'GMT', 'yyyy-MM-dd');
  const endStr = Utilities.formatDate(yesterday, 'GMT', 'yyyy-MM-dd');

  const url = 'https://api.kelkoogroup.net/publisher/reports/v1/aggregated?start=' + startStr + '&end=' + endStr + '&groupBy=merchantId&format=JSON';
  const options = {
    method: 'get',
    headers: { Authorization: 'Bearer ' + API_KEY, Accept: 'application/json' },
    muteHttpExceptions: true
  };

  const response = UrlFetchApp.fetch(url, options);
  if (response.getResponseCode() !== 200) throw new Error('API Fail: ' + response.getContentText());

  const stats = JSON.parse(response.getContentText());
  const performanceMap = {};
  stats.forEach(function (item) {
    performanceMap[item.merchantId] = { leads: item.leadCount || 0, sales: item.saleCount || 0 };
  });

  const dateStr = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd');
  const sheetName = dateStr + '_fixim_' + (account === 2 ? '2' : '1');
  const targetSheet = ss.getSheetByName(sheetName);
  if (!targetSheet) {
    SpreadsheetApp.getUi().alert('Sheet not found: ' + sheetName);
    return;
  }

  const dataRange = targetSheet.getDataRange();
  const values = dataRange.getValues();
  const backgrounds = dataRange.getBackgrounds();
  const headers = values[0];

  const idIdx = headers.indexOf('id');
  const visibleIdx = headers.indexOf('visible');
  if (idIdx === -1 || visibleIdx === -1) {
    SpreadsheetApp.getUi().alert("Required columns 'id' or 'visible' not found.");
    return;
  }

  for (let i = 1; i < values.length; i++) {
    const mId = values[i][idIdx];
    const isVisible = values[i][visibleIdx];
    const perf = performanceMap[mId] || { leads: 0, sales: 0 };
    var color = null;
    if (isVisible === false || isVisible === 'FALSE') {
      color = '#ea9999';
    } else if (perf.sales > 0) {
      color = '#cfe2f3';
    } else if (perf.leads >= 800) {
      color = '#f4cccc';
    } else if (perf.leads >= 400) {
      color = '#fce5cd';
    } else if (perf.leads >= 1) {
      color = '#fff2cc';
    } else {
      color = '#d9ead3';
    }
    backgrounds[i] = Array(headers.length).fill(color);
  }

  dataRange.setBackgrounds(backgrounds);
  SpreadsheetApp.getUi().alert('Account ' + account + ': Audit complete. Avoid Red rows (Visibility: FALSE).');
}
