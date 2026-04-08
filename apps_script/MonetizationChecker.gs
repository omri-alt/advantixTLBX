/**
 * Updates active offers sheet with monetization status (columns I, J).
 * Infers account from sheet name: ..._offers_2 -> keyKL_2, else keyKL_1.
 */
function checkLiveMonetization() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getActiveSheet();
  const sheetName = sheet.getName();

  const account = sheetName.indexOf('_offers_2') !== -1 ? 2 : 1;
  const API_KEY = PropertiesService.getScriptProperties().getProperty(account === 2 ? 'keyKL_2' : 'keyKL_1');
  if (!API_KEY) {
    SpreadsheetApp.getUi().alert('Set Script Property keyKL_' + account + ' first.');
    return;
  }

  const data = sheet.getDataRange().getValues();
  const headers = data[0];

  const geoIdx = headers.indexOf('Country');
  const urlIdx = headers.indexOf('Store Link');
  const liveStatusIdx = headers.indexOf('live');
  if (geoIdx === -1 || urlIdx === -1 || liveStatusIdx === -1) {
    SpreadsheetApp.getUi().alert('Sheet must have columns: Country, Store Link, live.');
    return;
  }

  const statusCol = 9;
  const timestampCol = 10;
  if (headers.length < 10) {
    sheet.getRange(1, statusCol).setValue('Monetization Status').setFontWeight('bold');
    sheet.getRange(1, timestampCol).setValue('Last Checked').setFontWeight('bold');
  }

  let resultsProcessed = 0;
  for (let i = 1; i < data.length; i++) {
    if (data[i][liveStatusIdx] !== 'live') continue;

    const geo = (data[i][geoIdx] || '').toString().toLowerCase();
    const productUrl = encodeURIComponent(data[i][urlIdx] || '');
    const now = new Date().toLocaleString();
    let status = 'Error';
    let color = '#ffffff';

    try {
      const endpoint = 'https://api.kelkoogroup.net/publisher/shopping/v2/search/link?country=' + geo + '&merchantUrl=' + productUrl;
      const options = { method: 'get', headers: { Authorization: 'Bearer ' + API_KEY }, muteHttpExceptions: true };
      const response = UrlFetchApp.fetch(endpoint, options);
      const respCode = response.getResponseCode();
      const content = response.getContentText();

      if (respCode === 200 && content.indexOf('http') !== -1) {
        status = '✅ Monetized';
        color = '#d9ead3';
      } else {
        status = '❌ Unmonetized';
        color = '#ea9999';
      }
    } catch (err) {
      status = '⚠️ API Timeout';
      color = '#fff2cc';
    }

    sheet.getRange(i + 1, statusCol).setValue(status).setBackground(color);
    sheet.getRange(i + 1, timestampCol).setValue(now);
    resultsProcessed++;
    Utilities.sleep(10);
  }

  SpreadsheetApp.getUi().alert('Account ' + account + ': Processed ' + resultsProcessed + ' live links.');
}
