/**
 * Builds offers sheet from merchant IDs per geo. Sheet: {date}_offers_1 or _2.
 * Script Properties: keyKL_1, keyKL_2.
 */
function runMultiCountryAudit(mapping, account) {
  account = account === 2 ? 2 : 1;
  const API_KEY = PropertiesService.getScriptProperties().getProperty(account === 2 ? 'keyKL_2' : 'keyKL_1');
  if (!API_KEY) throw new Error('Set Script Property keyKL_' + account + ' first.');

  var masterData = [];
  var geoKeys = Object.keys(mapping);

  geoKeys.forEach(function (geo) {
    var mId = mapping[geo];
    try {
      var url = 'https://api.kelkoogroup.net/publisher/shopping/v2/feeds/pla?country=' + geo.toLowerCase() + '&merchantId=' + mId + '&format=JSON&numberOfParts=8&part=1';
      var response = UrlFetchApp.fetch(url, {
        headers: { Authorization: 'Bearer ' + API_KEY, Accept: 'text/tab-separated-values' },
        muteHttpExceptions: true
      });

      var content = response.getContentText();
      var lines = content.split('\n');
      if (lines.length <= 2) return;

      var dataLines = lines.slice(1, 101);
      dataLines.forEach(function (line) {
        if (!line.trim()) return;
        var columns = line.split('\t');
        var directLink = columns[2] ? columns[2].replace(/^"|"$/g, '').trim() : '';
        if (!directLink.startsWith('http')) {
          directLink = columns.find(function (col) { return col.trim().startsWith('http'); }) || 'Link Not Found';
        }
        masterData.push({
          Country: geo.toUpperCase(),
          'Merchant ID': mId,
          'Product Title': (columns[1] || 'N/A').replace(/^"|"$/g, ''),
          'Store Link': directLink,
          'Audit Status': 'Active',
          Timestamp: new Date().toLocaleString()
        });
      });
    } catch (err) {
      console.error('[' + geo.toUpperCase() + '] ' + err.message);
    }
  });

  if (masterData.length > 0) {
    var datePart = Utilities.formatDate(new Date(), 'GMT', 'yyyy-MM-dd');
    var sheetName = datePart + '_offers_' + (account === 2 ? '2' : '1');
    var headers = ['Country', 'Merchant ID', 'Product Title', 'Store Link', 'Audit Status', 'Timestamp'];
    writeToSheet(masterData, headers, sheetName);
    return 'Account ' + account + ': Generated ' + masterData.length + ' links in ' + sheetName + '.';
  } else {
    throw new Error('No products found for selected countries.');
  }
}
