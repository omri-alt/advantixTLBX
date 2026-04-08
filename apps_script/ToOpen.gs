/**
 * Menu and sidebar for Kelkoo Tools (2 accounts).
 */
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Kelkoo Tools')
    .addItem('Import Static Merchants (Account 1)', 'importAccount1')
    .addItem('Import Static Merchants (Account 2)', 'importAccount2')
    .addSeparator()
    .addItem('Open Product Search Sidebar', 'showSidebar')
    .addSeparator()
    .addItem('Audit Performance (Account 1)', 'auditAccount1')
    .addItem('Audit Performance (Account 2)', 'auditAccount2')
    .addSeparator()
    .addItem('Check Live Monetization', 'checkLiveMonetization')
    .addToUi();
}

function importAccount1() { importKelkooStaticMerchants(1); }
function importAccount2() { importKelkooStaticMerchants(2); }
function auditAccount1() { auditCampaignPerformance(1); }
function auditAccount2() { auditCampaignPerformance(2); }

function showSidebar() {
  const html = HtmlService.createHtmlOutputFromFile('Sidebar')
    .setTitle('Product Search')
    .setWidth(320);
  SpreadsheetApp.getUi().showSidebar(html);
}
