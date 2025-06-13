const xlsx = require('xlsx');
const path = require('path');
const fs = require('fs');

siteIdToPgidMap = {  
  46991: 6470,
  46997: 6442,
  46998: 6480  
}


siteIdToSidMap = {
  47421: 235073250,
  47471: 235073751,
  47937: 235079256,
  47939: 235079277,
}

siteIdToVidMap = {
  47937: 12093,
  47939: 12095,
}


const workbook = xlsx.readFile(path.join(__dirname, 'xyz.xlsx'));
const sheetName = workbook.SheetNames[5]; 
const worksheet = workbook.Sheets[sheetName];

const getValuesFromIndexes = (row, indexes) => indexes.map(i => row[i]);



const rawData = xlsx.utils.sheet_to_json(worksheet, { header: 1 });
const headers = rawData[1]; 
const dataRows = rawData.slice(2); 

const totalRequestsIndexes = [17]; // Example
const adImpressionsIndexes = [18];
const revenueIndexes = [19];


const formattedData = dataRows
  .map(row => {
    const rowObj = {};
    headers.forEach((header, index) => {
      rowObj[header] = row[index];
    });

    const siteId = rowObj["Site ID [Extracted from Column B]"];
    const adunitId = rowObj["Adunit ID [Extracted from Column B]"];
    const adUnitName = rowObj["Ad unit"];


    if (!siteId || !adunitId || siteId === '#N/A' || adunitId === '#N/A' || siteId === 46331) return null;

    if (typeof adUnitName === 'string' && adUnitName.toLowerCase().startsWith('ar')) return null;

    const siteNum = Number(siteId);
    if (Number.isNaN(siteNum) ) return null;

    const sid = siteIdToSidMap[siteNum];
    const pgid = siteIdToPgidMap[siteNum];
    const vid = siteIdToVidMap[siteNum];

    if (!sid || !pgid || !vid) return null;

    const totalRequests = getValuesFromIndexes(row, totalRequestsIndexes);
    const adImpressions = getValuesFromIndexes(row, adImpressionsIndexes);
    const adRevenue = getValuesFromIndexes(row, revenueIndexes);

    return {
      date: "10-06-2025",
      cid: 0,
      device_type: 0,
      ad_format_type: 0,
      variationtype: 0,
      sid,
      pgid,
      vid,
      site_id: siteNum,
      ntwauid: adunitId,
      ntwid: 23,
      total_requests: totalRequests[0],
      ad_exchange_impressions: adImpressions[0],
      ad_exchange_revenue: adRevenue[0]

    };
  })
  .filter(row => row !== null);


const newWorkbook = xlsx.utils.book_new();
const newWorksheet = xlsx.utils.json_to_sheet(formattedData);

xlsx.utils.book_append_sheet(newWorkbook, newWorksheet, 'FilteredData');
const outputExcelPath = path.join(__dirname, '10_June.xlsx');
xlsx.writeFile(newWorkbook, outputExcelPath);

console.log(`Saved ${formattedData.length} entries to Excel at: ${outputExcelPath}`);












