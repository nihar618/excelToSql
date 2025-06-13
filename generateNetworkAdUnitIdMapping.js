const fs = require('fs');
const path = require('path');
const sql = require('mssql');

const sectionIds = JSON.parse(fs.readFileSync(path.join(__dirname, 'unique_networkadunit_ids.txt'), 'utf8'));

const config = {
  user: '',
  password: '',
  server: '', 
  database: '',
  options: {
    trustServerCertificate: true 
  },
  pool: {
    max: 5,
    min: 1,
    idleTimeoutMillis: 30000,
  },
  requestTimeout: 60000,
  connectionTimeout: 60000  
};

const batchSize = 10;
const siteIdToSidMap = {};

async function connectDB() {
  try {
    await sql.close(); 
  } catch (_) {}
  return sql.connect(config);
}

async function processBatch(batch, batchIndex) {
  const escapedIds = batch.map(id =>
    typeof id === 'number' ? id : `'${id.replace(/'/g, "''")}'`
  );
  const query = `SELECT ntauid, id FROM NetworkAdUnit WHERE id IN (${escapedIds.join(',')})`;
  console.log(`Batch ${batchIndex}: ${batch.length} IDs`);
  const result = await sql.query(query);
  console.log(result);

  result.recordset.forEach(row => {
    siteIdToSidMap[row.id] = row.ntauid;
  });
}

(async () => {
  let batchIndex = 0;

  while (batchIndex * batchSize < sectionIds.length) {
    const batchStart = batchIndex * batchSize;
    const batch = sectionIds.slice(batchStart, batchStart + batchSize);

    try {
      if (!sql.connected) {
        console.log(`Reconnecting to SQL Server...`);
        await connectDB();
      }

      await processBatch(batch, batchIndex + 1);
      batchIndex++;
    } catch (err) {
      console.error(`Error on batch ${batchIndex + 1}: ${err.message}`);

      console.log('Waiting 5 seconds before retrying...');
      await new Promise(res => setTimeout(res, 5000));


      sql.close().catch(() => {});
    }
  }

  // Save result to file
  const outputPath = path.join(__dirname, 'siteIdToNidMap.js');
  const jsFormatted = 'const siteIdToNidMap = ' + JSON.stringify(siteIdToSidMap, null, 2) + ';\nmodule.exports = siteIdToNidMap;';
  fs.writeFileSync(outputPath, jsFormatted);

  console.log(`Finished. Total mapped entries: ${Object.keys(siteIdToSidMap).length}`);
  console.log(`Saved to: ${outputPath}`);

  await sql.close();
})();
