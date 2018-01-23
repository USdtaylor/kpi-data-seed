const sql = require('mssql');
const config = require('./config.json');
const fs = require('fs');
const { promisify } = require('util');
const { validateConfig, log, logWarning, logError, getTargetConnectionPool } = require('./lib');

const readFileAsync = promisify(fs.readFile);
const openAsync = promisify(fs.open);

if (validateConfig.error) {
   logError('Config validation failed', JSON.stringify(validateConfig.error.details, null, 2));
   return;
}

const insertRecords = async (pool, tableName, records) => {
   const recordset = (await pool.request().query('SELECT TOP 0 * FROM ' + tableName)).recordsets[0];
   const table = sql.Table.fromRecordset(recordset, tableName);
   records
      .map(record => { // convert string dates to real dates for all date columns
         const newRecord = { ...record, Processed: false };
         table.columns.forEach(column => {
            if (
               (column.type.declaration === 'datetimeoffset' || column.type.declaration === 'date')
               && newRecord[column.name]
            ) {
               newRecord[column.name] = new Date(newRecord[column.name])
            }
         });
         return newRecord
      })
      .forEach(record => {
         table.rows.add(...Object.values(record))
      });
   return table;
};

(async () => {
   let pool;
   try {
      pool = await getTargetConnectionPool();

      const tableToFile = {
         'Invoicing': './invoicing.json',
         'Operations': './operations.json',
         'Receivables': './receivables.json'
      };

      for (let [tableName, filename] of Object.entries(tableToFile)) {
         const fh = await openAsync(filename, 'r').catch(() => {
            logWarning('cannot open file', filename);
            return -1
         });

         if (fh === -1) continue;

         if (config.Options.WipeStaging) {
            log('wiping', 'Staging.' + tableName, 'table')
            await pool.request().query('DELETE FROM Staging.' + tableName)
         }

         if (config.Options.WipeDBO) {
            log('wiping', 'dbo.' + tableName, 'table')
            await pool.request().query('DELETE FROM dbo.' + tableName)
         }

         log('reading records from', filename);
         const records = JSON.parse(await readFileAsync(filename));
         log('inserting records in memory');
         const table = await insertRecords(pool, 'Staging.' + tableName, records);
         log('writing records to database');
         const results = await pool.request().bulk(table);
         log('inserted', results.rowsAffected, 'rows');
      }

      if (config.Options.DoKPIRun) {
         log('running spKPIRun sproc');
         await pool.request().execute('spKPIRun');
      }
   } catch (err) {
      console.log(err);
      logError(err);
   } finally {
      if (pool) { pool.close() }
      log('data load finished')
   }
})();