const sql = require('mssql');
const config = require('./config.json');
const fs = require('fs');
const { promisify } = require('util');
const writeFileAsync = promisify(fs.writeFile);
const { validateConfig, log, logError, getSourceConnectionPool } = require('./lib');

if (validateConfig.error) {
   logError('Config validation failed', JSON.stringify(validateConfig.error.details, null, 2));
   return;
}

(async () => {
   let pool;
   try {
      pool = await getSourceConnectionPool();

      const textQueries = {
         operations: `
            SELECT [ServerName]
               ,[DBName]
               ,[ClientTag]
               ,[ClientID]
               ,[ActionDate]
               ,[TotalLag]
               ,[CreatedClaims]
               ,[FiledClaims]
               ,[TotalDaysToFile]
               ,[HardCloseBatchDate]
               ,[ExtractionDate]
               ,[Processed]
               ,[OrgID]
               ,[LookupClientID]
            FROM [Staging].[Operations]
            WHERE [ClientTag] = @ClientTag
            AND DATEDIFF(MONTH, [ActionDate], GETDATE()) < @MonthSize
            ORDER BY [ActionDate] DESC
            FOR JSON AUTO, INCLUDE_NULL_VALUES`,

         invoicing: `
            SELECT [ServerName]
               ,[DBName]
               ,[ClientID]
               ,[ClientTag]
               ,[ActionDate]
               ,[CleanRecords]
               ,[AllRecords]
               ,[Rejections]
               ,[FiledClaims]
               ,[HardCloseBatchDate]
               ,[Denials]
               ,[ProcessedLineItems]
               ,[ExtractionDate]
               ,[Processed]
               ,[OrgID]
               ,[ClientID2]
               ,[LookupClientID]
            FROM [Staging].[Invoicing]
            WHERE [ClientTag] = @ClientTag
            AND DATEDIFF(MONTH, [ActionDate], GETDATE()) < @MonthSize
            ORDER BY [ActionDate] DESC
            FOR JSON AUTO, INCLUDE_NULL_VALUES`,

         receivables: `
            SELECT [ServerName]
               ,[DBName]
               ,[ClientID]
               ,[ClientTag]
               ,[HardCloseBatchDate]
               ,[YearNumber]
               ,[MonthNumber]
               ,[ARType]
               ,[ARAging_0to30]
               ,[ARAging_31to60]
               ,[ARAging_61to90]
               ,[ARAging_91to120]
               ,[ARAging_Over120]
               ,[TotalCharges]
               ,[DaysInRange]
               ,[ExtractionDate]
               ,[Processed]
               ,[OrgID]
               ,[LookupClientID]
            FROM [Staging].[Receivables]
            WHERE [ClientTag] = @ClientTag
            AND DATEDIFF(MONTH, [ExtractionDate], GETDATE()) < 20
            ORDER BY [ExtractionDate] DESC
            FOR JSON AUTO, INCLUDE_NULL_VALUES`
      };

      const filenames = Object.keys(textQueries).map(e => `./${e}.json`);
      const json = Object.keys(textQueries).map(() => []);

      for(let client of config.Clients) {
         log('Pulling data for client', client.Tag);

         const results = await Promise.all(Object.entries(textQueries).map(([name, query]) =>
            pool
               .request()
               .input('ClientTag', sql.VarChar(5), client.Tag)
               .input('MonthSize', sql.Int, config.Options.MonthSize)
               .query(query)
         ));

         results.forEach((result, i) => {
            log(Object.keys(textQueries)[i], 'query received', result.rowsAffected[0], 'records')
         });

         results
            .map(r => r.recordset[0]) // select first recordset
            .map(r => Object.values(r)[0]) // select first column
            .map(r => JSON.parse(r)) // parse into json
            .map(o => o.map(r => ({ ...r, ClientTag: client.ReplaceTag }))) // replace tag
            .forEach((o, i) => json[i] = [...json[i], ...o])
      }

      log('Writing data for all clients');
      await Promise.all(json.map((o, i) => writeFileAsync(filenames[i], JSON.stringify(o, null, 2))))
   } catch (err) {
      logError(err)
   } finally {
      if (pool) { pool.close() }
      log('data download finished')
   }
})();