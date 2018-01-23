const sql = require('mssql');
const config = require('./config.json');
const chalk = require('chalk');
const Joi = require('joi');

const validateConfig = Joi.validate(config, Joi.object().keys({
   DataSource: {
      SQLServerInstance: Joi.string().required(),
      SQLServerUser: Joi.string().required(),
      SQLServerPassword: Joi.string().required(),
      CatalogName: Joi.string().required()
   },
   DataTarget: {
      SQLServerInstance: Joi.string().required(),
      SQLServerUser: Joi.string().required(),
      SQLServerPassword: Joi.string().required(),
      CatalogName: Joi.string().required()
   },
   Options: {
      MonthSize: Joi.number().integer().min(1).required(),
      WipeStaging: Joi.boolean().required(),
      DoKPIRun: Joi.boolean().required(),
      WipeDBO: Joi.boolean().required()
   },
   Clients: Joi.array().items(Joi.object().keys({
      Tag: Joi.string().required(),
      ReplaceTag: Joi.string().required()
   })).min(1)
}));

const getSourceConnectionPool = () => sql.connect({
   user: config.DataSource.SQLServerUser,
   password: config.DataSource.SQLServerPassword,
   server: config.DataSource.SQLServerInstance,
   database: config.DataSource.CatalogName,
   requestTimeout: 60000,
   options: {
      encrypt: true
   }
});
const getTargetConnectionPool = () => sql.connect({
   user: config.DataTarget.SQLServerUser,
   password: config.DataTarget.SQLServerPassword,
   server: config.DataTarget.SQLServerInstance,
   database: config.DataTarget.CatalogName,
   requestTimeout: 60000,
   options: {
      encrypt: true
   }
});
const logColor = (fn, color, ...args) => {
   fn(chalk[color](...args));
}
const logError = (...text) => logColor(console.trace, 'red', ...text);
const logWarning = (...text) => logColor(console.log, 'yellow', ...text);
const log = (...text) => logColor(console.log, 'blue', ...text);

sql.on('error', logError)

module.exports = {
   validateConfig,
   getSourceConnectionPool,
   getTargetConnectionPool,
   log,
   logWarning,
   logError
}