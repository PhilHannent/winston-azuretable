const util = require('util');
const azure = require('azure-storage');
const uuidv4 = require('uuid/v4');
const Transport = require('winston-transport');
const entGen = azure.TableUtilities.entityGenerator;

// constant
const DEFAULT_TABLE_NAME = 'log';
const DEFAULT_IS_SILENT = false;
const DEFAULT_NESTED_META = false;
const WINSTON_LOGGER_NAME = 'azurelogger';
const WINSTON_DEFAULT_LEVEL = 'info';
const DATE_MAX = new Date(3000, 1);

//
// ### function AzureLogger (options)
// #### @options {Object} Options for this instance.
// Constructor function for the AzureLogger transport object responsible
// for persisting log messages and metadata to Azure Table Storage.
//
var AzureLogger = module.exports.AzureLogger = function (options) {
	Transport.call(this, options);

	const account = options.account || process.env.AZURE_STORAGE_ACCOUNT;
	const key = options.key || process.env.AZURE_STORAGE_ACCESS_KEY;
	const self = this;

	if (options.useDevStorage !== true) {
		if (!account) {
			throw new Error('azure storage account name required.');
		}

		if (!key) {
			throw new Error('azure storage account key required.');
		}
	}

	this.name = WINSTON_LOGGER_NAME;
	this.level = options.level || WINSTON_DEFAULT_LEVEL;
	this.partitionKey = options.partitionKey || process.env.NODE_ENV;
	this.tableName = options.tableName || DEFAULT_TABLE_NAME;
	this.silent = options.silent || DEFAULT_IS_SILENT;
	this.nestedMeta = options.nestedMeta || DEFAULT_NESTED_META;

	this.tableService = options.useDevStorage ?
		azure.createTableService('UseDevelopmentStorage=true') :
		azure.createTableService(account, key);

	this.tableService.createTableIfNotExists(this.tableName, function (err) {
		if (err) {
			self.emit('error', err);
		}

		if (options.callback) {
			options.callback();
		}
	});
};

//
// Inherits from 'Transport'.
//
util.inherits(AzureLogger, Transport);

AzureLogger.prototype.name = 'azureLogger';

//
// ### function log (level, msg, [meta], callback)
// #### @level {string} Level at which to log the message.
// #### @msg {string} Message to log
// #### @meta {Object} **Optional** Additional metadata to attach
// #### @callback {function} Continuation to respond to when complete.
// Core logging method exposed to Winston. Metadata is optional.
//
AzureLogger.prototype.log = function (info, callback) {
	const self = this;
	const level = info.level;
	const msg = info.message;

	callback = callback || function () { };

	if (this.silent) {
		return callback(null, true);
	}

	const data = {
		PartitionKey: entGen.String(this.partitionKey),
		RowKey: entGen.String(uuidv4()),
		hostname: entGen.String(require('os').hostname()),
		pid: entGen.Int32(process.pid),
		level: entGen.String(level),
		msg: entGen.String(msg),
		createdDateTime: entGen.DateTime(new Date()),
	};

	if (self.batchCheckId) {
		clearTimeout(self.batchCheckId);
	}

	if (!self.batch) {
		self.batch = new azure.TableBatch();
	}

	self.batch.insertEntity(data);
	if (self.batch.size() === 100) {
		let batch = self.batch;
		self.batch = new azure.TableBatch();
		self.tableService.executeBatch(self.tableName, batch, (error/*, result*/) => {
			if (error) {
				console.error(new Date().toISOString(), error);
			}
		});
	}

	self.batchCheckId = setTimeout(function () {
		checkBatchStatus(self);
	}, 3000);

	callback(null, false);
};


function checkBatchStatus(log) {
	// console.log('checkBatchStatus %d', log.batch.size());
	if (log.batch.size() > 0) {
		log.tableService.executeBatch(log.tableName, log.batch, (error/*, result*/) => {
			if (error) {
				return console.error(new Date().toISOString(), error);
			}

			log.batch.clear();
			log.emit('logged');
		});
	}
}

//
// ### function query (options, callback)
// #### @options {Object} Loggly-like query options for this instance.
// #### @callback {function} Continuation to respond to when complete.
// Query the transport. Options object is optional.
//
AzureLogger.prototype.query = function (options, callback) {
	if (typeof options === 'function') {
		callback = options;
		options = {};
	}

	const self = this;
	options = this.normalizeQuery(options);

	// ToDo: handle pagination
	const query = new azure.TableQuery()
		.where('PartitionKey eq ?', self.partitionKey)
		.and('RowKey le ?', (DATE_MAX - options.from.getTime()).toString())
		.and('RowKey ge ?', (DATE_MAX - options.until.getTime()).toString())
		.top(options.rows);

	this.tableService.queryEntities(self.tableName, query, null, function (queryError, queryResult) {
		if (queryError) {
			return callback(queryError);
		}

		var entityResult = [],
			entities = queryResult.entries;

		for (var i = 0; i < entities.length; i++) {
			var entity = {};
			for (var key in entities[i]) {
				if (key === '.metadata') {
					continue;
				}

				if (options.fields) {
					if (options.fields.indexOf(key) === -1) {
						continue;
					}
				}

				entity[key] = entities[i][key]._;
			}

			entityResult.push(entity);
		}

		if (options.order !== 'desc') {
			entityResult = entityResult.reverse();
		}

		return callback(null, entityResult);
	});
};
