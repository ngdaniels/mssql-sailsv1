var _ = require('lodash');
var mssql = require('mssql');
var Query = require('../helpers/query');
var sql = require('../helpers/sql.js');
var utils = require('../helpers/utils');
var CursorJoin = require('waterline-cursor');

/**
 * mssql-sailsv1
 *
 * Most of the methods below are optional.
 *
 * If you don't need / can't get to every method, just implement
 * what you have time for.  The other methods will only fail if
 * you try to call them!
 *
 * For many adapters, this file is all you need.  For very complex adapters,
 * you may need more flexiblity. In any case, it's probably a good idea to
 * start with one file and refactor only if necessary. If you do go that route,
 * it's conventional in Node to create a `./lib` directory for your private
 * submodules and load them at the top of the file with other dependencies.
 * e.g. var update = `require('./lib/update')`;
 */
module.exports = (function mssqlSails() {

  // You'll want to maintain a reference to each connection
  // that gets registered with this adapter.
  var connections = { };

  // You may also want to store additional, private data
  // per-connection (esp. if your data store uses persistent
  // connections).
  //
  // Keep in mind that models can be configured to use different databases
  // within the same app, at the same time.
  //
  // i.e. if you're writing a MariaDB adapter, you should be aware that one
  // model might be configured as `host="localhost"` and another might be using
  // `host="foo.com"` at the same time.  Same thing goes for user, database,
  // password, or any other config.
  //
  // You don't have to support this feature right off the bat in your
  // adapter, but it ought to get done eventually.
  //

  var adapter = {
	
	identity: 'mssql-sailsv1',
	
    // Waterline Adapter API Version
    adapterApiVersion: 1,
	
	//  ╔═╗═╗ ╦╔═╗╔═╗╔═╗╔═╗  ┌─┐┬─┐┬┬  ┬┌─┐┌┬┐┌─┐
	//  ║╣ ╔╩╦╝╠═╝║ ║╚═╗║╣   ├─┘├┬┘│└┐┌┘├─┤ │ ├┤
	//  ╚═╝╩ ╚═╩  ╚═╝╚═╝╚═╝  ┴  ┴└─┴ └┘ ┴ ┴ ┴ └─┘
	//  ┌┬┐┌─┐┌┬┐┌─┐┌─┐┌┬┐┌─┐┬─┐┌─┐┌─┐
	//   ││├─┤ │ ├─┤└─┐ │ │ │├┬┘├┤ └─┐
	//  ─┴┘┴ ┴ ┴ ┴ ┴└─┘ ┴ └─┘┴└─└─┘└─┘
	// This allows outside access to the connection manager.
	datastores: connections,
	
    // Set to true if this adapter supports (or requires) things like data
    // types, validations, keys, etc. If true, the schema for models using this
    // adapter will be automatically synced when the server starts. Not
    // terribly relevant if your data store is not SQL/schemaful.  If setting
    // syncable, you should consider the migrate option, which allows you to
    // set how the sync will be performed. It can be overridden globally in an
    // app (config/adapters.js) and on a per-model basis.  IMPORTANT: `migrate`
    // is not a production data migration solution! In production, always use
    // `migrate: safe`  drop   => Drop schema and data, then recreate it alter
    // => Drop/add columns as necessary. safe   => Don't change anything (good
    // for production DBs)
    syncable: true,

    // Default configuration for connections
    defaults: {
      port: process.env.MSSQL_PORT || 1433,
      host: process.env.MSSQL_HOST || 'localhost',
      database: process.env.MSSQL_DATABASE,
      user: process.env.MSSQL_USER,
      password: process.env.MSSQL_PASSWORD,
      schema: true,

      connectionTimeout: 60 * 1000,
      requestTimeout: 60 * 1000,
      persistent: false,

      options: {
        encrypt: false
      },

      pool: {
        min: 5,
        max: 30,
        idleTimeout: 300 * 1000
      }
    },

    /**
     *
     * This method runs when a model is initially registered
     * at server-start-time.  This is the only required method.
     *
     * @param  {[type]}   connection [description]
     * @param  {[type]}   collection [description]
     * @param  {Function} cb         [description]
     * @return {[type]}              [description]
     */
    registerDatastore: function(connection, collections, cb) {console.log();
      if (!connection.identity) return cb(new Error('Connection is missing an identity.'));
      if (connections[connection.identity]) return cb(new Error('Connection is already registered.'));

      // Add in logic here to initialize connection
      // e.g. connections[connection.identity] = new Database(connection,
      // collections);
      connections[connection.identity] = {
        identity: connection.identity,
		datastores: connections,
		config: connection,
        collections: collections
      };

      setImmediate(function done() {
        return cb();
      });
    },

    /**
     * Ensures that the given connection is connected with the marshalled
     * configuration.
     * @param {String} connection
     * @param {Function} cb
     */
    connectConnection: function(connection, cb){
      var uniqId = _.uniqueId();
      var isPersistent = connections[connection].config.persistent;
	  var connection = connections[connection];
  
      if (isPersistent && (!connection.mssqlConnection || !connection.mssqlConnection._connected)) {
          connection.mssqlConnection = new mssql.ConnectionPool(marshalConfig(connection.config));
          connection.mssqlConnection.connect().then(function() {
		  	cb(undefined, uniqId);
		  }).catch(function(err){
		  	cb(err, uniqId);
		  });
      }
      else if (!isPersistent && (!connection.mssqlConnection || !connection.mssqlConnection[uniqId] || !connection.mssqlConnection[uniqId]._connected)) {
        if (!connection.mssqlConnection) {
            connection.mssqlConnection = {};
        }
  
        connection.mssqlConnection[uniqId] = new mssql.ConnectionPool(marshalConfig(connection.config));
        connection.mssqlConnection[uniqId].connect().then(function() {
			cb(undefined, uniqId);
        }).catch(function(err){
			cb(err, uniqId);
		});
      }
      else {
        _.defer(cb);
      }
    },


    /**
     * Fired when a model is unregistered, typically when the server
     * is killed. Useful for tearing-down remaining open connections,
     * etc.
     *
     * @param  {Function} cb [description]
     * @return {[type]}      [description]
     */
    // Teardown a Connection
    teardown: function (conn, cb) {
      if (typeof conn == 'function') {
        cb = conn;
        conn = null;
      }
      if (!conn) {
        _.each(connections, function (c) {
          if (c.persistent) {
            c.mssqlConnection && c.mssqlConnection.close();
          }
          else {
            _.each(c.mssqlConnection, function(handle) {
              handle && handle.close();
            });
          }
        });
        connections = { };
        return cb();
      }
      if (!connections[conn]) return cb();

      if (connections[conn].persistent) {
        connections[conn].mssqlConnection.close();
      }
      else {
        _.each(connections[conn], function(handle) {
          handle.mssqlConnection && handle.mssqlConnection.close();
        });
      }
      delete connections[conn];

      cb();
    },
	/* 
		Creaty Query: 
		To Do: Set description
	*/
	create: function (connection, values, cb) {
      var identityInsert = false;
      var pk = adapter.getPrimaryKey(connection, values.using);
      //console.log('pk=', pk);
    	
      // Remove primary key if the value is NULL. This allows the auto-increment
      // to work properly if set.
      if (_.isNull(values.newRecord[pk])) {
        delete values.newRecord[pk];
      }
      	  
      Object.keys(values.newRecord).forEach(function(key) {
        values.newRecord[key] = utils.prepareValue(values.newRecord[key]);
        if (pk == key) {
          identityInsert = true;
          //console.log(pk, '==', key);
        }
      });
      var schemaName = getSchemaName(connection, values.using);
      var tableName =  '[' + schemaName + ']' + '.[' + values.using + ']';
      var statement = sql.insertQuery(tableName, values.newRecord, pk);
      if (identityInsert) {
        statement = 'SET IDENTITY_INSERT '+ tableName + ' ON; '+
          statement +
          'SET IDENTITY_INSERT '+ tableName + ' OFF;';
      }
      
      adapter.connectConnection(connection, function __CREATE__(err, uniqId) {
        if (err) {
          console.error(err);
          return cb(err);
        }
      
        uniqId = uniqId || false;
        var mssqlConnect;
        if (!uniqId) {
          mssqlConnect = connections[connection].mssqlConnection;
        }
        else {
          mssqlConnect = connections[connection].mssqlConnection[uniqId];
        }
      
        var request = new mssql.Request(mssqlConnect);
        request.query(statement, function(err, recordsets) {
          if (err) {
            console.error(err);
            return cb(err);
          }
          var recordset = recordsets.recordset.length ? recordsets.recordset[0] : {};
          var model = values.newRecord;
          if (recordset[pk]) {
            model = _.extend({}, values.newRecord, recordset);
          }
      
          var _query = new Query(connections[connection].collections[values.using].definition);
          var castValues = _query.cast(model);
      
          if (!connections[connection].persistent) {
            mssqlConnect && mssqlConnect.close();
          }
   	      castValues = values.meta && values.meta.fetch ? castValues : undefined;
          cb(err, castValues);
        });
      });
    },
	
	/* 
		CreatyEach Query: 
		To Do: Set description
	*/
	createEach: function (connection, values, cb) {
		var identityInsert = false;
		var pk = adapter.getPrimaryKey(connection, values.using);
		//console.log('pk=', pk);
		var schemaName = getSchemaName(connection, values.using);
		var tableName =  '[' + schemaName + ']' + '.[' + values.using + ']';
		var statement = '';
		// Remove primary key if the value is NULL. This allows the auto-increment
		// to work properly if set.
		_.each(values.newRecords, function removeNullPrimaryKey(record) {
			if (_.isNull(record[pk])) {
			  delete record[pk];
			}
			Object.keys(record).forEach(function(key) {
			  record[key] = utils.prepareValue(record[key]);
			  if (pk == key) {
			    identityInsert = true;
			    //console.log(pk, '==', key);
			  }
			});
			statement += sql.insertQuery(tableName, record, pk)+ "; ";
		});
		
		if (identityInsert) {
		statement = 'SET IDENTITY_INSERT '+ tableName + ' ON; '+
		  statement +
		  'SET IDENTITY_INSERT '+ tableName + ' OFF;';
		}
		
		//console.log('create statement:', statement);
		
		adapter.connectConnection(connection, function __CREATE__(err, uniqId) {
		if (err) {
		  console.error(err);
		  return cb(err);
		}
		
		uniqId = uniqId || false;
		var mssqlConnect;
		if (!uniqId) {
		  mssqlConnect = connections[connection].mssqlConnection;
		}
		else {
		  mssqlConnect = connections[connection].mssqlConnection[uniqId];
		}
		
		var request = new mssql.Request(mssqlConnect);
		request.query(statement, function(err, recordsets) {
		  if (err) {
		    console.error(err);
		    return cb(err);
		  }
		  var recordset = recordsets.recordset;
		  var _query = new Query(connections[connection].collections[values.using].definition);
		  var castValues = [];
		  //Casting each values
		  _.each(recordsets.recordset, function removeNullPrimaryKey(record, key) {
		  	var model = values.newRecords[key];
		        if (record[pk]) {
		          model = _.extend({}, values.newRecords[key], record);
		        }
		
		        castValues.push(_query.cast(model));
		  });
		
		  //console.log('castValues', castValues);
		  if (!connections[connection].persistent) {
		    mssqlConnect && mssqlConnect.close();
		  }
		  castValues = values.meta && values.meta.fetch ? castValues : undefined;
		  cb(err, castValues);
		});
      });
	},

    /**
     *
     * REQUIRED method if users expect to call Model.find(), Model.findOne(),
     * or related.
     *
     * You should implement this method to respond with an array of instances.
     * Waterline core will take care of supporting all the other different
     * find methods/usages.
     *
     */
    find: function (connection, options, cb) {
    // Check if this is an aggregate query and that there is something to return
      if (options.groupBy || options.sum || options.average || options.min || options.max) {
        if (!options.sum && !options.average && !options.min && !options.max) {
          return cb(new Error('Cannot groupBy without a calculation'));
        }
      }

      options.__primaryKey__ = adapter.getPrimaryKey(connection, options.using);
      var schemaName = getSchemaName(connection, options.using);
      var tableName =  '[' + schemaName + ']' + '.[' + options.using + ']';
      var statement = sql.selectQuery(tableName, options.criteria); 
      adapter.connectConnection(connection, function __FIND__(err, uniqId) {
        if (err) {
          console.error(err);
          return cb(err);
        }

        uniqId = uniqId || false;
        var mssqlConnect;
        if (!uniqId) {
          mssqlConnect = connections[connection].mssqlConnection;
        }
        else {
          mssqlConnect = connections[connection].mssqlConnection[uniqId];
        }

        var request = new mssql.Request(mssqlConnect);
        //console.log('find:', statement);
        request.query(statement, function(err, recordset) {
          if (err) return cb(err);
          if (!connections[connection].persistent) {
            mssqlConnect && mssqlConnect.close();
          } 
          cb(undefined, recordset.recordset);
        });

      });
    },

    update: function (connection, query, cb) {
      var schemaName = getSchemaName(connection, query.using);
      var tableName =  '[' + schemaName + ']' + '.[' + query.using + ']';

      var criteria = sql.serializeOptions(query.using, query.criteria);

      var pk = adapter.getPrimaryKey(connection, query.using);

      var statement = 'SELECT [' + pk + '] FROM' + tableName + ' ' + criteria;
      adapter.connectConnection(connection, function __UPDATE__(err, uniqId) {
        if (err) {
          console.error(err);
          return cb(err);
        }

        uniqId = uniqId || false;
        var mssqlConnect;
        if (!uniqId) {
          mssqlConnect = connections[connection].mssqlConnection;
        }
        else {
          mssqlConnect = connections[connection].mssqlConnection[uniqId];
        }

        var request = new mssql.Request(mssqlConnect);
        request.query(statement, function(err, recordset) {
          if (err) return cb(err);
          //console.log('updating pks', recordset);
          if (_.isEmpty(recordset.recordset)) {
            return cb(null, [ ]);
          }

          var pks = [];
          recordset.recordset.forEach(function(row) {
            pks.push(row[pk]);
          });

          Object.keys(query.valuesToSet).forEach(function(key) {
            query.valuesToSet[key] = utils.prepareValue(query.valuesToSet[key]);
          });

          delete query.valuesToSet[pk];

          statement = 'UPDATE ' + tableName + ' SET ' + sql.updateCriteria(query.using, query.valuesToSet) + ' ';
          statement += sql.serializeOptions(query.using, query.criteria);

          request.query(statement, function(err, _recordset) {
            if (err) return cb(err);

            var criteria;

            if (pks.length === 1) {
              criteria = { where: {}, limit: 1 };
              criteria.where[pk] = pks[0];
            } else {
              criteria = { where: {}};
              criteria.where[pk] = pks;
            }

            if (!connections[connection].persistent) {
              mssqlConnect && mssqlConnect.close();
            }
			criteria = {using: query.using, criteria: criteria};
			if (query.meta && query.meta.fetch)
            	return adapter.find(connection, criteria, cb);
			return cb();
          });
        });
      });
    },

    destroy: function (connection, options, cb) {
      var schemaName = getSchemaName(connection, options.using);
      var tableName =  '[' + schemaName + ']' + '.[' + options.using + ']';
      var statement = 'DELETE FROM '+tableName;
      statement += sql.serializeOptions(options.using, options.criteria);
      adapter.connectConnection(connection, function __DELETE__(err, uniqId) {
        if (err) {
          console.error(err);
          return cb(err);
        }
		
		uniqId = uniqId || false;
		var mssqlConnect;
		if (!uniqId) {
		  mssqlConnect = connections[connection].mssqlConnection;
		}
		else {
		  mssqlConnect = connections[connection].mssqlConnection[uniqId];
		}
		
		if (options.meta && options.meta.fetch){
		  adapter.find(connection, options, function (err, records){
            if (err) return cb(err);

            var request = new mssql.Request(mssqlConnect);
            request.query(statement, function(err, emptyDeleteRecordSet) {
              if (err) return cb(err);
              if (!connections[connection].persistent) {
                mssqlConnect && mssqlConnect.close();
              }
              cb(null, records);
            });
          });
		}else{
		  var request = new mssql.Request(mssqlConnect);
		  request.query(statement, function(err, emptyDeleteRecordSet) {
		    if (err) return cb(err);
		    if (!connections[connection].persistent) {
		      mssqlConnect && mssqlConnect.close();
		    }
		    cb();
		  });
		}
      });
    },

    join: function (connectionName, criteria, cb) { 
      if (_.isObject(criteria)) {
        delete criteria.select;
      }

      CursorJoin({
        instructions: criteria,
        parentCollection: criteria.using,
        //nativeJoins: true,

        $find: function (collectionIdentity, query, cb) {
		  var opt = query.using ? query : {using: collectionIdentity, criteria: query};
		  return adapter.find(connectionName, opt, cb);
        },

        $getPK: function (collectionIdentity) {
          if (!collectionIdentity) return; 
          return adapter.getPrimaryKey(connectionName, collectionIdentity);
        }
      }, cb);
    },
	
	/* 
		Count Query: Return the number of matching records.
	*/
	count: function (connection, options, cb) {
		var schemaName = getSchemaName(connection, options.using);
	    var tableName =  '[' + schemaName + ']' + '.[' + options.using + ']';
	    var statement = sql.selectQuery(tableName, options.criteria); 
	    adapter.connectConnection(connection, function __FIND__(err, uniqId) {
	        if (err) {
	          console.error(err);
	          return cb(err);
	        }
	
	        uniqId = uniqId || false;
	        var mssqlConnect;
	        if (!uniqId) {
	          mssqlConnect = connections[connection].mssqlConnection;
	        }
	        else {
	          mssqlConnect = connections[connection].mssqlConnection[uniqId];
	        }
	
	        var request = new mssql.Request(mssqlConnect);
	        //console.log('find:', statement);
	        request.query(statement, function(err, recordset) {
	          if (err) return cb(err);
	          if (!connections[connection].persistent) {
	            mssqlConnect && mssqlConnect.close();
	          } 
	          cb(undefined, recordset.recordset.length);
	        });
	
	    });
	},
	
	// Return attributes
    describe: function (connection, collection, cb) {
      // Add in logic here to describe a collection (e.g. DESCRIBE TABLE logic)
      var schemaName = getSchemaName(connection, collection);
      var statement = "SELECT c.name AS ColumnName,TYPE_NAME(c.user_type_id) AS TypeName,c.is_nullable AS Nullable,c.is_identity AS AutoIncrement,ISNULL((SELECT is_unique FROM sys.indexes i LEFT OUTER JOIN sys.index_columns ic ON i.index_id=ic.index_id WHERE i.object_id=t.object_id AND ic.object_id=t.object_id AND ic.column_id=c.column_id),0) AS [Unique],ISNULL((SELECT is_primary_key FROM sys.indexes i LEFT OUTER JOIN sys.index_columns ic ON i.index_id=ic.index_id WHERE i.object_id=t.object_id AND ic.object_id=t.object_id AND ic.column_id=c.column_id),0) AS PrimaryKey,ISNULL((SELECT COUNT(*) FROM sys.indexes i LEFT OUTER JOIN sys.index_columns ic ON i.index_id=ic.index_id WHERE i.object_id=t.object_id AND ic.object_id=t.object_id AND ic.column_id=c.column_id),0) AS Indexed FROM sys.tables t INNER JOIN sys.columns c ON c.object_id=t.object_id LEFT OUTER JOIN sys.index_columns ic ON ic.object_id=t.object_id WHERE t.name='" + collection + "' AND OBJECT_SCHEMA_NAME(t.object_id) = '" + schemaName + "'";
      adapter.connectConnection(connection, function __DESCRIBE__(err, uniqId) {
        if (err) {
          console.error(err);
          return cb(err);
        }

        uniqId = uniqId || false;
        var mssqlConnect;
        if (!uniqId) {
          mssqlConnect = connections[connection].mssqlConnection;
        }
        else {
          mssqlConnect = connections[connection].mssqlConnection[uniqId];
        }

        var request = new mssql.Request(mssqlConnect);
        request.query(statement, function(err, recordset) {
          if (err) return cb(err);
          if (recordset.length === 0) return cb();
          var normalizedSchema = sql.normalizeSchema(recordset);
          connections[connection].config.schema = normalizedSchema;
          if (!connections[connection].persistent) {
            mssqlConnect && mssqlConnect.close();
          }
          cb(null, normalizedSchema);
        });
      });
    },

    /**
     *
     * REQUIRED method if integrating with a schemaful
     * (SQL-ish) database.
     *
     */
    define: function (connection, collection, definition, cb, meta) {
      // Add in logic here to create a collection (e.g. CREATE TABLE logic)
      adapter.connectConnection(connection, function __DEFINE__(err, uniqId) {
        if (err) {
          console.error(err);
          return cb(err);
        }
        var schema = sql.schema(collection, definition);
        var schemaName = getSchemaName(connection, collection)
        var tableName =  '[' + schemaName + ']' + '.[' + collection + ']';
        var statement = 'CREATE TABLE ' + tableName + ' (' + schema + ')';

        uniqId = uniqId || false;
        var mssqlConnect;
        if (!uniqId) {
          mssqlConnect = connections[connection].mssqlConnection;
        }
        else {
          mssqlConnect = connections[connection].mssqlConnection[uniqId];
        }

        var request = new mssql.Request(mssqlConnect);
        request.query(statement, function(err, recordset) {
          if (err) return cb(err);
          if (!connections[connection].persistent) {
            mssqlConnect && mssqlConnect.close();
          }
          cb(null, {});
        });

      });
    },

    /**
     *
     * REQUIRED method if integrating with a schemaful
     * (SQL-ish) database.
     *
     */
    drop: function (connection, collection, relations, cb, meta) {
      // Add in logic here to delete a collection (e.g. DROP TABLE logic)
      var schemaName = getSchemaName(connection, collection);
      var tableName =  '[' + schemaName + ']' + '.[' + collection + ']';
      var statement = 'IF OBJECT_ID(\''+ tableName + '\', \'U\') IS NOT NULL DROP TABLE '+tableName ;
      adapter.connectConnection(connection, function __DROP__(err, uniqId) {
        if (err) {
          console.error(err);
          return cb(err);
        }

        uniqId = uniqId || false;
        var mssqlConnect;
        if (!uniqId) {
          mssqlConnect = connections[connection].mssqlConnection;
        }
        else {
          mssqlConnect = connections[connection].mssqlConnection[uniqId];
        }

        var request = new mssql.Request(mssqlConnect);
        request.query(statement, function (err) {
          if (err) return cb(err);
          if (!connections[connection].persistent) {
            mssqlConnect && mssqlConnect.close();
          }
          cb(null, {});
        });
      });
    },
	
	// Raw Query Interface
    query: function (connection, collection, query, data, cb) {
      if (_.isFunction(data)) {
        cb = data;
        data = null;
      }

      adapter.connectConnection(connection, function __FIND__(err, uniqId) {
        if (err) {
          console.error(err);
          return cb(err);
        }

        uniqId = uniqId || false;
        var mssqlConnect;
        if (!uniqId) {
          mssqlConnect = connections[connection].mssqlConnection;
        }
        else {
          mssqlConnect = connections[connection].mssqlConnection[uniqId];
        }

        var request = new mssql.Request(mssqlConnect);
        request.query(query, function (err, recordset) {
          if (err) return cb(err);
          if (connections[connection] && !connections[connection].persistent) {
            mssqlConnect && mssqlConnect.close();
          }
          cb(null, recordset);
        });

      });
    },

    getPrimaryKey: function (connection, collection) {
      var pk = 'id';
	  var collections = connections[connection].collections[collection];
	  var primaryKey = collections ? collections.primaryKey : '';
	  pk = collections ? collections.definition[primaryKey].columnName : pk;
      return pk;
    },

  };

  function marshalConfig(_config) {
    var config = _.defaults(_config, {
      server: _config.host,
      pool: {
        max: _config.pool.max,
        min: _config.pool.min,
        idleTimeoutMillis: _config.pool.idleTimeout * 1000
      }
    });
	
    return _config.url || config;
  }

  function getSchemaName (connection, collection) {
    var collectionObject = connections[connection].collections[collection];
	var schema = 'dbo';
	schema = (collectionObject && collectionObject.meta && collectionObject.meta.schemaName ) ? collectionObject.meta.schemaName : schema;
    return  schema;
  }

  return adapter;
})();
