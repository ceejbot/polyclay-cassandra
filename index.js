var
	_           = require('lodash'),
	assert      = require('assert'),
	P           = require('p-promise'),
	PolyClay    = require('polyclay'),
	scamandrios = require('scamandrios'),
	util        = require('util')
	;

var Query = scamandrios.Query;

//-------------------------------------------
// Add new types to polyclay

function typedListValidator(type, prop)
{
	if (prop === null) return true;
	if (!Array.isArray(prop)) return false;
	return _.every(prop, PolyClay.validate[type]);
}

function typedHashValidator(type, prop)
{
	if (_.isUndefined(prop) || prop === null) return true;
	if (!_.isObject(prop)) return false;
	return _.every(prop, function(value, key)
	{
		return PolyClay.validate[type](value);
	});
}

function typedSetValidator(type, prop)
{
	if (_.isUndefined(prop) || prop === null) return true;
	if (!Array.isArray(prop)) return false;
	var unique = _.uniq(prop);
	if (prop.length !== unique.length) return false;
	return _.every(prop, PolyClay.validate[type]);
}
_.each(['string', 'number', 'date', 'boolean', 'uuid', 'timeuuid'], function(type)
{
	PolyClay.addType(
	{
		name: 'list:' + type,
		defaultFunc: function() { return []; },
		validatorFunc: function(prop) { return typedListValidator(type, prop); }
	});

	PolyClay.addType(
	{
		name:          'map:' + type,
		defaultFunc:   function() { return {}; },
		validatorFunc: function(prop) { return typedHashValidator(type, prop); }
	});

	if (type !== 'boolean')
	{
		PolyClay.addType(
		{
			name:          'set:' + type,
			defaultFunc:   function() { return []; },
			validatorFunc: function(prop) { return typedSetValidator(type, prop); }
		});
	}
});

var regexUUID = /^[a-f\d]{8}-[a-f\d]{4}-[14][a-f\d]{3}-[89ab][a-f\d]{3}-[a-f\d]{12}$/i;
var regexTimeUUID = /^[a-f\d]{8}-[a-f\d]{4}-1[a-f\d]{3}-[89ab][a-f\d]{3}-[a-f\d]{12}$/i;

PolyClay.addType(
{
	name:          'uuid',
	defaultFunc:   function() { return null; },
	validatorFunc: function(prop) { return prop == null || regexUUID.test(prop); }
});

PolyClay.addType(
{
	name:          'timeuuid',
	defaultFunc:   function() { return null; },
	validatorFunc: function(prop) { return prop == null || regexTimeUUID.test(prop); }
});

//-------------------------------------------

function Defaults()
{
	this.hosts    = ['127.0.0.1:9160'];
	this.keyspace = 'test';
	this.user     = '';
	this.pass     = '';
	this.timeout  = 3000;
}

function CassandraAdapter()
{
	this.options        = new Defaults();
	this.constructor    = null;
	this.family         = null;
	this.tables         = {};
	this.connection     = null;
	this.withConnection = null;
	this.columnFamily   = null;
}

CassandraAdapter.prototype.configure = function(options, modelfunc)
{
	var self = this;

	_.assign(this.options, options);
	this.constructor = modelfunc;
	this.family = modelfunc.prototype.plural;

	// If you hand us a pool, we presume you have added an error listener.
	if (options.connection)
		this.connection = options.connection;
	else
	{
		this.connection = new scamandrios.ConnectionPool({
			hosts:        this.options.hosts, // ['localhost:9160'],
			user:         this.options.user,
			password:     this.options.password,
			timeout:      3000
		});

		this.connection.on('error', function(err)
		{
			console.error(err.name, err.message);
			throw(err);
		});
	}

	this.withConnection = this.connection.connect();
};

CassandraAdapter.prototype.getAttachmentTable = function()
{
	return P();
};

var typeToValidator =
{
	'string':        'text',
	'number':        'double',
	'boolean':       'boolean',
	'date':          'timestamp',
	'uuid':          'uuid',
	'timeuuid':      'timeuuid',
	'set:string':    'set<text>',
	'set:number':    'set<double>',
	'set:date':      'set<timestamp>',
	'set:uuid':      'set<uuid>',
	'set:timeuuid':  'set<timeuuid>',
	'list:string':   'list<text>',
	'list:number':   'list<double>',
	'list:date':     'list<timestamp>',
	'list:uuid':     'list<uuid>',
	'list:timeuuid': 'list<timeuuid>',
	'map:string':    'map<text, text>',
	'map:boolean':   'map<text, boolean>',
	'map:number':    'map<text, double>',
	'map:date':      'map<text, timestamp>',
	'map:uuid':      'map<text, uuid>',
	'map:timeuuid':  'map<text, timeuuid>',
	'array':         'text',
	'hash':          'text',
	'reference':     'text',
	'untyped':       'text'
};
CassandraAdapter.typeToValidator = typeToValidator;

CassandraAdapter.prototype.createTableAs =

CassandraAdapter.prototype.createModelTable = function()
{
	var self = this;

	var throwaway = new self.constructor();
	var properties = throwaway.propertyTypes();

	var query = 'CREATE TABLE ' + self.options.keyspace + '.' + self.family + '(';

	var cols = [];

	_.forOwn(properties, function(property, name)
	{
		var columnType = typeToValidator[property] || 'text';
		var part = name + ' ' + columnType;

		cols.push(part);
	});
	query += cols.join(', ');
	query += ', PRIMARY KEY (' + throwaway.keyfield + '))';

	return self.connection.cql(query)
	.then(function() { return 'OK'; })
	.fail(function(error)
	{
		if (/^Cannot add already existing/i.test(error.why))
			return 'OK';

		throw error;
	});
};

CassandraAdapter.prototype.createKeyspace = function()
{
	var self = this;

	var query = new Query("CREATE KEYSPACE {keyspace} WITH REPLICATION = {replication};")
	.params(
	{
		'keyspace': self.options.keyspace,
		replication: {'class' : 'SimpleStrategy', 'replication_factor': 3 }
	}).types({ replication: 'map<text, text>'});

	return query.execute(self.connection)
	.then(function()
	{
		return 'OK';
	}).fail(function(error)
	{
		if (/^Cannot add existing/i.test(error.why))
			return 'OK';

		throw error;
	});
};

CassandraAdapter.prototype.provision = function(callback)
{
	var self = this, isPool = false;

	self.withConnection
	.then(function(reply)
	{
		var query = new Query('SELECT keyspace_name FROM system.schema_keyspaces WHERE keyspace_name = {name};');
		query.types({ 'name': 'text' }).params({ 'name': self.options.keyspace });
		return query.execute(self.connection);
	})
	.then(function(results)
	{
		if (results.length) return true;
		return self.createKeyspace();
	})
	.then(function()
	{
		var query = new Query('use {keyspace};').params({'keyspace': self.options.keyspace});

		if (!Array.isArray(self.connection.clients))
			return query.execute(self.connection);

		var actions = _.map(self.connection.clients, function(conn)
		{
			return query.execute(conn);
		});

		return P.allSettled(actions);
	})
	.then(function() { return self.createModelTable(); })
	.then(function(response)
	{
		callback(null, response);
	})
	.fail(function(error)
	{
		console.log(error);
		callback(error);
	}).done();
};

CassandraAdapter.prototype.shutdown = function() {};

CassandraAdapter.prototype.save = function(obj, json, callback)
{
	assert(obj.key);
	var self = this;

	var properties = serialize(obj);
	var q1 = 'INSERT INTO {keyspace}.{family} (';
	var q2 = ') VALUES (';

	var types = {};
	var params = { 'family': this.family, 'keyspace': this.options.keyspace };

	var ctr = 0;
	_.forOwn(properties, function(v, k)
	{
		q1 += ('{c' + ctr + '}, ');
		q2 += '{v' + ctr + '}, ';

		params['c' + ctr] = k;
		params['v' + ctr] = v;

		types['v' + ctr] = typeToValidator[obj.propertyType(k)];
		ctr++;
	});
	q1 = q1.slice(0, q1.length - 2);
	q2 = q2.slice(0, q2.length - 2);
	var query = new Query(q1 + q2 + ')').types(types).params(params);

	return this.withConnection
	.then(function() { return query.execute(self.connection); })
	.then(function(resp) { callback(null, 'OK');}, callback)
	.done();
};

CassandraAdapter.prototype.update = CassandraAdapter.prototype.save;

CassandraAdapter.prototype.merge = function(key, properties, callback)
{
	var self = this;

	var throwaway = new this.constructor();

	var types = {};
	var params = { 'keyfield': throwaway.keyfield, 'key': key };

	var ctr = 0;
	var query = 'UPDATE ' + this.family + ' SET ';

	_.forOwn(properties, function(v, k)
	{
		query += '{c' + ctr + '} = {v' + ctr + '}, ';

		params['c' + ctr] = k;
		params['v' + ctr] = v;

		types['v' + ctr] = typeToValidator[throwaway.propertyType(k)];
		ctr++;
	});

	query = query.slice(0, query.length - 2);
	query += ' WHERE {keyfield} = {key}';

	this.withConnection
	.then(function() { return new Query(query).params(params).types(types).execute(self.connection); })
	.then(function()
	{
		callback(null, 'OK');
	}, callback)
	.done();
};

CassandraAdapter.prototype.saveAttachment = function(obj, attachment, callback)
{
	callback(null, 'OK');
};

CassandraAdapter.prototype.get = function(key, callback)
{
	var self = this,
		results = [];

	if (Array.isArray(key))
		return this.getBatch(key, callback);

	var throwaway = new this.constructor();

	var query = new Query('SELECT * from {keyspace}.{family} WHERE {keyfield} = {key}').params(
	{
		'keyspace': this.options.keyspace,
		'family': this.family,
		'keyfield': throwaway.keyfield,
		'key': key
	}).types(
	{
		'key': typeToValidator[throwaway.propertyType(throwaway.keyfield)]
	});

	query.execute(self.connection)
	.then(function(rows)
	{
		rows.forEach(function(row)
		{
			var props = {};
			row.forEach(function(n, v, ts, ttl)
			{
				props[n] = v;
			});

			results.push(self.inflate(props));
		});
		callback(null, results[0]);

	}, callback)
	.done();
};

CassandraAdapter.prototype.getBatch = function(keylist, callback)
{
	var results = [];
	var self = this;

	var throwaway = new this.constructor();
	var keyType = typeToValidator[throwaway.propertyType(throwaway.keyfield)];

	var filters = [];

	var types = {};
	var params = {};

	for (var i = 0, l = keylist.length; i < l; i++)
	{
		filters.push('{v' + i + '}');
		types['v' + i] = keyType;
		params['v' + i] = keylist[i];
	}

	var keystring = filters.join(', ');

	var query = new Query('SELECT * from {keyspace}.{family} WHERE {keyfield} IN (' + keystring + ')').params(
	{
		'keyspace': this.options.keyspace,
		'family': this.family,
		'keyfield': self.constructor.prototype.keyfield
	}).params(params).types(types);

	this.withConnection
	.then(function() { return query.execute(self.connection); })
	.then(function(rows)
	{
		rows.forEach(function(row)
		{
			var props = {};
			row.forEach(function(n, v, ts, ttl)
			{
				props[n] = v;
			});
			results.push(self.inflate(props));
		});

		callback(null, results);

	}, callback)
	.done();
};

CassandraAdapter.prototype.all = function(callback)
{
	var results = [];
	var self = this;

	var q = new Query('SELECT * from {keyspace}.{family}');

	q.params({ 'family': self.family, 'keyspace': self.options.keyspace });

	this.withConnection
	.then(function() { return q.execute(self.connection); })
	.then(function(rows)
	{
		rows.forEach(function(row)
		{
			var props = {};
			row.forEach(function(n, v, ts, ttl)
			{
				props[n] = v;
			});
			results.push(self.inflate(props));
		});

		callback(null, results, true);
	}, callback)
	.done();
};

CassandraAdapter.prototype.attachment = function(key, name, callback)
{
	callback();
};

CassandraAdapter.prototype.remove = function(obj, callback)
{
	var self = this;

	var query = new Query('DELETE FROM {keyspace}.{family} WHERE {keyfield} = {key}').params(
	{
		'keyspace': self.options.keyspace,
		'family': self.family,
		'keyfield': obj.keyfield,
		'key': obj.key
	}).types(
	{
		'key': typeToValidator[obj.propertyType(obj.keyfield)]
	});

	this.withConnection
	.then(function() { return query.execute(self.connection); })
	.then(function(res)
	{
		callback(null, 'OK');
	}, callback)
	.done();
};

CassandraAdapter.prototype.destroyMany = function(objlist, callback)
{
	var self = this;

	var throwaway = new this.constructor();
	var keyType = typeToValidator[throwaway.propertyType(throwaway.keyfield)];

	var keylist = _.map(objlist, function(item)
	{
		if (_.isObject(item))
			return item.key;
		else
			return item;
	});

	var filters = [];

	var types = {};
	var params = {};

	for (var i = 0, l = keylist.length; i < l; i++)
	{
		filters.push('{v' + i + '}');
		types['v' + i] = keyType;
		params['v' + i] = keylist[i];
	}

	var keystring = filters.join(', ');

	var query = new Query('DELETE from {keyspace}.{family} WHERE {keyfield} IN (' + keystring + ')').params(
	{
		'keyspace': this.options.keyspace,
		'family': this.family,
		'keyfield': self.constructor.prototype.keyfield
	}).params(params).types(types);

	this.withConnection
	.then(function() { return query.execute(self.connection); })
	.then(function(reply)
	{
		callback();
	}, callback)
	.done();
};

CassandraAdapter.prototype.removeAttachment = function(obj, name, callback)
{
	callback(null, 'OK');
};

var stringifyPat = /^(array|hash|reference|untyped)$/;

function serializeDate(d)
{
	var timestamp = +d;
	return isFinite(timestamp) ? timestamp : +new Date(d);
}

function serializeDateCollection(d, prop, collection)
{
	collection[prop] = serializeDate(d);
}

function serialize(obj)
{
	var struct = obj.serialize();

	var keys = Object.keys(struct);
	for (var i = 0; i < keys.length; i++)
	{
		var k = keys[i];
		var type = obj.propertyType(k);

		if (stringifyPat.test(type))
			struct[k] = JSON.stringify(struct[k]);
		else if (type.lastIndexOf('set:', 0) === 0)
		{
			if (struct[k].length === 0)
				delete struct[k];
			else
			{
				if (type === 'set:date')
					_.each(struct[k], serializeDateCollection);

				struct[k]._iset = true;
			}
		}
		else if (type === 'list:date')
			_.each(struct[k], serializeDateCollection);
		else if (type === 'date')
			struct[k] = serializeDate(struct[k]);
		else if (type.lastIndexOf('map:', 0) === 0)
		{
			if (_.isEmpty(struct[k]))
				delete struct[k];
			else if (type === 'map:date')
				_.forOwn(struct[k], serializeDateCollection);
		}
	}

	return struct;
}

function deserializeCollection(collection, type)
{
	if (collection == null || type == null)
		return [];

	return _.map(collection, function(value)
	{
		return deserialize(value, type);
	});
}

function deserializeMap(map, type)
{
	if (map == null || type == null)
		return {};

	var result = {};

	_.forOwn(map, function(value, key)
	{
		result[key] = deserialize(value, type);
	});

	return result;
}

function deserialize(value, type)
{
	switch (type)
	{
	case 'string':        return value;
	case 'boolean':       return value;
	case 'date':          return new Date(value);
	case 'number':        return value;
	case 'uuid':
	case 'timeuuid':      return value == null ? null : String(value);
	case 'untyped':       return value ? JSON.parse(value) : value;
	case 'array':         return JSON.parse(value);
	case 'hash':          return JSON.parse(value);
	case 'reference':     return JSON.parse(value);

	case 'list:string':
	case 'set:string':    return deserializeCollection(value, 'string');
	case 'list:date':
	case 'set:date':      return deserializeCollection(value, 'date');
	case 'list:uuid':
	case 'set:uuid':      return deserializeCollection(value, 'uuid');
	case 'list:timeuuid':
	case 'set:timeuuid':  return deserializeCollection(value, 'timeuuid');
	case 'list:number':
	case 'set:number':    return deserializeCollection(value, 'number');

	case 'map:string':    return deserializeMap(value, 'string');
	case 'map:boolean':   return deserializeMap(value, 'boolean');
	case 'map:date':      return deserializeMap(value, 'date');
	case 'map:uuid':      return deserializeMap(value, 'uuid');
	case 'map:timeuuid':  return deserializeMap(value, 'timeuuid');
	case 'map:number':    return deserializeMap(value, 'number');

	default:              return JSON.parse(value);
	}
}

CassandraAdapter.prototype.inflate = function(hash)
{
	if (!hash || !_.isObject(hash))
		return;

	var obj = new this.constructor();

	var converted = {};
	_.forOwn(hash, function(v, k)
	{
		var type = obj.propertyType(k);
		if (type != null)
			converted[k] = deserialize(v, type);
	});

	obj.initFromStorage(converted);
	return obj;
};

module.exports = CassandraAdapter;
