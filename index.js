var
	_           = require('lodash'),
	assert      = require('assert'),
	P           = require('p-promise'),
	PolyClay    = require('polyclay'),
	scamandrios = require('scamandrios'),
	util        = require('util')
	;

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
_.each(['string', 'number', 'date', 'boolean'], function(type)
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
	this.options      = new Defaults();
	this.constructor  = null;
	this.family       = null;
	this.attachfamily = null;
	this.tables       = {};
	this.connection   = null;
	this.withKeyspace = null;
	this.keyspace     = null;
	this.columnFamily = null;
	this.attachments  = null;
}

CassandraAdapter.prototype.configure = function(options, modelfunc)
{
	var self = this;

	_.assign(this.options, options);
	this.constructor = modelfunc;
	this.family = modelfunc.prototype.plural;
	this.attachfamily = this.family + '_attachments';

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

	this.withKeyspace = this.connection.connect().then(function()
	{
		return self.connection.assignKeyspace(self.options.keyspace);
	}).then(function(ks)
	{
		if (Array.isArray(ks))
		{
			var promise = _.find(ks, { 'state': 'fulfilled' });

			if (!promise)
			{
				var invalidError = new Error('Missing or invalid response.');
				_.assign(invalidError, { 'keyspace': self.options.keyspace, 'response': ks });
				throw invalidError;
			}

			self.keyspace = promise.value;
		}
		else
			self.keyspace = ks;

		return self.keyspace;
	});
};

CassandraAdapter.prototype.getAttachmentTable = function()
{
	return this.keyspace.getTableAs(this.attachfamily, 'attachments');
};

var typeToValidator =
{
	'string':      'text',
	'number':      'double',
	'boolean':     'boolean',
	'date':        'timestamp',
	'set:string':  'set<text>',
	'set:number':  'set<double>',
	'set:date':    'set<timestamp>',
	'map:string':  'map<text, text>',
	'map:boolean': 'map<text, boolean>',
	'map:number':  'map<text, double>',
	'map:date':    'map<text, timestamp>',
	'array':       'text',
	'hash':        'text',
	'reference':   'text',
};
CassandraAdapter.typeToValidator = typeToValidator;

CassandraAdapter.prototype.createTableAs = 

CassandraAdapter.prototype.createModelTable = function()
{
	var self = this;

	var throwaway = new self.constructor();
	var properties = throwaway.propertyTypes();

	var query = 'CREATE TABLE ' + self.family + '(';

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
	.fail(function(error)
	{
		if (/^Cannot add already existing/i.test(error.why))
			return 'OK';

		throw error;
	});
};

CassandraAdapter.prototype.createAttachmentsTable = function()
{
	var self = this;

	return this.keyspace.createTableAs(this.attachfamily, 'attachments',
	{
		description: 'polyclay ' + this.constructor.prototype.singular + ' attachments',
		columns:
		[
			{ name: 'name',         validation_class: 'UTF8Type'  },
			{ name: 'content_type', validation_class: 'UTF8Type'  },
			{ name: 'data',         validation_class: 'AsciiType' }
		]
	})
	.then(function(table)
	{
		self.attachments = table;
		return table;
	});
};

CassandraAdapter.prototype.provision = function(callback)
{
	var self = this;

	return this.withKeyspace
	.then(function()
	{
		return P.all(
		[
			self.createModelTable(),
			self.createAttachmentsTable()
		]);
	})
	.then(function()
	{
		callback(null, 'OK');
	}).fail(function(err) { callback(err); })
	.done();
};

CassandraAdapter.prototype.shutdown = function() {};

CassandraAdapter.prototype.save = function(obj, json, callback)
{
	assert(obj.key);
	var self = this;

	var properties = serialize(obj);
	var params = [];
	var q1 = 'INSERT INTO ' + this.family + '(';
	var q2 = ') VALUES (';

	_.forOwn(properties, function(v, k)
	{
		q1 += (k + ', ');
		q2 += '?, ';
		params.push(v);
	});
	q1 = q1.slice(0, q1.length - 2);
	q2 = q2.slice(0, q2.length - 2);
	var query = q1 + q2 + ')';

	return this.withKeyspace
	.then(function() { return self.connection.cql(query, params); })
	.then(function() { return self.createAttachmentsTable(); })
	.then(function() { return self.saveAttachments(obj.key, json._attachments); })
	.then(function(resp) { callback(null, 'OK');})
	.fail(function(err) { callback(err); })
	.done();
};

CassandraAdapter.prototype.update = CassandraAdapter.prototype.save;

CassandraAdapter.prototype.merge = function(key, properties, callback)
{
	var self = this;

	var params = [];
	var query = 'UPDATE ' + this.family + ' SET ';

	_.forOwn(properties, function(v, k)
	{
		query += (k + ' = ?, ');
		params.push(v);
	});

	query = query.slice(0, query.length - 2);
	query += ' WHERE ' + self.constructor.prototype.keyfield + ' = ?';
	params.push(key);

	return this.withKeyspace
	.then(function() { return self.connection.cql(query, params); })
	.then(function()
	{
		callback(null, 'OK');
	})
	.fail(function(err) { callback(err); })
	.done();
};

CassandraAdapter.prototype.saveAttachment = function(obj, attachment, callback)
{
	var self = this;
	var key = makeAttachKey(obj.key, attachment.name);

	var values =
	{
		name:         attachment.name,
		content_type: attachment.content_type,
		data:         new Buffer(attachment.body).toString('base64')
	};

	return this.getAttachmentTable()
	.then(function() { return self.attachments.insert(key, values); })
	.then(function(res)
	{
		callback(null, 'OK');
	})
	.fail(function(err) { callback(err); })
	.done();
};

CassandraAdapter.prototype.saveAttachments = function(key, attachments)
{
	if (!attachments || !_.isObject(attachments))
		return P('OK');
	var names = Object.keys(attachments);
	if (!names.length)
		return P('OK');

	var self = this;

	return this.getAttachmentTable()
	.then(function()
	{
		var actions = _.map(names, function(name)
		{
			var attach = attachments[name];
			var k = makeAttachKey(key, name);
			var values =
			{
				name:         name,
				content_type: attach.content_type,
				data:         attach.data // use the B64-encoded version
			};

			return self.attachments.insert(k, values);
		});

		return P.all(actions);
	});
};

CassandraAdapter.prototype.get = function(key, callback)
{
	var self = this,
		results = [];

	if (Array.isArray(key))
		return this.getBatch(key, callback);

	var query = 'SELECT * from ' + this.options.keyspace + '.' + this.family + ' WHERE ' + self.constructor.prototype.keyfield + ' = ?';

	this.withKeyspace.then(function() { return self.connection.cql(query, [key]); })
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

	})
	.fail(function(err) { callback(err); })
	.done();
};

function quote(string)
{
    return "'" + string.replace(/\'/g, "''") + "'";
}

CassandraAdapter.prototype.getBatch = function(keylist, callback)
{
	var results = [];
	var self = this;

	var keystring = _.map(keylist, quote).join(', ');
	var query = 'SELECT * from ' + this.options.keyspace + '.' + this.family + ' WHERE ' + self.constructor.prototype.keyfield + ' IN (' + keystring + ')';

	this.withKeyspace.then(function() { return self.connection.cql(query); })
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

	})
	.fail(function(err) { callback(err); })
	.done();
};

CassandraAdapter.prototype.all = function(callback)
{
	var results = [];
	var self = this;

	this.withKeyspace.then(function() { return self.connection.cql('SELECT * from ' + self.family); })
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
	})
	.fail(function(err) { callback(err); })
	.done();
};

function makeAttachKey(k, n)
{
	return k + ':' + n;
}

CassandraAdapter.prototype.attachment = function(key, name, callback)
{
	var results = [];
	var self = this;

	var cassKey = makeAttachKey(key, name);
	var query = 'SELECT * from ' + self.attachfamily + ' WHERE key = ?';

	this.withKeyspace.then(function() { return self.connection.cql(query, [cassKey]); })
	.then(function(rows)
	{
		if (rows.length === 0)
			return callback(null, null);

		var found = null;

		rows.forEach(function(row)
		{
			var props = {};
			row.forEach(function(n, v, ts, ttl)
			{
				props[n] = v;
			});

			if (props.data)
			{
				var b = new Buffer(props.data, 'base64');
				if (props.content_type.match(/text/))
					props.body = b.toString();
				else
					props.body = b;
			}

			if (props.name === name)
				found = props;
		});

		return callback(null, found ? found.body : null);
	})
	.fail(function(err) { callback(err); })
	.done();
};

CassandraAdapter.prototype.remove = function(obj, callback)
{
	var self = this;
	var query = 'DELETE FROM ' + self.family + ' WHERE ' + self.constructor.prototype.keyfield + ' = ?';

	this.withKeyspace.then(function() { return self.connection.cql(query, [obj.key]); })
	.then(function(reply)
	{
		return self.removeAllAttachments(obj.key);
	}).then(function(res)
	{
		callback(null, 'OK');
	})
	.fail(function(err) { callback(err); })
	.done();
};

CassandraAdapter.prototype.destroyMany = function(objlist, callback)
{
	var self = this;
	var keylist = _.map(objlist, function(item)
	{
		if (_.isObject(item))
			return item.key;
		else
			return item;
	});

	var actions = _.map(keylist, function(k)
	{
		return self.removeAllAttachments(k);
	});

	var keystring = _.map(keylist, quote).join(', ');
	var query = 'DELETE from ' + self.family + ' WHERE ' + self.constructor.prototype.keyfield + ' IN (' + keystring + ')';

	P.all(actions).then(function() { return self.connection.cql(query); })
	.then(function(reply)
	{
		callback();
	})
	.fail(function(err) { callback(err); })
	.done();
};

CassandraAdapter.prototype.removeAttachment = function(obj, name, callback)
{
	var self = this;
	var key = makeAttachKey(obj.key, name);
	this.getAttachmentTable()
	.then(function() { return self.attachments.remove(key); })
	.then(function(res) { callback(null, 'OK'); })
	.fail(function(err) { callback(err); })
	.done();
};

CassandraAdapter.prototype.removeAllAttachments = function(key)
{
	var self = this;

	// I feel that this is grotty.
	var props = _.filter(Object.keys(self.constructor.prototype), function(item)
	{
		return item.match(/^fetch_/);
	});

	var actions = _.map(props, function(p)
	{
		var akey = key + ':' + p.replace(/^fetch_/, '');
		return self.getAttachmentTable().then(function() { return self.attachments.remove(akey); });
	});

	return P.all(actions);
};

var stringifyPat = /^(array|hash|reference|untyped)$/;

function serializeDateMap(d, key, map)
{
	map[key] = d.getTime();
}

function serialize(obj)
{
	var struct = obj.serialize();
	var types = obj.propertyTypes();

	var keys = Object.keys(struct);
	for (var i = 0; i < keys.length; i++)
	{
		var k = keys[i];

		if (stringifyPat.test(types[k]))
			struct[k] = JSON.stringify(struct[k]);
		else if (types[k].indexOf('set:') === 0)
		{
			if (struct[k].length === 0)
				delete struct[k];
			else
				struct[k]._iset = true;
		}
		else if ('date' === types[k])
			struct[k] = struct[k].getTime();
		else if (types[k].indexOf('map:') === 0)
		{
			if (Object.keys(struct[k]).length === 0)
				delete struct[k];
			if (types[k] === 'map:date')
				_.each(struct[k], serializeDateMap);
		}
	}

	return struct;
}

function deserialize(value, type)
{
	switch (type)
	{
	case 'string':      return value;
	case 'boolean':     return value;
	case 'date':        return new Date(value);
	case 'number':      return value;
	case 'untyped':     return value ? JSON.parse(value) : value;
	case 'array':       return JSON.parse(value);
	case 'hash':        return JSON.parse(value);
	case 'reference':   return JSON.parse(value);
	case 'set:string':  return value;
	case 'set:date':    return value;
	case 'set:number':  return value;
	case 'map:string':  return value;
	case 'map:boolean': return value;
	case 'map:date':    return value;
	case 'map:number':  return value;
	default:            return JSON.parse(value);
	}
}

CassandraAdapter.prototype.inflate = function(hash)
{
	if (!hash || !_.isObject(hash))
		return;

	var obj = new this.constructor();
	var types = obj.propertyTypes();

	var converted = {};
	_.forOwn(hash, function(v, k)
	{
		var type = types[k];
		converted[k] = k === obj.keyfield ? v : deserialize(v, type);
	});

	obj.update(converted);
	return obj;
};

module.exports = CassandraAdapter;
