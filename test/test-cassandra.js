/*global describe:true, it:true, before:true, after:true */

var
	chai   = require('chai'),
	assert = chai.assert,
	expect = chai.expect,
	should = chai.should()
	;

var
	_                = require('lodash'),
	fs               = require('fs'),
	path             = require('path'),
	polyclay         = require('polyclay'),
	scamandrios      = require('scamandrios'),
	util             = require('util'),
	uuid             = require('node-uuid'),
	CassandraAdapter = require('../index')
	;

var testDir = process.cwd();
if (path.basename(testDir) !== 'test')
	testDir = path.join(testDir, 'test');



describe('cassandra adapter', function()
{
	var testKSName = 'polyclay_unit_tests';
	var modelDefinition =
	{
		properties:
		{
			id:            'uuid',
			name:          'string',
			time_id:       'timeuuid',
			id_list:       'list:uuid',
			time_id_list:  'list:timeuuid',
			id_set:        'set:uuid',
			time_id_set:   'set:timeuuid',
			id_map:        'map:uuid',
			time_id_map:   'map:timeuuid',
			emails_list:   'list:string',
			created:       'date',
			foozles:       'array',
			snozzers:      'hash',
			is_valid:      'boolean',
			count:         'number',
			floating:      'number',
			required_prop: 'string',
			primes:        'set:number',
			pet_types:     'set:string',
			expiries:      'set:date',
			timestamps:    'list:date',
			vaccinated:    'map:boolean',
			birthdays:     'map:date',
			pet_names:     'map:string',
			pet_counts:    'map:number'
		},
		optional:   [ 'computed', 'ephemeral' ],
		required:   [ 'name', 'is_valid', 'required_prop'],
		singular:   'model',
		plural:     'models',
		initialize: function()
		{
			this.ran_init = true;
		}
	};

	var Model, instance, another, hookTest, hookid;
	var connection;

	before(function()
	{
		Model = polyclay.Model.buildClass(modelDefinition);
		polyclay.persist(Model, 'id');
	});

	it('can connect to cassandra', function(done)
	{
		connection = new scamandrios.Connection({
			hosts:    ['localhost:9160'],
		});

		connection.on('error', function(err)
		{
			console.error(err.name, err.message);
			throw(err);
		});

		connection.connect().then(function(keyspace)
		{
			done();
		}, function(err) { should.not.exist(err); });
	});

	it('can be configured for database access', function()
	{
		var options =
		{
			connection: connection,
			keyspace: 'polyclay_unit_tests',
		};

		Model.setStorage(options, CassandraAdapter);
		Model.adapter.should.be.ok;
		Model.adapter.connection.should.be.ok;
		Model.adapter.constructor.should.equal(Model);
		Model.adapter.family.should.equal(Model.prototype.plural);
	});

	it('adds an error listener to any connection it constructs', function()
	{
		var listeners = Model.adapter.connection.listeners('error');
		listeners.should.be.an('array');
		listeners.length.should.be.above(0);
	});

	it('provision creates a keyspace and one table', function(done)
	{
		Model.provision(function(err, response)
		{
			should.not.exist(err);
			response.should.equal('OK');
			done();
		});
	});

	it('provision may be called twice with no ill effects', function(done)
	{
		Model.provision(function(err, response)
		{
			should.not.exist(err);
			response.should.equal('OK');
			done();
		});
	});

	it('throws when asked to save a document without a key', function()
	{
		var noID = function()
		{
			var obj = new Model();
			obj.name = 'idless';
			obj.save(function(err, reply)
			{
			});
		};

		noID.should.throw(Error);
	});

	it('can save a document in the db', function(done)
	{
		instance = new Model();
		instance.update(
		{
			id:            uuid.v4(),
			name:          'test',
			time_id:       uuid.v1(),
			created:       Date.now(),
			foozles:       ['three', 'two', 'one'],
			snozzers:      { field: 'value' },
			is_valid:      true,
			count:         3,
			floating:      3.14159,
			required_prop: 'requirement met',
			computed:      17,
			expiries:      [Date.now()],
			timestamps:    [Date.now(), new Date()],
			id_list:       [uuid.v1(), uuid.v4()],
			time_id_list:  [uuid.v1(), uuid.v1()],
			id_set:        [uuid.v4(), uuid.v1()],
			time_id_set:   [uuid.v1(), uuid.v1()],
			id_map:        { 'v1': uuid.v1(), 'v4': uuid.v4() },
			time_id_map:   { '1a': uuid.v1(), '1b': uuid.v1() }
		});

		instance.save(function(err, reply)
		{
			should.not.exist(err);
			reply.should.be.ok;
			done();
		});
	});

	it('can retrieve the saved document', function(done)
	{
		Model.get(instance.key, function(err, retrieved)
		{
			should.not.exist(err);
			retrieved.should.be.ok;
			retrieved.should.be.an('object');
			retrieved.key.should.equal(instance.key);
			retrieved.name.should.equal(instance.name);
			retrieved.is_valid.should.equal(instance.is_valid);
			retrieved.count.should.equal(instance.count);
			retrieved.floating.should.equal(instance.floating);
			retrieved.computed.should.equal(instance.computed);
			retrieved.created.getTime().should.equal(instance.created.getTime());

			retrieved.time_id.should.equal(instance.time_id);

			retrieved.id_list.should.deep.equal(instance.id_list);
			retrieved.time_id_list.should.deep.equal(instance.time_id_list);

			var retrievedIds = retrieved.id_set.sort();
			var retrievedTimeIds = retrieved.time_id_set.sort();

			// The `slice` call is necessary because the adapter adds an
			// `_iset` property to the original.
			var originalIds = instance.id_set.slice(0).sort();
			var originalTimeIds = instance.time_id_set.slice(0).sort();

			retrievedIds.should.deep.equal(originalIds);
			retrievedTimeIds.should.deep.equal(originalTimeIds);

			retrieved.id_map.should.deep.equal(instance.id_map);
			retrieved.time_id_map.should.deep.equal(instance.time_id_map);

			retrieved.isDirty().should.not.be.ok;

			done();
		});
	});

	it('can update the document', function(done)
	{
		instance.name = "New name";
		instance.isDirty().should.be.true;
		instance.save(function(err, response)
		{
			should.not.exist(err);
			response.should.be.a('string');
			response.should.equal('OK');
			instance.isDirty().should.equal(false);
			done();
		});
	});

	var testBatch;

	it('can fetch in batches', function(done)
	{
		var ids = [ instance.key ];
		testBatch = new Model();
		testBatch.name = 'two';
		testBatch.key = uuid.v4();
		testBatch.save(function(err, response)
		{
			should.not.exist(err);
			ids.push(testBatch.key);

			Model.get(ids, function(err, itemlist)
			{
				should.not.exist(err);
				itemlist.should.be.an('array');
				itemlist.length.should.equal(2);
				done();
			});
		});
	});

	it('the adapter get() can handle an id or an array of ids', function(done)
	{
		var ids = [ instance.key, testBatch.key ];
		Model.adapter.get(ids, function(err, itemlist)
		{
			should.not.exist(err);
			itemlist.should.be.an('array');
			itemlist.length.should.equal(2);
			done();
		});
	});

	it('can fetch all', function(done)
	{
		Model.all(function(err, itemlist)
		{
			should.not.exist(err);
			itemlist.should.be.an('array');
			itemlist.length.should.equal(2);
			done();
		});
	});

	var setModel;

	it('can save a document with a set field', function(done)
	{
		setModel = new Model();
		setModel.update(
		{
			id:            uuid.v4(),
			name:          'has-set',
			created:       Date.now(),
			pet_types:     ['cat', 'dog', 'coati'],
			primes:        [3, 5, 7, 11, 13]
		});

		setModel.save(function(err, reply)
		{
			should.not.exist(err);
			reply.should.be.ok;
			done();
		});
	});

	it('can retrieve a document with a set field', function(done)
	{
		Model.get(setModel.key, function(err, obj)
		{
			should.not.exist(err);
			obj.name.should.equal('has-set');
			obj.pet_types.length.should.equal(3);
			obj.pet_types.indexOf('cat').should.equal(0);
			obj.pet_types.indexOf('coati').should.equal(1);
			obj.pet_types.indexOf('dog').should.equal(2);
			obj.primes.length.should.equal(5);
			obj.primes[0].should.equal(3);
			done();
		});
	});

	it('inflates empty sets', function(done)
	{
		var emptySetModel = new Model();
		emptySetModel.update(
		{
			id:            uuid.v4(),
			name:          'has-empty-set',
			created:       Date.now(),
			pet_types:     [],
			primes:        []
		});

		emptySetModel.save(function(err, reply)
		{
			should.not.exist(err);
			reply.should.be.ok;

			Model.get(emptySetModel.key, function(err, obj)
			{
				should.not.exist(err);
				obj.name.should.equal('has-empty-set');
				obj.pet_types.should.deep.equal([]);
				obj.primes.should.deep.equal([]);
				done();
			});
		});
	});

	it('can save a document with collections of dates', function(done)
	{
		var model = new Model();

		model.update(
		{
			id:            uuid.v4(),
			name:          'has-set-date',
			created:       Date.now(),
			expiries:      [new Date(Date.UTC(2013, 6, 10)), 1373328000000, '2013-07-08T00:00:00.000Z'],
			timestamps:    [new Date(Date.UTC(2013, 6, 10)), 1373414400000, '2013-07-10T00:00:00.000Z']
		});

		model.save(function(err, reply)
		{
			should.not.exist(err);
			reply.should.be.ok;

			Model.get(model.key, function(err, model)
			{
				should.not.exist(err);
				model.name.should.equal('has-set-date');
				model.timestamps.should.deep.equal([new Date(Date.UTC(2013, 6, 10)), new Date(Date.UTC(2013, 6, 10)), new Date(Date.UTC(2013, 6, 10))]);

				var expiries = _.sortBy(model.expiries, function(expiry) { return +expiry; });
				expiries.should.deep.equal([new Date(Date.UTC(2013, 6, 8)), new Date(Date.UTC(2013, 6, 9)), new Date(Date.UTC(2013, 6, 10))]);

				done();
			});
		});
	});

	var mapBooleanModel;

	it('can save a document with a map:boolean field', function(done)
	{
		mapBooleanModel = new Model();
		mapBooleanModel.update(
		{
			id:            uuid.v4(),
			name:          'has-map-boolean',
			created:       Date.now(),
			vaccinated:    { 'cat': true, 'dog': true, 'coati': false },
		});

		mapBooleanModel.save(function(err, reply)
		{
			should.not.exist(err);
			reply.should.be.ok;
			done();
		});
	});

	it('can retrieve a document with a map:boolean field', function(done)
	{
		Model.get(mapBooleanModel.key, function(err, obj)
		{
			should.not.exist(err);
			obj.name.should.equal('has-map-boolean');
			obj.vaccinated.should.be.an('object');
			Object.keys(obj.vaccinated).length.should.equal(3);
			obj.vaccinated.should.have.property('cat');
			obj.vaccinated.cat.should.equal(true);
			obj.vaccinated.dog.should.equal(true);
			obj.vaccinated.coati.should.equal(false);
			done();
		});
	});

	it('can save a document with a map:date field containing a date-like value', function(done)
	{
		var obj = new Model();
		obj.update(
		{
			id:            uuid.v4(),
			name:          'has-map-date-like-field',
			created:       Date.now(),
			birthdays:     { 'Mina': 1154390400000 }
		});

		obj.save(function(err, reply)
		{
			should.not.exist(err);
			reply.should.be.ok;

			Model.get(obj.key, function(err, model)
			{
				should.not.exist(err);
				model.name.should.equal('has-map-date-like-field');
				model.birthdays.should.deep.equal({ 'Mina': new Date(Date.UTC(2006, 7, 1)) });
				done();
			});
		});
	});

	var mapDateModel;

	it('can save a document with a map:date field', function(done)
	{
		mapDateModel = new Model();
		mapDateModel.update(
		{
			id:            uuid.v4(),
			name:          'has-map-date',
			created:       Date.now(),
			birthdays:     { 'Mina': new Date(2006, 7, 1) },
		});

		mapDateModel.save(function(err, reply)
		{
			should.not.exist(err);
			reply.should.be.ok;
			done();
		});
	});

	it('can retrieve a document with a map:date field', function(done)
	{
		Model.get(mapDateModel.key, function(err, obj)
		{
			should.not.exist(err);
			obj.name.should.equal('has-map-date');
			obj.birthdays.should.be.an('object');
			Object.keys(obj.birthdays).length.should.equal(1);
			obj.birthdays.should.have.property('Mina');
			obj.birthdays.Mina.should.be.a('date');
			done();
		});
	});

	var mapStringModel;

	it('can save a document with a map:string field', function(done)
	{
		mapStringModel = new Model();
		mapStringModel.update(
		{
			id:            uuid.v4(),
			name:          'has-map-string',
			created:       Date.now(),
			pet_names:     { 'cat': 'Mina', 'dog': 'Pixel', coati: 'Rex' },
		});

		mapStringModel.save(function(err, reply)
		{
			should.not.exist(err);
			reply.should.be.ok;
			done();
		});
	});

	it('can retrieve a document with a map:string field', function(done)
	{
		Model.get(mapStringModel.key, function(err, obj)
		{
			should.not.exist(err);
			obj.name.should.equal('has-map-string');
			obj.pet_names.should.be.an('object');
			Object.keys(obj.pet_names).length.should.equal(3);
			obj.pet_names.cat.should.equal('Mina');
			obj.pet_names.dog.should.equal('Pixel');
			obj.pet_names.coati.should.equal('Rex');
			done();
		});
	});

	var mapNumberModel;

	it('can save a document with a map:number field', function(done)
	{
		mapNumberModel = new Model();
		mapNumberModel.update(
		{
			id:            uuid.v4(),
			name:          'has-map-number',
			created:       Date.now(),
			pet_counts:    { 'cat': 1, 'dog': 2, coati: 4.5 }
		});

		mapNumberModel.save(function(err, reply)
		{
			should.not.exist(err);
			reply.should.be.ok;
			done();
		});
	});

	it('can retrieve a document with a map:number field', function(done)
	{
		Model.get(mapNumberModel.key, function(err, obj)
		{
			should.not.exist(err);
			obj.name.should.equal('has-map-number');
			obj.pet_counts.should.be.an('object');
			Object.keys(obj.pet_counts).length.should.equal(3);
			obj.pet_counts.cat.should.equal(1);
			obj.pet_counts.dog.should.equal(2);
			obj.pet_counts.coati.should.equal(4.5);
			done();
		});
	});

	it('inflates empty maps', function(done)
	{
		var obj = new Model();
		obj.update(
		{
			id:            uuid.v4(),
			name:          'has-empty-map',
			created:       Date.now(),
			vaccinated:    {}
		});

		obj.save(function(err, reply)
		{
			should.not.exist(err);
			reply.should.be.ok;

			Model.get(obj.key, function(err, obj)
			{
				should.not.exist(err);
				obj.name.should.equal('has-empty-map');
				obj.vaccinated.should.deep.equal({});
				done();
			});
		});
	});

	it('constructMany() retuns an empty list when given empty input', function(done)
	{
		Model.constructMany([], function(err, results)
		{
			should.not.exist(err);
			results.should.be.an('array');
			results.length.should.equal(0);
			done();
		});
	});

	it('merge() updates properties then saves the object', function(done)
	{
		Model.get(testBatch.key, function(err, item)
		{
			should.not.exist(err);

			item.merge({ is_valid: true, count: 1023 }, function(err, response)
			{
				should.not.exist(err);
				Model.get(item.key, function(err, stored)
				{
					should.not.exist(err);
					stored.count.should.equal(1023);
					stored.is_valid.should.equal(true);
					stored.name.should.equal(item.name);
					done();
				});
			});
		});
	});

	it('can remove a document from the db', function(done)
	{
		instance.destroy(function(err, deleted)
		{
			should.not.exist(err);
			deleted.should.be.ok;
			instance.destroyed.should.be.true;
			done();
		});
	});

	it('can remove documents in batches', function(done)
	{
		var obj2 = new Model();
		obj2.key = uuid.v4();
		obj2.name = 'two';
		obj2.save(function(err, response)
		{
			Model.get(testBatch.key, function(err, obj)
			{
				should.not.exist(err);
				obj.should.be.an('object');

				var itemlist = [obj, obj2.key];
				Model.destroyMany(itemlist, function(err, response)
				{
					should.not.exist(err);
					// TODO examine response more carefully
					done();
				});
			});
		});
	});

	it('destroyMany() does nothing when given empty input', function(done)
	{
		Model.destroyMany(null, function(err)
		{
			should.not.exist(err);
			done();
		});
	});

	it('destroy responds with an error when passed an object without an id', function(done)
	{
		var obj = new Model();
		obj.destroy(function(err, destroyed)
		{
			err.should.be.an('object');
			err.message.should.equal('cannot destroy object without an id');
			done();
		});
	});

	it('destroy responds with an error when passed an object that has already been destroyed', function(done)
	{
		var obj = new Model();
		obj.key = uuid.v4();
		obj.destroyed = true;
		obj.destroy(function(err, destroyed)
		{
			err.should.be.an('object');
			err.message.should.equal('object already destroyed');
			done();
		});
	});

	it('inflate() handles bad json by assigning properties directly', function()
	{
		var bad =
		{
			name: 'this is not valid json'
		};
		var result = Model.adapter.inflate(bad);
		result.name.should.equal(bad.name);
	});

	it('inflate() does not construct an object when given a null payload', function()
	{
		var result = Model.adapter.inflate(null);
		assert.equal(result, undefined, 'inflate() created a bad object!');
	});

	after(function(done)
	{
		connection.executeCQL(new Buffer('DROP KEYSPACE polyclay_unit_tests;'))
		.then(function(response)
		{
			done();
		}).
		fail(function(err)
		{
			console.log(err.why);
			should.not.exist(err);
		})
		.done();
	});

});
