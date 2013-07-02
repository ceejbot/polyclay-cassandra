/*global describe:true, it:true, before:true, after:true */

var
	chai   = require('chai'),
	assert = chai.assert,
	expect = chai.expect,
	should = chai.should()
	;

var
	fs               = require('fs'),
	path             = require('path'),
	polyclay         = require('polyclay'),
	scamandrios      = require('scamandrios'),
	util             = require('util'),
	CassandraAdapter = require('../index')
	;

var testDir = process.cwd();
if (path.basename(testDir) !== 'test')
	testDir = path.join(testDir, 'test');
var attachmentdata = fs.readFileSync(path.join(testDir, 'test.png'));

describe('new polyclay types', function()
{
	describe('#set type', function()
	{
		var SetModel;

		it('adds a set type', function()
		{
			var setDef =
			{
				properties:
				{
					id:      'string',
					animals: 'set:string',
					silly:   'set:number'
				}
			};
			SetModel = polyclay.Model.buildClass(setDef);

			var obj = new SetModel();
			obj.should.have.property('animals');
			obj.animals.should.be.an('array');
			obj.silly.should.be.an('array');
		});

		it('enforces uniqueness in the validator', function()
		{
			var obj = new SetModel();
			obj.animals = ['cat', 'dog', 'coati'];
			obj.silly = [ 2, 4, 6];
			obj.valid().should.equal(true);

			obj.animals.push('coati');
			obj.valid().should.equal(false);
		});

	});

	describe('#list:string type', function()
	{
		var StringListModel;

		it('adds a list -> string type', function()
		{
			var listDef =
			{
				properties:
				{
					id:       'string',
					petNames: 'list:string'
				}
			};
			StringListModel = polyclay.Model.buildClass(listDef);

			var obj = new StringListModel();
			obj.should.have.property('petNames');
			obj.petNames.should.be.an('array');
		});

		it('fails the is-valid check if one of the elements is not a string', function()
		{
			var obj = new StringListModel();
			obj.key = 'bad';
			obj.petNames = ['Bishonen', 'Mina'];
			obj.valid().should.equal(true);

			obj.petNames.push(1);
			obj.valid().should.equal(false);
		});
	});

	describe('#list:number type', function()
	{
		var NumberListModel;

		it('adds a list -> number type', function(){
			var listDef =
			{
				properties:
				{
					id:           'string',
					luckyNumbers: 'list:number'
				}
			};
			NumberListModel = polyclay.Model.buildClass(listDef);

			var obj = new NumberListModel();
			obj.should.have.property('luckyNumbers');
			obj.luckyNumbers.should.be.an('array');
		});

		it('fails the is-valid check if one of the elements is not a number', function()
		{
			var obj = new NumberListModel();
			obj.key = 'bad';
			obj.luckyNumbers = [24, 47, 66];
			obj.valid().should.equal(true);

			obj.luckyNumbers.push('71');
			obj.valid().should.equal(false);
		});
	});

	describe('#list:date type', function()
	{
		var DateListModel;

		it('adds a list -> date type', function(){
			var listDef =
			{
				properties:
				{
					id:        'string',
					birthdays: 'list:date'
				}
			};
			DateListModel = polyclay.Model.buildClass(listDef);

			var obj = new DateListModel();
			obj.should.have.property('birthdays');
			obj.birthdays.should.be.an('array');
		});

		it('fails the is-valid check if one of the elements is not a date', function()
		{
			var obj = new DateListModel();
			obj.key = 'bad';
			obj.birthdays = [new Date(2002, 6, 14), new Date(2006, 7, 1)];
			obj.valid().should.equal(true);

			obj.birthdays.push(1154415600000);
			obj.valid().should.equal(false);
		});
	});

	describe('#list:boolean type', function()
	{
		var BooleanListModel;

		it('adds a list -> boolean type', function(){
			var listDef =
			{
				properties:
				{
					id:        'string',
					petNames:  'list:string',
					petsValid: 'list:boolean'
				}
			};

			BooleanListModel = polyclay.Model.buildClass(listDef);

			var obj = new BooleanListModel();

			obj.should.have.property('petNames');
			obj.petNames.should.be.an('array');

			obj.should.have.property('petsValid');
			obj.petsValid.should.be.an('array');
		});

		it('fails the is-valid check if one of the elements is not a boolean', function()
		{
			var obj = new BooleanListModel();
			obj.key = 'bad';

			obj.petNames = ['Bishonen', 'Mina'];
			obj.petsValid = [true, true];

			obj.valid().should.equal(true);

			obj.petsValid.push(1);
			obj.valid().should.equal(false);
		});
	});

	describe('#map:string type', function()
	{
		var StringMapModel;

		it('adds a map -> string type', function()
		{
			var mapDef =
			{
				properties:
				{
					id:            'string',
					petNames:      'map:string',
				}
			};
			StringMapModel = polyclay.Model.buildClass(mapDef);

			var obj = new StringMapModel();
			obj.should.have.property('petNames');
			obj.petNames.should.be.an('object');
		});

		it('fails the is-valid check if one of the values is not a string', function()
		{
			var obj = new StringMapModel();
			obj.key = 'bad';
			obj.petNames = {};
			obj.petNames['cat'] = 'Felix';
			obj.petNames['dog'] = 'Fido';
			obj.valid().should.equal(true);

			obj.petNames['coati'] = 4;
			obj.valid().should.equal(false);
		});
	});

	describe('#map:boolean type', function()
	{
		var BooleanMapModel;

		it('adds a map -> boolean type', function()
		{
			var mapDef =
			{
				properties:
				{
					id:            'string',
					petsValidated: 'map:boolean',
				}
			};
			BooleanMapModel = polyclay.Model.buildClass(mapDef);

			var obj = new BooleanMapModel();
			obj.should.have.property('petsValidated');
			obj.petsValidated.should.be.an('object');
		});

		it('fails the is-valid check if one of the values is not a boolean', function()
		{
			var obj = new BooleanMapModel();
			obj.key = 'bad';
			obj.petsValidated = {};
			obj.petsValidated['cat'] = true;
			obj.petsValidated['dog'] = false;
			obj.valid().should.equal(true);

			obj.petsValidated['coati'] = 4;
			obj.valid().should.equal(false);
		});
	});

	describe('#map:number type', function()
	{
		var NumberMapModel;

		it('adds a map -> number type', function()
		{
			var mapDef =
			{
				properties:
				{
					id:            'string',
					petCounts:     'map:number',
				}
			};
			NumberMapModel = polyclay.Model.buildClass(mapDef);

			var obj = new NumberMapModel();
			obj.should.have.property('petCounts');
			obj.petCounts.should.be.an('object');
		});

		it('fails the is-valid check if one of the values is not a number', function()
		{
			var obj = new NumberMapModel();
			obj.key = 'bad';
			obj.petCounts = {};
			obj.petCounts['cat'] = 16;
			obj.petCounts['dog'] = 2;
			obj.valid().should.equal(true);

			obj.petCounts['coati'] = '4';
			obj.valid().should.equal(false);
		});
	});

	describe('#map:date type', function()
	{
		var DateMapModel;

		it('adds a map -> date type', function()
		{
			var mapDef =
			{
				properties:
				{
					id:            'string',
					petBirthdays:  'map:date'
				}
			};
			DateMapModel = polyclay.Model.buildClass(mapDef);

			var obj = new DateMapModel();
			obj.should.have.property('petBirthdays');
			obj.petBirthdays.should.be.an('object');
		});

		it('fails the is-valid check if one of the values is not a number', function()
		{
			var obj = new DateMapModel();
			obj.key = 'bad';
			obj.petBirthdays = {};
			obj.petBirthdays['Bishonen'] = new Date(2002, 6, 14);
			obj.petBirthdays['Mina'] = new Date(2006, 7, 1);
			obj.valid().should.equal(true);

			obj.petBirthdays['coati'] = '4';
			obj.valid().should.equal(false);
		});
	});

});

describe('cassandra adapter', function()
{
	var testKSName = 'polyclay_unit_tests';
	var modelDefinition =
	{
		properties:
		{
			id:            'string',
			name:          'string',
			created:       'date',
			foozles:       'array',
			snozzers:      'hash',
			is_valid:      'boolean',
			count:         'number',
			floating:      'number',
			required_prop: 'string',
			primes:        'set:number',
			pet_types:     'set:string',
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

	it('provision creates a keyspace and two tables, I mean, column families', function(done)
	{
		Model.provision(function(err, response)
		{
			should.not.exist(err);
			response.should.equal('OK');
			Model.adapter.keyspace.should.be.an('object');
			Model.adapter.attachments.should.be.an('object');
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
			id:            '1',
			name:          'test',
			created:       Date.now(),
			foozles:       ['three', 'two', 'one'],
			snozzers:      { field: 'value' },
			is_valid:      true,
			count:         3,
			floating:      3.14159,
			required_prop: 'requirement met',
			computed:      17,
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

	it('can fetch in batches', function(done)
	{
		var ids = [ instance.key ];
		var obj = new Model();
		obj.name = 'two';
		obj.key = '2';
		obj.save(function(err, response)
		{
			ids.push(obj.key);

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
		var ids = [ '1', '2' ];
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

	it('can save a document with a set field', function(done)
	{
		var obj = new Model();
		obj.update(
		{
			id:            '3',
			name:          'has-set',
			created:       Date.now(),
			pet_types:     ['cat', 'dog', 'coati'],
			primes:        [3, 5, 7, 11, 13]
		});

		obj.save(function(err, reply)
		{
			should.not.exist(err);
			reply.should.be.ok;
			done();
		});
	});

	it('can retrieve a document with a set field', function(done)
	{
		Model.get('3', function(err, obj)
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

	it('can save a document with a map:boolean field', function(done)
	{
		var obj = new Model();
		obj.update(
		{
			id:            '4',
			name:          'has-map-boolean',
			created:       Date.now(),
			vaccinated:    { 'cat': true, 'dog': true, 'coati': false },
		});

		obj.save(function(err, reply)
		{
			should.not.exist(err);
			reply.should.be.ok;
			done();
		});
	});

	it('can retrieve a document with a map:boolean field', function(done)
	{
		Model.get('4', function(err, obj)
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

	it('can save a document with a map:date field', function(done)
	{
		var obj = new Model();
		obj.update(
		{
			id:            '5',
			name:          'has-map-date',
			created:       Date.now(),
			birthdays:     { 'Mina': new Date(2006, 7, 1) },
		});

		obj.save(function(err, reply)
		{
			should.not.exist(err);
			reply.should.be.ok;
			done();
		});
	});

	it('can retrieve a document with a map:date field', function(done)
	{
		Model.get('5', function(err, obj)
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

	it('can save a document with a map:string field', function(done)
	{
		var obj = new Model();
		obj.update(
		{
			id:            '6',
			name:          'has-map-string',
			created:       Date.now(),
			pet_names:     { 'cat': 'Mina', 'dog': 'Pixel', coati: 'Rex' },
		});

		obj.save(function(err, reply)
		{
			should.not.exist(err);
			reply.should.be.ok;
			done();
		});
	});

	it('can retrieve a document with a map:string field', function(done)
	{
		Model.get('6', function(err, obj)
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

	it('can save a document with a map:number field', function(done)
	{
		var obj = new Model();
		obj.update(
		{
			id:            '7',
			name:          'has-map-number',
			created:       Date.now(),
			pet_counts:    { 'cat': 1, 'dog': 2, coati: 4.5 }
		});

		obj.save(function(err, reply)
		{
			should.not.exist(err);
			reply.should.be.ok;
			done();
		});
	});

	it('can retrieve a document with a map:number field', function(done)
	{
		Model.get('7', function(err, obj)
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
		Model.get('2', function(err, item)
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

	it('can add an attachment type', function()
	{
		Model.defineAttachment('frogs', 'text/plain');
		Model.defineAttachment('avatar', 'image/png');

		instance.set_frogs.should.be.a('function');
		instance.fetch_frogs.should.be.a('function');
		var property = Object.getOwnPropertyDescriptor(Model.prototype, 'frogs');
		property.get.should.be.a('function');
		property.set.should.be.a('function');
	});

	it('can save attachments', function(done)
	{
		instance.avatar = attachmentdata;
		instance.frogs = 'This is bunch of frogs.';
		instance.isDirty().should.equal.true;
		instance.save(function(err, response)
		{
			should.not.exist(err);
			instance.isDirty().should.equal.false;
			done();
		});
	});

	it('can retrieve attachments', function(done)
	{
		Model.get(instance.key, function(err, retrieved)
		{
			should.not.exist(err);
			retrieved.should.be.ok;
			retrieved.should.be.an('object');

			retrieved.fetch_frogs(function(err, frogs)
			{
				should.not.exist(err);
				frogs.should.be.a('string');
				frogs.should.equal('This is bunch of frogs.');
				retrieved.fetch_avatar(function(err, imagedata)
				{
					should.not.exist(err);
					imagedata.should.be.ok;
					assert(Buffer.isBuffer(imagedata), 'expected image attachment to be a Buffer');
					imagedata.length.should.equal(attachmentdata.length);
					done();
				});
			});
		});
	});

	it('can update an attachment', function(done)
	{
		instance.frogs = 'Poison frogs are awesome.';
		instance.save(function(err, response)
		{
			should.not.exist(err);
			Model.get(instance.key, function(err, retrieved)
			{
				should.not.exist(err);
				retrieved.fetch_frogs(function(err, frogs)
				{
					should.not.exist(err);
					frogs.should.equal(instance.frogs);
					retrieved.fetch_avatar(function(err, imagedata)
					{
						should.not.exist(err);
						imagedata.length.should.equal(attachmentdata.length);
						done();
					});
				});
			});
		});
	});

	it('can store an attachment directly', function(done)
	{
		instance.frogs = 'Poison frogs are awesome, but I think sand frogs are adorable.';
		instance.saveAttachment('frogs', function(err, response)
		{
			should.not.exist(err);
			Model.get(instance.key, function(err, retrieved)
			{
				should.not.exist(err);
				retrieved.fetch_frogs(function(err, frogs)
				{
					should.not.exist(err);
					frogs.should.equal(instance.frogs);
					done();
				});
			});
		});
	});

	it('saveAttachment() clears the dirty bit', function(done)
	{
		instance.frogs = 'This is bunch of frogs.';
		instance.isDirty().should.equal(true);
		instance.saveAttachment('frogs', function(err, response)
		{
			should.not.exist(err);
			instance.isDirty().should.equal(false);
			done();
		});
	});

	it('can remove an attachment', function(done)
	{
		instance.removeAttachment('frogs', function(err, deleted)
		{
			should.not.exist(err);
			deleted.should.be.true;
			done();
		});
	});

	it('caches an attachment after it is fetched', function(done)
	{
		instance.avatar = attachmentdata;
		instance.save(function(err, response)
		{
			should.not.exist(err);
			instance.isDirty().should.be.false;
			instance.fetch_avatar(function(err, imagedata)
			{
				should.not.exist(err);
				var cached = instance.__attachments['avatar'].body;
				cached.should.be.okay;
				(cached instanceof Buffer).should.equal(true);
				polyclay.dataLength(cached).should.equal(polyclay.dataLength(attachmentdata));
				done();
			});
		});
	});

	it('can fetch an attachment directly', function(done)
	{
		Model.adapter.attachment('1', 'avatar', function(err, body)
		{
			should.not.exist(err);
			(body instanceof Buffer).should.equal(true);
			polyclay.dataLength(body).should.equal(polyclay.dataLength(attachmentdata));
			done();
		});
	});

	it('removes an attachment when its data is set to null', function(done)
	{
		instance.avatar = null;
		instance.save(function(err, response)
		{
			should.not.exist(err);
			Model.get(instance.key, function(err, retrieved)
			{
				should.not.exist(err);
				retrieved.fetch_avatar(function(err, imagedata)
				{
					should.not.exist(err);
					should.not.exist(imagedata);
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
		obj2.key = '4';
		obj2.name = 'two';
		obj2.save(function(err, response)
		{
			Model.get('2', function(err, obj)
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
		obj.key = 'foozle';
		obj.destroyed = true;
		obj.destroy(function(err, destroyed)
		{
			err.should.be.an('object');
			err.message.should.equal('object already destroyed');
			done();
		});
	});

	it('removes attachments when removing an object', function(done)
	{
		var adapter = Model.adapter;

		var obj = new Model();
		obj.key = 'cats';
		obj.frogs = 'Cats do not eat frogs.';
		obj.name = 'all about cats';

		obj.save(function(err, reply)
		{
			should.not.exist(err);
			reply.should.equal('OK');

			return obj.destroy(function(err, destroyed)
			{
				should.not.exist(err);

				var key = 'cats:frogs';
				var query = 'SELECT * from polyclay_unit_tests.' + adapter.attachfamily + ' WHERE key = ?';

				adapter.connection.cql(query, [key])
				.then(function(rows)
				{
					rows.should.be.ok;
					rows.should.be.an('array');
					rows.length.should.equal(0);
					done();
				}).fail(function(err) { should.not.exist.err; })
				.done();
			});
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
		connection.dropKeyspace('polyclay_unit_tests')
		.then(function(response)
		{
			done();
		}, function(err)
		{
			should.not.exist(err);
		});
	});

});
