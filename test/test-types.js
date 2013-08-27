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
            obj.valid().should.equal(true);

            obj.birthdays.push('Invalid Date');
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

            obj.petBirthdays['coati'] = 'string';
            obj.valid().should.equal(false);
        });
    });

    describe('#uuid type', function()
    {
        var UUIDMapModel;

        it('adds a uuid type', function()
        {
            var mapDef =
            {
                properties:
                {
                    id:         'uuid',
                    timestamps: 'set:uuid'
                },
                required: ['id']
            };
            UUIDMapModel = polyclay.Model.buildClass(mapDef);

            var obj = new UUIDMapModel();
            obj.should.have.property('id');
            should.equal(obj.id, null);

            obj.should.have.property('timestamps');
            obj.timestamps.should.be.an('array');
        });

        it('fails the is-valid check if one of the values is not a uuid', function()
        {
            var obj = new UUIDMapModel();
            obj.timestamp = Date.now();

            should.equal(obj.id, null);
            obj.valid().should.equal(true);

            obj.id = uuid.v4();
            obj.valid().should.equal(true);

            obj.id = uuid.v1();
            obj.valid().should.equal(true);

            function shouldThrow()
            {
                obj.id = 'coati';
            }
            shouldThrow.should.throw(Error);

            obj.timestamps.push(uuid.v1());
            obj.valid().should.equal(true);

            obj.timestamps.push(uuid.v4());
            obj.valid().should.equal(true);

            obj.timestamps.push('coati');
            obj.valid().should.equal(false);
        });
    });

    describe('#timeuuid type', function()
    {
        var TimeUUIDMapModel;

        it('adds a timeuuid type', function()
        {
            var mapDef =
            {
                properties:
                {
                    id:   'timeuuid',
                    pets: 'map:timeuuid'
                },
                required: ['id']
            };
            TimeUUIDMapModel = polyclay.Model.buildClass(mapDef);

            var obj = new TimeUUIDMapModel();
            obj.should.have.property('id');
            should.equal(obj.id, null);

            obj.should.have.property('pets');
            obj.pets.should.be.an('object');
        });

        it('fails the is-valid check if one of the values is not a timeuuid', function()
        {
            var obj = new TimeUUIDMapModel();
            obj.timestamp = Date.now();

            should.equal(obj.id, null);
            obj.valid().should.equal(true);

            obj.id = uuid.v1();
            obj.valid().should.equal(true);

            function shouldThrow()
            {
                obj.id = uuid.v4();
            }
            shouldThrow.should.throw(Error);

            obj.pets['Bishonen'] = uuid.v1();
            obj.pets['Mina'] = uuid.v1();
            obj.valid().should.equal(true);

            obj.pets['coati'] = uuid.v4();
            obj.valid().should.equal(false);

            obj.pets['kiwi'] = 'string';
            obj.valid().should.equal(false);
        });
    });

});
