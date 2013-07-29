polyclay-cassandra
==================

A cassandra persistence adapter for [Polyclay](https://github.com/ceejbot/polyclay).

[![Build Status](https://secure.travis-ci.org/ceejbot/polyclay-cassandra.png)](http://travis-ci.org/ceejbot/polyclay-cassandra)

This module relies on [scamandrios](https://github.com/ceejbot/scamandrios) for its cassandra driver.

## Installation

`npm install polyclay-cassandra polyclay`

Polyclay is a peer dependency of this module.

## Quick start

A quick example until more documentation is written:


```javascript
var polyclay = require('polyclay'),
    CassandraAdapter = require('polyclay-cassandra'),
    scamandrios = require('scamandrios')
    ;


var testKSName = 'polyclay_unit_tests';
var modelDefinition =
{
    properties:
    {
        key:           'string',
        name:          'string',
        created:       'date',
        foozles:       'array',
        snozzers:      'hash',
        is_valid:      'boolean',
        count:         'number',
        floating:      'number',
        map:            'string',
    },
    required:   [ 'name', 'is_valid', 'required_prop'],
    singular:   'model',
    plural:     'models'
};

Model = polyclay.Model.buildClass(modelDefinition);
polyclay.persist(Model);

connection = new scamandrios.Connection({
    hosts:    ['localhost:9160'],
});

var options =
{
    connection: connection,
    keyspace: 'polyclay_unit_tests',
};

Model.setStorage(options, CassandraAdapter);
```

The keyspace is available at `obj.adapter.keyspace`, the model column family at `obj.adapter.columnFamily`, and the attachments column family at `obj.adapter.attachments`. `adapter.provision()` creates keyspaces and column families if necessary. It is safe to call provision more than once; it will avoid trying to create the tables if they already exist.

## Cassandra types

The adapter adds several cassandra-specific types to the core javascript type list. The polyclay model definition names are in the left column. Cassandra types are in the right. No attempt has been made to implement map types with anything other than string keys.

| Polyclay type   | Cassandra type
| ==============: | :=============
| string          | text
| number          | double
| boolean         | boolean
| date            | timestamp
| uuid            | uuid
| timeuuid        | timeuuid
| set:string      | set<text>
| set:number      | set<double>
| set:date        | set<timestamp>
| set:uuid        | set<uuid>
| set:timeuuid    | set<timeuuid>
| list:string     | list<text>
| list:number     | list<double>
| list:date       | list<timestamp>
| list:uuid       | list<uuid>
| list:timeuuid   | list<timeuuid>
| map:string      | map<text, text>
| map:boolean     | map<text, boolean>
| map:number      | map<text, double>
| map:date        | map<text, timestamp>
| map:uuid        | map<text, uuid>
| map:timeuuid    | map<text, timeuuid>
| array           | text (json-stringified)
| hash            | text (json-stringified)
| reference       | text (json-stringified)

## Object inflation

TBD




