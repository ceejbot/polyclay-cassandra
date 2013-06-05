polyclay-cassandra
==============

A cassandra persistence adapter for [Polyclay](https://github.com/ceejbot/polyclay).

[![Build Status](https://secure.travis-ci.org/ceejbot/polyclay-cassandra.png)](http://travis-ci.org/ceejbot/polyclay-cassandra)

This module relies on [scamandrios](https://github.com/ceejbot/scamandrios) for its cassandra driver.

## Installation

`npm install polyclay-cassandra`

## How-to

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
        required_prop: 'string',
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

The redis client is available at obj.adapter.redis. The db name falls back to the model plural if you don't include it. The dbname is used to namespace model keys.

