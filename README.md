polyclay-cassandra
==============

A cassandra persistence adapter for [Polyclay](https://github.com/ceejbot/polyclay).

[![Build Status](https://secure.travis-ci.org/ceejbot/polyclay.png)](http://travis-ci.org/ceejbot/polyclay)

## How-to

For the redis adapter, specify host & port of your redis server. The 'dbname' option is used to namespace keys. The redis adapter will store models in hash keys of the form <dbname>:<key>. It will also use a set at key <dbname>:ids to track model ids.

```javascript
var polyclay = require('polyclay'),
    RedisAdapter = require('polyclay-redis');

var RedisModelFunc = polyclay.Model.buildClass({
    properties:
    {
        name: 'string',
        description: 'string'
    },
    singular: 'widget',
    plural: 'widgets'
});
polyclay.persist(Widget);


polyclay.persist(RedisModelFunc, 'name');

var options =
{
    host: 'localhost',
    port: 6379,
    dbname: 'widgets' // optional
};
RedisModelFunc.setStorage(options, RedisAdapter);
```

The redis client is available at obj.adapter.redis. The db name falls back to the model plural if you don't include it. The dbname is used to namespace model keys.

