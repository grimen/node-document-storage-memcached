require('sugar');
var util = require('util');

// HACK: ...until Node.js `require` supports `instanceof` on modules loaded more than once. (bug in Node.js)
var Storage = global.NodeDocumentStorage || (global.NodeDocumentStorage = require('node-document-storage'));

// -----------------------
//  DOCS
// --------------------
//  - https://github.com/3rd-Eden/node-memcached

// -----------------------
//  TODO
// --------------------
//  - https://github.com/alevy/memjs (SASL-support)

// -----------------------
//  Constructor
// --------------------

// new Memcached ();
// new Memcached (options);
// new Memcached (url);
// new Memcached (url, options);
function Memcached () {
  var self = this;

  self.klass = Memcached;
  self.klass.super_.apply(self, arguments);

  var auth = [
        self.options.server.username,
        self.options.server.password
      ].compact().join(':');

  var domain_and_port = [
        self.options.server.hostname,
        self.options.server.port
      ].compact().join(':');

  self.options.server.endpoint = auth.isBlank() ? domain_and_port : [auth, domain_and_port].join('@');
}

util.inherits(Memcached, Storage);

// -----------------------
//  Class
// --------------------
Memcached.id = 'memcached';
Memcached.protocol = 'memcache';

Memcached.defaults = Memcached.defaults || {};
Memcached.defaults.url = Storage.env('MEMCACHED_URL') || 'memcache://localhost:11211/{db}-{env}'.assign({
  db: 'default',
  env: process.env.NODE_ENV || 'development'
});
Memcached.defaults.options = {
  client: {
    maxKeySize: 250,
    maxExpiration: 2592000,
    maxValue: 1048576,
    poolSize: 10,
    reconnect: 18000000,
    timeout: 5000,
    retries: 5,
    retry: 30000,
    remove: true,
    failOverServers: undefined,
    keyCompression: true
  }
};

Memcached.url = Memcached.defaults.url;
Memcached.options = Memcached.defaults.options;

Memcached.reset = Storage.reset;

// -----------------------
//  Instance
// --------------------

// #connect ()
Memcached.prototype.connect = function() {
  var self = this;

  self._connect(function() {
    var memcached = require('memcached');

    self.client = new memcached(self.options.server.endpoint, self.options.server);

    self.client.on('failure', function(err) {
      self.emit('error', err);
    });

    self.client.set('node-document-auth', 1, 10000, function(err) {
      self.authorized = !err;

      if (err) {
        self.emit('error', err);
      }
      self.emit('ready', err);
    });
  });
};

// #set (key, value, callback)
// #set (keys, values, callback)
Memcached.prototype.set = function() {
  var self = this;

  self._set(arguments, function(key_values, options, done, next) {
    key_values.each(function(key, value) {
      var resource = self.resource(key).trimmed;

      self.client.set(resource.path, value, 0, function(err, response) {
        next(key, err, !err, response);
      });
    });
  });
};

// #get (key, [options], callback)
// #get (keys, [options], callback)
Memcached.prototype.get = function() {
  var self = this;

  self._get(arguments, function(keys, options, done, next) {
    var paths = keys.map(function(key) {
      var resource = self.resource(key).trimmed;
      return resource.path;
    });

    self.client.get(paths, function(err, responses) {
      var result,
          results = [],
          errors = [];

      paths.each(function(key) {
        result = responses[key] || null;
        results.push(result);
        errors.push(err);
      });

      done(err, results, responses);
    });
  });
};

// #del (key, [options], callback)
// #del (keys, [options], callback)
Memcached.prototype.del = function() {
  var self = this;

  self._del(arguments, function(keys, options, done, next) {
    keys.each(function(key) {
      var resource = self.resource(key).trimmed;

      self.client.del(resource.path, function(err, response) {
        next(key, err, response, response);
      });
    });
  });
};

// #exists (key, [options], callback)
// #exists (keys, [options], callback)
Memcached.prototype.exists = function() {
  var self = this;

  self._exists(arguments, function(keys, options, done, next) {
    keys.each(function(key) {
      var resource = self.resource(key).trimmed;

      // HACK: Try to append `NULL` to KEY; if the key exists this operation returns TRUE, otherwise FALSE.
      self.client.append(resource.path, null, function(err, response) {
        next(key, err, response, response);
      });
    });
  });
};

// #end ()
Memcached.prototype.end = function() {
  var self = this;

  if (self.client) {
    self.client.end();
  }
};

// #pack (object)
Memcached.prototype.pack = JSON.stringify;

// #unpack (json)
Memcached.prototype.unpack = JSON.parse;

// -----------------------
//  Export
// --------------------

module.exports = Memcached;
