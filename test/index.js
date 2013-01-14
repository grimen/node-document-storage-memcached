
// -----------------------
//  Test
// --------------------

var Storage = require('node-document-storage');

module.exports = Storage.Spec('Memcached', {
  module: require('..'),
  engine: require('memcached'),
  db: 'default-test',
  default_url: 'memcache://localhost:11211/default-test',
  authorized_url: 'memcache://776617:00112ab7bcb3dc6ad345@dev1.ec2.memcachier.com:11211/test',
  unauthorized_url: 'memcache://776617:123@dev1.ec2.memcachier.com:11211/test',
  client: {
    get: function(db, type, id, callback) {
      var key = [db, type, id].join('/');

      var client = new (require('memcached'))('localhost:11211');

      client.get(key, function(err, res) {
        callback(err, res || null);
      });
    },

    set: function(db, type, id, data, callback) {
      var key = [db, type, id].join('/');

      var client = new (require('memcached'))('localhost:11211');

      client.set(key, data, 0, function(err, res) {
        callback(err, !!res);
      });
    },

    del: function(db, type, id, callback) {
      var key = [db, type, id].join('/');

      var client = new (require('memcached'))('localhost:11211');

      client.del(key, function(err, res) {
        callback(err, !!res);
      });
    },

    exists: function(db, type, id, callback) {
      var key = [db, type, id].join('/');

      var client = new (require('memcached'))('localhost:11211');

      client.append(key, null, function(err, res) {
        callback(err, !!res);
      });
    }
  }
});
