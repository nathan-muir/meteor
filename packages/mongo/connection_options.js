/**
 * @summary Allows for user specified connection options
 * @example http://mongodb.github.io/node-mongodb-native/3.0/reference/connecting/connection-settings/
 * @locus Server
 * @param {Object} options User specified Mongo connection options
 */
let _connectionOptions = null;
export function setConnectionOptions (options) {
  check(options, Object);
  _connectionOptions = options;
}

export function getConnectionOptions() {
  return _connectionOptions || {};
}
