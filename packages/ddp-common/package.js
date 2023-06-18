Package.describe({
  summary: "Code shared beween ddp-client and ddp-server",
  version: '1.4.0',
  documentation: null
});

Package.onUse(function (api) {
  api.use([
    'check',
    'random',
    'ecmascript',
    'ejson',
    'tracker',
    'retry',
  ], ['client', 'server']);

  api.mainModule('namespace.js');
  api.export('DDPCommon');
});

Package.onTest(function (api) {
  // XXX we should write unit tests for heartbeat
});
