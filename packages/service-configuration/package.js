Package.describe({
  summary: 'Manage the configuration for third-party services',
  version: '1.3.1',
});

Package.onUse(function(api) {
  api.use('accounts-base', ['client', 'server']);
  api.use('mongo', ['client', 'server']);
  api.use('ecmascript', ['client', 'server']);
  api.export('ServiceConfiguration');
  api.mainModule('service_configuration_common.js', 'client');
  api.mainModule('service_configuration_server.js', 'server');
  api.addAssets('service-configuration.d.ts', 'server');
});
