Package.describe({
  name: 'tracker',
  summary: "Dependency tracker to allow reactive callbacks",
  version: '1.3.2',
});

Package.onUse(function (api) {
  api.use("ecmascript");
  api.addFiles("tracker.js", "server");
  api.addFiles("tracker_client.js", "client");
  api.export("Tracker");
  api.export("Deps");
  api.addAssets("tracker.d.ts", ["client", "server"]);
});

Package.onTest(function (api) {
  api.use('tinytest');
  api.use('test-helpers');
  api.use('tracker');
  api.addFiles('tracker_tests.js', 'client');
});
