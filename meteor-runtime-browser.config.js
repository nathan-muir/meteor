import alias from '@rollup/plugin-alias';
import replace from '@rollup/plugin-replace';
import inject from '@rollup/plugin-inject';
import path from 'node:path';

const projectRootDir = path.resolve(__dirname);

const packages = [
  { name: 'meteor', globals: ['Meteor'], main: 'main_client.js' },
  { name: 'ejson', globals: ['EJSON'], main: 'ejson.js' },
  // { name: 'ddp', globals: ['DDP'], main: '../ddp-client/client/client.js' },
  { name: 'ddp-client', globals: ['DDP'], main: 'client/client.js' },
  { name: 'ddp-common', globals: ['DDPCommon'], main: 'namespace.js' },
  { name: 'mongo-id', globals: ['MongoID'], main: 'id.js' },
  { name: 'random', globals: ['Random'], main: 'main_client.js' },
  { name: 'tracker', globals: ['Tracker'], main: 'tracker.js' },
  { name: 'reload', globals: ['Reload'], main: 'reload.js' },
  { name: 'mongo', globals: ['Mongo'], main: 'collection.js' },
  { name: 'minimongo', globals: ['Minimongo', 'LocalCollection'], main: 'minimongo_client.js' },
  { name: 'diff-sequence', globals: ['DiffSequence'], main: 'diff.js' },
  { name: 'geojson-utils', globals: ['GeoJSON'], main: 'main.js' },
  { name: 'id-map', globals: ['IdMap'], main: 'id-map.js' },
  { name: 'ordered-dict', globals: ['OrderedDict'], main: 'ordered_dict.js' },
  { name: 'mongo-decimal', globals: [], main: 'decimal.js', packageDir: 'packages/non-core' },
  { name: 'reactive-dict', globals: ['ReactiveDict'], main: 'migration.js' },
  { name: 'reactive-var', globals: ['ReactiveVar'], main: 'reactive-var.js' },
  { name: 'accounts-base', globals: ['Accounts'], main: 'client_main.js' },
  { name: 'localstorage', globals: [], main: 'localstorage.js' },
  { name: 'check', globals: ['check', 'Match'], main: 'match.js' },
  { name: 'callback-hook', globals: ['Hook'], main: 'hook.js' },
  { name: 'url', globals: [], main: 'modern.js' },
  { name: 'service-configuration', globals: ['ServiceConfiguration'], main: 'service_configuration_common.js' },
  { name: 'accounts-password', globals: [], main: 'password_client.js' },
  { name: 'allow-deny', globals: ['AllowDeny'], main: 'allow-deny.js' },
  { name: 'logging', globals: ['Log'], main: 'logging.js' },
  { name: 'socket-stream-client', globals: [], main: 'browser.js' },
  { name: 'retry', globals: ['Retry'], main: 'retry.js' },
  { name: 'base64', globals: ['Base64'], main: 'base64.js' },
  { name: 'sha', globals: ['SHA256'], main: 'sha256.js' },
  { name: 'http', globals: ['HTTP'], main: 'httpcall_client.js', packageDir: 'packages/deprecated' },
  { name: 'session', globals: ['Session'], main: 'session.js' },
]

const injects = packages.map(p => {
  if (p.globals.length) {
    return inject({
      modules: Object.fromEntries(
        p.globals.map(name => [name, [`meteor/${p.name}`, name]])
      ),
      exclude: `${p.packageDir || 'packages'}/${p.name}/**`
    })
  }
  return null;
}).filter(i => i != null)

const aliases = packages.flatMap(p => [
  {
    find: new RegExp(`^meteor/${p.name}/(.*)`),
    replacement: path.resolve(projectRootDir, `${p.packageDir || 'packages'}/${p.name}/$1`)
  },
  {
    find: `meteor/${p.name}`,
    replacement: path.resolve(projectRootDir, `${p.packageDir || 'packages'}/${p.name}/${p.main}`)
  },
])

export default [
  {
    input: './meteor-runtime-browser.js',
    output: {
      file: './meteor-runtime-browser.esm.js',
      format: 'es',
    },
    external: [
      'decimal.js',
    ],
    plugins: [
      replace({
        preventAssignment: true,
        values: {
          'Meteor.isServer': 'false',
          'Meteor.isClient': 'true',
          'Meteor.isCordova': 'false',
          'Package.autopublish': 'undefined',
          'Package.blaze': 'undefined',
          'Package.mongo': 'undefined',
          'Package.insecure': 'undefined',
        }
      }),
      inject({
        _: 'underscore',
      }),
      ...injects,
      alias({
        entries: [
          {
            find: 'meteor/underscore',
            replacement: 'underscore'
          },
          ...aliases
        ]
      })
    ]
  }
]
