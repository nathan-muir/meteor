import alias from '@rollup/plugin-alias';
import replace from '@rollup/plugin-replace';
import inject from '@rollup/plugin-inject';
import legacy from '@rollup/plugin-legacy';
import commonjs from '@rollup/plugin-commonjs';
import strip from '@rollup/plugin-strip';
import path from 'node:path';
import * as url from 'node:url';

const projectRootDir = url.fileURLToPath(new URL('.', import.meta.url));

const MeteorClientNamespace = [
  {
    path: 'packages/meteor/debug.js',
    names: [
      '_debug',
      '_suppress_log',
      '_suppressed_log_expected',
    ]
  },
  {
    path: 'packages/meteor/dynamics_browser.js',
    names: [
      'EnvironmentVariable',
      'bindEnvironment',
      '_nodeCodeMustBeInFiber',
    ]
  },
  {
    path: 'packages/meteor/helpers.js',
    names: [
      'release',
      '_get',
      '_ensure',
      '_delete',
      'promisify',
      'wrapAsync',
      'wrapFn',
      '_inherits',
      '_wrapAsync',
    ]
  },
  {
    path: 'packages/meteor/errors.js',
    names: [
      'Error',
      'makeErrorType',
    ]
  },
  {
    path: 'packages/meteor/timers.js',
    names: [
      'setTimeout',
      'setInterval',
      'clearInterval',
      'clearTimeout',
      'defer',
    ]
  },
  {
    path: 'packages/meteor/setimmediate.js',
    names: [
      '_setImmediate'
    ]
  },
  {
    package: 'meteor/localstorage',
    names: [
      '_localStorage'
    ]
  },
  {
    path: 'packages/meteor/startup_client.js',
    names: [
      'startup'
    ]
  },
  {
    path: 'packages/meteor/fiber_stubs_client.js',
    names: [
      '_noYieldsAllowed',
      '_SynchronousQueue',
    ]
  },
  {
    path: 'packages/meteor/url_common.js',
    names: [
      'absoluteUrl',
      '_relativeToSiteRootUrl',
    ]
  },
  {
    path: 'packages/meteor/string_utils.js',
    names: ['_escapeRegExp'],
  },
  {
    package: 'meteor/ddp-client',
    names: [
      'refresh',
      'connection',
      'subscribe',
      'methods',
      'call',
      'callAsync',
      'apply',
      'applyAsync',
      'status',
      'reconnect',
      'disconnect',
    ]
  },
  {
    package: 'meteor/accounts-base',
    names: [
      'users',
      'userId',
      'user',
      'userAsync',
      'loggingIn',
      'loggingOut',
      'logout',
      'logoutOtherClients',
      'loginWithToken',
    ]
  },
  {
    package: 'meteor/accounts-password',
    names: [
      'loginWithPassword',
      'loginWithPasswordAnd2faCode',
    ],
  },
  {
    path: 'packages/meteor/client_environment.js',
    names: [
      'isProduction',
      'isDevelopment',
      'isModern',
      'gitCommitHash',
      'settings',
    ]
  }
];

const MeteorServerNamespace = [
  {
    path: 'packages/meteor/debug.js',
    names: [
      '_debug',
      '_suppress_log',
      '_suppressed_log_expected',
    ]
  },
  {
    path: 'packages/meteor/dynamics_nodejs.js',
    names: [
      'EnvironmentVariable',
      'bindEnvironment',
      '_nodeCodeMustBeInFiber',
    ]
  },
  {
    path: 'packages/meteor/helpers.js',
    names: [
      'release',
      '_get',
      '_ensure',
      '_delete',
      'promisify',
      'wrapAsync',
      'wrapFn',
      '_inherits',
      '_wrapAsync',
    ]
  },
  {
    path: 'packages/meteor/errors.js',
    names: [
      'Error',
      'makeErrorType',
    ]
  },
  {
    path: 'packages/meteor/timers.js',
    names: [
      'setTimeout',
      'setInterval',
      'clearInterval',
      'clearTimeout',
      'defer',
    ]
  },
  {
    path: 'packages/meteor/setimmediate.js',
    names: [
      '_setImmediate'
    ]
  },
  {
    package: 'meteor/localstorage',
    names: [
      '_localStorage'
    ]
  },
  {
    path: 'packages/meteor/startup_server.js',
    names: [
      'startup'
    ]
  },
  {
    path: 'packages/meteor/fiber_helpers.js',
    names: [
      '_noYieldsAllowed',
      '_SynchronousQueue',
    ]
  },
  {
    package: 'denque',
    names: [
      '_DoubleEndedQueue',
    ]
  },
  {
    path: 'packages/meteor/url_server.js',
    names: [
      'absoluteUrl',
      '_relativeToSiteRootUrl',
    ]
  },
  {
    path: 'packages/meteor/string_utils.js',
    names: ['_escapeRegExp'],
  },
  {
    package: 'meteor/ddp-server',
    names: [
      'server',
      'refresh',
      'publish',
      'methods',
      'call',
      'callAsync',
      'apply',
      'applyAsync',
      'onConnection',
      'onMessage',
    ]
  },
  {
    package: 'meteor/accounts-base',
    names: [
      'users',
      'userId',
      'user',
      'userAsync',
    ]
  },
  {
    package: 'meteor/accounts-password',
    names: [
      'loginWithPassword',
      'loginWithPasswordAnd2faCode',
    ],
  },
  {
    path: 'packages/meteor/client_environment.js',
    names: [
      'isProduction',
      'isDevelopment',
      'isModern',
      'gitCommitHash',
      'settings',
    ]
  }
];


const packages = [
  {
    name: 'accounts-base',
    client: 'client_main.js',
    server: 'server_main.js',
    globals: ['Accounts'],
  },
  {
    name: 'accounts-github',
  },
  {
    name: 'accounts-oauth',
  },
  {
    name: 'accounts-password',
    client: 'password_client.js',
    server: 'password_server.js',
    globals: [],
  },
  {
    name: 'allow-deny',
    client: 'allow-deny.js',
    server: 'allow-deny.js',
    globals: ['AllowDeny'],
  },
  {
    name: 'base64',
    client: 'base64.js',
    server: 'base64.js',
    globals: ['Base64'],
  },
  {
    name: 'binary-heap',
  },
  {
    name: 'callback-hook',
    client: 'hook.js',
    server: 'hook.js',
    globals: ['Hook'],
  },

  {
    name: 'check',
    client: 'match.js',
    server: 'match.js',
    globals: ['check', 'Match'],
  },

  {
    name: 'ddp-client',
    client: 'client/client.js',
    server: 'server/server.js',
    globals: ['DDP'],
  },
  {
    virtual: true,
    name: 'ddp-client-environment',
    srcDir: 'packages/ddp-client',
    client: 'common/environment.js',
    server: 'common/environment.js',
  },
  {
    name: 'ddp-common',
    client: 'namespace.js',
    server: 'namespace.js',
    globals: ['DDPCommon'],
  },
  {
    name: 'ddp-rate-limiter',
    server: 'ddp-rate-limiter.js',
    globals: ['DDPRateLimiter'],
  },
  {
    virtual: true,
    name: 'ddp-common-utils',
    srcDir: 'packages/ddp-common',
    client: 'utils.js',
    server: 'utils.js',
  },
  {
    name: 'ddp-server',
    server: 'server_convenience.js',
    globals: ['DDPServer'],
  },
  {
    name: 'diff-sequence',
    client: 'diff.js',
    server: 'diff.js',
    globals: ['DiffSequence'],
  },
  {
    name: 'ejson',
    client: 'ejson.js',
    server: 'ejson.js',
    globals: ['EJSON'],
  },
  {
    name: 'email',
    server: 'email.js',
    globals: ['Email'],
  },
  {
    name: 'fetch',
    server: 'server.js',
    globals: [],
  },
  {
    name: 'geojson-utils',
    client: 'geojson-utils.js',
    server: 'geojson-utils.js',
    globals: ['GeoJSON'],
  },
  {
    name: 'google-oauth',
    client: 'google_client.js',
    server: 'google_server.js',
    globals: ['Google'],
  },
  {
    name: 'http',
    client: 'httpcall_client.js',
    server: 'httpcall_server.js',
    srcDir: 'packages/deprecated/http',
    globals: ['HTTP'],
  },
  {
    name: 'id-map',
    client: 'id-map.js',
    server: 'id-map.js',
    globals: ['IdMap'],
  },
  {
    name: 'localstorage',
    client: 'localstorage.js',
    // server: 'localstorage.js',
    globals: [],
  },
  {
    name: 'logging',
    client: 'logging.js',
    server: 'logging.js',
    globals: ['Log'],
  },
  {
    virtual: true,
    name: 'meteor',
    srcDir: '.',
    client: 'meteor-star-client.js',
    server: 'meteor-star-server.js',
  },
  {
    name: 'minimongo',
    client: 'minimongo_client.js',
    server: 'minimongo_server.js',
    globals: ['Minimongo', 'LocalCollection'],
  },
  {
    name: 'mongo',
    client: 'client_main.js',
    server: 'server_main.js',
    globals: ['Mongo'],
  },
  {
    name: 'mongo-id',
    client: 'id.js',
    server: 'id.js',
    globals: ['MongoID'],
  },
  {
    name: 'mongo-decimal',
    client: 'decimal.js',
    server: 'decimal.js',
    srcDir: 'packages/non-core/mongo-decimal',
    globals: ['Decimal'],
  },
  {
    name: 'npm-mongo',
    server: 'wrapper.js',
    globals: [
      "NpmModuleMongodb",
      "NpmModuleMongodbVersion",
    ]
  },
  {
    name: 'oauth',
    globals: ['OAuth'],
  },
  {
    name: 'oauth2',
  },
  {
    name: 'ordered-dict',
    client: 'ordered_dict.js',
    server: 'ordered_dict.js',
    globals: ['OrderedDict'],
  },
  {
    name: 'promise',
    client: 'client.js',
    server: 'server.js',
    globals: ['Promise'],
  },
  {
    name: 'random',
    client: 'main_client.js',
    server: 'main_server.js',
    globals: ['Random'],
  },
  {
    name: 'rate-limit',
    server: 'rate-limit.js',
    globals: ['RateLimiter'],
  },
  {
    name: 'reactive-dict',
    client: 'migration.js',
    // server: 'migration.js',
    globals: ['ReactiveDict'],
  },

  {
    name: 'reactive-var',
    client: 'reactive-var.js',
    // server: 'reactive-var.js',
    globals: ['ReactiveVar'],
  },
  {
    name: 'reload',
    client: 'reload.js',
    globals: ['Reload']
  },
  {
    name: 'retry',
    client: 'retry.js',
    server: 'retry.js',
    globals: ['Retry'],
  },
  {
    name: 'routepolicy',
    server: 'main.js',
    globals: ['RoutePolicy']
  },
  {
    name: 'service-configuration',
    client: 'service_configuration_common.js',
    server: 'service_configuration_server.js',
    globals: ['ServiceConfiguration'],
  },
  {
    name: 'session',
    client: 'session.js',
    server: null,
    globals: ['Session'],
  },
  {
    name: 'sha',
    client: 'sha256.js',
    server: 'sha256.js',
    globals: ['SHA256'],
  },
  {
    name: 'socket-stream-client',
    client: 'browser.js',
    server: 'node.js',
    globals: [],
  },
  {
    name: 'tracker',
    client: 'tracker.js',
    server: 'tracker.js',
    globals: ['Tracker'],
  },


  {
    name: 'url',
    client: 'modern.js',
    server: 'server.js',
    globals: [],
  },
  {
    name: 'webapp',
    client: 'webapp_client.js',
    server: 'webapp_server',
    globals: ['WebApp', 'WebAppInternals'],
  }
];

const getPath = p => p.srcDir ?? `packages/${p.name}`
const getImportPath = (p, isClient) =>
  path.resolve(projectRootDir,
  `${getPath(p)}/${isClient ? p.client : p.server}`)

const makeInjects = (isClient) =>
  packages.map(p => {
    const hasGlobals = Array.isArray(p.globals) && p.globals.length > 0;
    const add = hasGlobals && ((p.client && isClient) || (p.server && !isClient));
    if (add) {
      return inject({
        modules: Object.fromEntries(
          p.globals.map(name => [name, [`meteor/${p.name}`, name]])
        ),
        exclude: `${getPath(p)}/**`
      })
    }
    return null;
  }).filter(i => i != null)

const makeAliases = (isClient) => packages.flatMap(p => {
    const add = !p.virtual && ((p.client && isClient) || (p.server && !isClient));
    if (add) {
      return [
        {
          find: new RegExp(`^meteor/${p.name}$`),
          replacement: getImportPath(p, isClient)
        },
      ]
    }
    return [];
  }
)

function makeConfig(input, output, isClient) {
  return {
    input: Object.values(input),
    output: {
      format: 'es',
      // hoistTransitiveImports: false,
      ...output,
      preserveModules: true,
      preserveModulesRoot: 'packages',
    },
    treeshake: 'smallest',
    external: isClient ? [
      'decimal.js',
    ] : [
      '@vlasky/whomst',
      'assert', // node:assert
      'basic-auth-connnect',
      'bcrypt',
      'chalk',
      'cluster',
      'compression',
      'connect',
      'cookie-parser',
      'crypto', //node:crypto
      'decimal.js',
      'denque',
      'faye-websocket',
      'fibers',
      'fibers/future',
      'fs', // node:fs
      'http', // node:http
      'meteor-promise',
      'meteor/inter-process-messaging',
      'meteor/modern-browsers', // will be automatically stripped
      'meteor/reload', // elided
      'mongodb',
      'node-fetch',
      'nodemailer',
      'os', // node:os
      'parseurl',
      'path', // node:path
      'permessage-deflate',
      'qs',
      'send',
      'sockjs',
      'underscore',
      'url', // node:url
      'useragent',
    ],
    plugins: [
      replace({
        preventAssignment: false,
        delimiters: ['', ''],
        values: {
          'DDP._CurrentMethodInvocation': 'DDP$_CurrentMethodInvocation',
          'DDP._CurrentPublicationInvocation': 'DDP$_CurrentPublicationInvocation',
          'DDP._CurrentInvocation': 'DDP$_CurrentMethodInvocation',
          // 'Package[\'oauth-encryption\']': 'Package.oauthEncryption',
          'Package[\'service-configuration\']': 'Package.serviceConfiguration',
          'Package[\'mongo-decimal\']': 'Package.mongoDecimal',
          'Package[\'ddp-rate-limiter\']': 'Package.ddpRateLimiter',
          'Package[\'audit-argument-checks\']': 'true',
          'Package[\'disable-oplog\']': 'true',
        }
      }),
      replace({
        preventAssignment: true,
        values: isClient ? {
          'Meteor.isServer': 'false',
          'Meteor.isClient': 'true',
          'Meteor.isCordova': 'false',
          'Package.autopublish': 'undefined',
          'Package.autoupdate': 'undefined',
          'Package.blaze': 'undefined',
          'Package.insecure': 'undefined',

        } : {
          'Meteor.isServer': 'true',
          'Meteor.isClient': 'false',
          'Meteor.isCordova': 'false',
          'Npm.require': 'require',
          'Meteor._printReceivedDDP': 'false',
          'Meteor._printSentDDP': 'false',
          'Package.autopublish': 'undefined',
          'Package.autoupdate': 'undefined',
          'Package.blaze': 'undefined',
          'Package.insecure': 'undefined',
        }
      }),
      strip({
        functions: ['setMinimumBrowserVersions'],
      }),
      inject({
        _: 'underscore',
        'DDP$_CurrentMethodInvocation': [
          path.resolve(projectRootDir, 'packages/ddp-client/common/environment.js'), 'CurrentMethodInvocation'
        ],
        'DDP$_CurrentPublicationInvocation': [
          path.resolve(projectRootDir, 'packages/ddp-client/common/environment.js'), 'CurrentPublicationInvocation'
        ],
        'Package.tracker': ['meteor/tracker', '*'],
        'Package.reload': ['meteor/reload', '*'],
        'Package.mongo': ['meteor/mongo', '*'],
        'Package.ddp': ['meteor/ddp', '*'],
        'Package.serviceConfiguration': ['meteor/service-configuration', '*'],
        'Package.mongoDecimal': ['meteor/mongo-decimal', '*'],
        'Package.ddpRateLimiter': ['meteor/ddp-rate-limiter', '*'],
        Package: [path.resolve(projectRootDir, 'package-map.js'), '*'],
        ...Object.fromEntries((isClient ? MeteorClientNamespace : MeteorServerNamespace).flatMap(nsp => {
          const importFrom = nsp.package ?? path.resolve(projectRootDir, nsp.path);
          return nsp.names.map(name => [`Meteor.${name}`, [importFrom, `Meteor$${name}`]])
        })),
        Meteor: [path.resolve(projectRootDir,
          isClient ? 'meteor-map-client.js' : 'meteor-map-server.js'), '*'],
      }),
      ...makeInjects(isClient),
      alias({
        entries: [
          {
            find: 'meteor/underscore',
            replacement: 'underscore'
          },
          {
            find: /^meteor\/meteor$/,
            replacement: path.resolve(projectRootDir, isClient ? 'meteor-star-client.js' :
              'meteor-star-server.js')
          },
          {
            find: 'meteor/ddp-common/utils.js',
            replacement: path.resolve(projectRootDir, 'packages/ddp-common/utils.js'),
          },
          ...makeAliases(isClient),
        ]
      }),
      commonjs({
        transformMixedEsModules: true,
      })
    ]
  };
}

const clientInputFiles = Object.fromEntries(
  packages.filter(p => p.client != null).map(p => {
    const d = p.srcDir ?? `packages/${p.name}`
    return [p.name, `${d}/${p.client}`]
  })
);

const serverInputFiles = Object.fromEntries(
  packages.filter(p => p.server != null).map(p => {
    const d = p.srcDir ?? `packages/${p.name}`
    return [p.name, `${d}/${p.server}`]
  })
);

export default [
  makeConfig(
    clientInputFiles,
    {
      dir: 'modules-client',
    },
    true,
  ),
  makeConfig(
    serverInputFiles,
    {
      dir: 'modules-server',
    },
    false,
  )
];
