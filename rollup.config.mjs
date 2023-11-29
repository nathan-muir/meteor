import alias from '@rollup/plugin-alias';
import replace from '@rollup/plugin-replace';
import inject from '@rollup/plugin-inject';
import commonjs from '@rollup/plugin-commonjs';
import strip from '@rollup/plugin-strip';
import path from 'node:path';
import * as url from 'node:url';
import prettier from 'rollup-plugin-prettier';

const projectRootDir = url.fileURLToPath(new URL('.', import.meta.url));

const MeteorClientNamespace = [
  {
    path: 'packages/meteor/debug.js',
    names: ['_debug', '_suppress_log', '_suppressed_log_expected'],
  },
  {
    path: 'packages/meteor/dynamics_browser.js',
    names: ['EnvironmentVariable', 'bindEnvironment', '_nodeCodeMustBeInFiber'],
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
      '_inherits',
      '_wrapAsync',
    ],
  },
  {
    path: 'packages/meteor/helpers_wrapfn.js',
    names: ['wrapFn'],
  },
  {
    path: 'packages/meteor/errors.js',
    names: ['Error', 'makeErrorType'],
  },
  {
    path: 'packages/meteor/timers.js',
    names: [
      'setTimeout',
      'setInterval',
      'clearInterval',
      'clearTimeout',
      'defer',
    ],
  },
  {
    path: 'packages/meteor/setimmediate.js',
    names: ['_setImmediate'],
  },
  {
    package: 'meteor/localstorage',
    names: ['_localStorage'],
  },
  {
    path: 'packages/meteor/startup_client.js',
    names: ['startup'],
  },
  {
    path: 'packages/meteor/fiber_stubs_client.js',
    names: ['_noYieldsAllowed', '_SynchronousQueue'],
  },
  {
    path: 'packages/meteor/url_common.js',
    names: ['absoluteUrl', '_relativeToSiteRootUrl'],
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
    ],
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
    ],
  },
  {
    package: 'meteor/accounts-password',
    names: ['loginWithPassword', 'loginWithPasswordAnd2faCode'],
  },
  {
    path: 'packages/meteor/client_environment.js',
    names: [
      'isProduction',
      'isDevelopment',
      'isModern',
      'gitCommitHash',
      'settings',
    ],
  },
];

const MeteorServerNamespace = [
  {
    path: 'packages/meteor/debug.js',
    names: ['_debug', '_suppress_log', '_suppressed_log_expected'],
  },
  {
    path: 'packages/meteor/dynamics_nodejs.js',
    names: ['EnvironmentVariable', 'bindEnvironment', '_nodeCodeMustBeInFiber'],
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
      '_inherits',
      '_wrapAsync',
    ],
  },
  {
    path: 'packages/meteor/helpers_wrapfn.js',
    names: ['wrapFn'],
  },
  {
    path: 'packages/meteor/errors.js',
    names: ['Error', 'makeErrorType'],
  },
  {
    path: 'packages/meteor/timers.js',
    names: [
      'setTimeout',
      'setInterval',
      'clearInterval',
      'clearTimeout',
      'defer',
    ],
  },
  {
    path: 'packages/meteor/setimmediate.js',
    names: ['_setImmediate'],
  },
  {
    package: 'meteor/localstorage',
    names: ['_localStorage'],
  },
  {
    path: 'packages/meteor/startup_server.js',
    names: ['startup'],
  },
  {
    path: 'packages/meteor/fiber_helpers.js',
    names: ['_noYieldsAllowed', '_SynchronousQueue'],
  },
  {
    path: 'packages/meteor/denque.js',
    names: ['_DoubleEndedQueue'],
  },
  {
    path: 'packages/meteor/url_common.js',
    names: ['absoluteUrl', '_relativeToSiteRootUrl'],
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
    ],
  },
  {
    package: 'meteor/accounts-base',
    names: ['users', 'userId', 'user', 'userAsync'],
  },
  {
    package: 'meteor/accounts-password',
    names: ['loginWithPassword', 'loginWithPasswordAnd2faCode'],
  },
  {
    path: 'packages/meteor/server_environment.js',
    names: [
      'isProduction',
      'isDevelopment',
      'isModern',
      'gitCommitHash',
      'settings',
    ],
  },
];

const StaticsShared = [
  {
    global: 'DDP',
    members: {
      _CurrentMethodInvocation: 'CurrentMethodInvocation',
      _CurrentPublicationInvocation: 'CurrentPublicationInvocation',
      _CurrentInvocation: 'CurrentMethodInvocation',
    },

    path: 'packages/ddp-client/common/environment.js',
  },
  {
    global: 'DDP',
    members: ['ConnectionError', 'ForcedReconnectError'],
    path: 'packages/ddp-client/common/errors.js',
  },
  {
    global: 'DDP',
    members: ['randomStream'],
    path: 'packages/ddp-client/common/random_stream.js',
  },
  {
    global: 'DDPServer',
    members: {
      _WriteFence: 'WriteFence',
      _CurrentWriteFence: 'CurrentWriteFence',
    },
    path: 'packages/ddp-server/writefence.js',
  },
  {
    global: 'DDPServer',
    members: {
      _Crossbar: 'Crossbar',
      _InvalidationCrossbar: 'InvalidationCrossbar',
    },
    path: 'packages/ddp-server/crossbar.js',
  },
  {
    global: 'LocalCollection',
    members: [
      '_IdMap',
      '_modify',
      '_observeFromObserveChanges',
      '_observeChangesCallbacksAreOrdered',
      '_isPlainObject',
      '_selectorIsId',
      '_selectorIsIdPerhapsAsObject',
    ],
    path: 'packages/minimongo/local_collection_static.js',
  },
];

function* statics(isClient) {
  for (const entry of StaticsShared) {
    if (Array.isArray(entry.members)) {
      for (const member of entry.members) {
        yield [entry.global, member, member, entry.path];
      }
    } else {
      for (const [member, importName] of Object.entries(entry.members)) {
        yield [entry.global, member, importName, entry.path];
      }
    }
  }
}

const packages = [
  {
    name: 'accounts-base',
    client: 'client_main.js',
    server: 'server_main.js',
    globals: ['Accounts'],
  },
  {
    name: 'accounts-google',
    client: 'google.js',
    server: 'google_server.js',
  },
  {
    name: 'accounts-oauth',
    client: 'oauth_client.js',
    server: 'oauth_server.js',
    globals: [{ name: 'AccountsOAuth', namespace: true }],
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
    client: 'binary-heap.js',
    server: 'binary-heap.js',
    globals: ['MaxHeap', 'MinHeap', 'MinMaxHeap'],
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
    name: 'modern-browsers',
    server: 'modern_fake.js',
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
    // server: 'wrapper.js',
    globals: ['NpmModuleMongodb', 'NpmModuleMongodbVersion'],
  },
  {
    name: 'oauth',
    client: 'oauth_client.js',
    server: 'oauth_server.js',
    globals: [{ name: 'OAuth', namespace: true }],
  },
  {
    name: 'oauth2',
    server: 'oauth2_server.js',
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
    client: 'migration-client.js',
    // server: 'migration.js',
    globals: ['ReactiveDict'],
  },

  {
    name: 'reactive-var',
    client: 'reactive-var-client.js',
    // server: 'reactive-var.js',
    globals: ['ReactiveVar'],
  },
  {
    name: 'reload',
    client: 'reload.js',
    globals: ['Reload'],
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
    globals: ['RoutePolicy'],
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
    client: 'tracker_client.js',
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
    server: 'webapp_server.js',
    // globals: ['WebApp', 'WebAppInternals'],
  },
];

const getPath = p => p.srcDir ?? `packages/${p.name}`;
const getImportPath = (p, isClient) =>
  path.resolve(
    projectRootDir,
    `${getPath(p)}/${isClient ? p.client : p.server}`,
  );

const makeInjects = isClient =>
  packages
    .map(p => {
      const hasGlobals = Array.isArray(p.globals) && p.globals.length > 0;
      const add =
        hasGlobals && ((p.client && isClient) || (p.server && !isClient));
      if (add) {
        return inject({
          modules: Object.fromEntries(
            p.globals.map(opt =>
              typeof opt === 'string'
                ? [opt, [`meteor/${p.name}`, opt]]
                : [
                    opt.name,
                    [`meteor/${p.name}`, opt.namespace ? '*' : opt.name],
                  ],
            ),
          ),
          exclude: `${getPath(p)}/**`,
        });
      }
      return null;
    })
    .filter(i => i != null);

const makeAliases = isClient =>
  packages.flatMap(p => {
    const add =
      !p.virtual && ((p.client && isClient) || (p.server && !isClient));
    if (add) {
      return [
        {
          find: new RegExp(`^meteor/${p.name}$`),
          replacement: getImportPath(p, isClient),
        },
      ];
    }
    return [];
  });

function makeConfig(input, output, isClient) {
  return {
    input: Object.values(input),
    output: {
      format: 'es',
      hoistTransitiveImports: false,
      ...output,
      preserveModules: true,
      preserveModulesRoot: 'packages',
    },
    treeshake: {
      preset: 'smallest',
      moduleSideEffects: [
        path.resolve(
          projectRootDir,
          'packages/non-core/mongo-decimal/decimal.js',
        ),
        path.resolve(projectRootDir, 'packages/promise/extensions.js'),
        path.resolve(
          projectRootDir,
          'packages/accounts-password/email_templates.js',
        ),
        path.resolve(projectRootDir, 'packages/accounts-oauth/oauth_common.js'),
        path.resolve(projectRootDir, 'packages/accounts-oauth/oauth_server.js'),
      ],
    },
    external: isClient
      ? ['decimal.js', 'sockjs-client']
      : [
          '@vlasky/whomst',
          'assert', // node:assert
          'basic-auth-connect',
          'bcrypt',
          'body-parser',
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
          'meteor/reload', // elided
          'mongodb',
          'mongodb-uri',
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
          // 'Package[\'oauth-encryption\']': 'Package.oauthEncryption',
          "Package['service-configuration']": 'Package.serviceConfiguration',
          "Package['mongo-decimal']": 'Package.mongoDecimal',
          "Package['ddp-rate-limiter']": 'Package.ddpRateLimiter',
          "Package['facts-base']": 'Package.factsBase',
          "Package['audit-argument-checks']": 'undefined',
          "Package['disable-oplog']": 'true',
          "Package['oauth-encryption']": 'Package.oauthEncryption',
          'Package["oauth-encryption"]': 'Package.oauthEncryption',
          'Accounts.oauth': 'AccountsOAuth',
          ...Object.fromEntries(
            Array.from(statics(isClient)).map(([global, member]) => [
              `${global}.${member}`,
              `${global}$${member}`,
            ]),
          ),
        },
      }),
      replace({
        preventAssignment: true,
        values: isClient
          ? {
              'Meteor.isServer': 'false',
              'Meteor.isClient': 'true',
              'Meteor.isCordova': 'false',
              'Package.autopublish': 'undefined',
              'Package.autoupdate': 'undefined',
              'Package.blaze': 'undefined',
              'Package.insecure': 'undefined',
            }
          : {
              'Meteor.isServer': 'true',
              'Meteor.isClient': 'false',
              'Meteor.isCordova': 'false',
              'Npm.require': 'require',
              'Meteor._printReceivedDDP': 'false',
              'Meteor._printSentDDP': 'false',
              'Package.autopublish': 'undefined',
              'Package.autoupdate': 'undefined',
              'Package.factsBase': 'undefined',
              'Package.blaze': 'undefined',
              'Package.insecure': 'undefined',
              'Package.tracker': 'undefined',
            },
      }),
      strip({
        functions: ['setMinimumBrowserVersions'],
      }),
      inject({
        ...(isClient
          ? {
              __meteor_runtime_config__: path.resolve(
                projectRootDir,
                'packages/meteor/meteor_runtime_config_client.js',
              ),
            }
          : {
              __meteor_runtime_config__: [
                path.resolve(
                  projectRootDir,
                  'packages/meteor/meteor_runtime_config_server.js',
                ),
                '*',
              ],
            }),
        _: ['underscore', '*'],
        'Package.tracker': ['meteor/tracker', '*'],
        'Package.reload': ['meteor/reload', '*'],
        'Package.mongo': ['meteor/mongo', '*'],
        'Package.ddp': ['meteor/ddp', '*'],
        'Package.serviceConfiguration': ['meteor/service-configuration', '*'],
        'Package.mongoDecimal': ['meteor/mongo-decimal', '*'],
        'Package.ddpRateLimiter': ['meteor/ddp-rate-limiter', '*'],
        WebApp: ['meteor/webapp', '*'],
        WebAppInternals: ['meteor/webapp', '*'],
        Package: [path.resolve(projectRootDir, 'package-map.js'), '*'],
        ...Object.fromEntries(
          Array.from(statics(isClient)).map(
            ([global, member, importName, _path]) => [
              `${global}$${member}`,
              [path.resolve(projectRootDir, _path), importName],
            ],
          ),
        ),
        ...Object.fromEntries(
          (isClient ? MeteorClientNamespace : MeteorServerNamespace).flatMap(
            nsp => {
              const importFrom =
                nsp.package ?? path.resolve(projectRootDir, nsp.path);
              return nsp.names.map(name => [
                `Meteor.${name}`,
                [importFrom, `Meteor$${name}`],
              ]);
            },
          ),
        ),
        Meteor: [
          path.resolve(
            projectRootDir,
            isClient ? 'meteor-map-client.js' : 'meteor-map-server.js',
          ),
          '*',
        ],
      }),
      ...makeInjects(isClient),
      alias({
        entries: [
          {
            find: 'meteor/underscore',
            replacement: 'underscore',
          },
          {
            find: /^meteor\/meteor$/,
            replacement: path.resolve(
              projectRootDir,
              isClient ? 'meteor-star-client.js' : 'meteor-star-server.js',
            ),
          },
          // TODO make replacement rule
          {
            find: 'meteor/ddp-common/utils.js',
            replacement: path.resolve(
              projectRootDir,
              'packages/ddp-common/utils.js',
            ),
          },
          {
            find: 'meteor/ddp-common/random_stream.js',
            replacement: path.resolve(
              projectRootDir,
              'packages/ddp-common/random_stream.js',
            ),
          },
          {
            find: 'meteor/minimongo/constants',
            replacement: path.resolve(
              projectRootDir,
              'packages/minimongo/constants.js',
            ),
          },
          {
            find: 'meteor/mongo/collection_util.js',
            replacement: path.resolve(
              projectRootDir,
              'packages/mongo/collection_util.js',
            ),
          },
          ...makeAliases(isClient),
        ],
      }),
      commonjs({
        strictRequires: false,
        transformMixedEsModules: true,
      }),

      prettier({
        singleQuote: true,
        trailingComma: 'all',
        arrowParens: 'avoid',
        htmlWhitespaceSensitivity: 'ignore',
        parser: 'espree',
      }),
    ],
  };
}

const clientInputFiles = Object.fromEntries(
  packages
    .filter(p => p.client != null)
    .map(p => {
      const d = p.srcDir ?? `packages/${p.name}`;
      return [p.name, `${d}/${p.client}`];
    }),
);

const serverInputFiles = Object.fromEntries(
  packages
    .filter(p => p.server != null)
    .map(p => {
      const d = p.srcDir ?? `packages/${p.name}`;
      return [p.name, `${d}/${p.server}`];
    }),
);

export default [
  makeConfig(
    clientInputFiles,
    {
      dir: '_prototype/meteor-runtime/src/browser',
    },
    true,
  ),
  makeConfig(
    serverInputFiles,
    {
      dir: '_prototype/meteor-runtime/src/node',
    },
    false,
  ),
];
