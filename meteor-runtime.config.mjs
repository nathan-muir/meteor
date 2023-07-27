import alias from '@rollup/plugin-alias';
import replace from '@rollup/plugin-replace';
import inject from '@rollup/plugin-inject';
import path from 'node:path';

export default [
  {
    input: './meteor-runtime-browser.mjs',
    output: {
      file: './meteor-runtime-browser.esm.js',
      format: 'es',
    },
    external: ['decimal.js', 'sockjs-client'],
  },
  {
    input: './meteor-runtime-server.mjs',
    output: {
      file: './meteor-runtime-server.esm.js',
      format: 'es',
    },
    external: [
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
  },
];
