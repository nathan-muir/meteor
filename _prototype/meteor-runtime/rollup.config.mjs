import prettier from 'rollup-plugin-prettier';

export default [
  {
    input: './meteor-runtime-browser.js',
    output: {
      file: './dist/meteor-runtime-browser.js',
      format: 'es',
    },
    external: ['decimal.js', 'sockjs-client'],
    plugins: [
      prettier({
        singleQuote: true,
        trailingComma: 'all',
        arrowParens: 'avoid',
        htmlWhitespaceSensitivity: 'ignore',
        parser: 'espree',
      })
    ]
  }, {
    input: './meteor-runtime-node.js',
    output: {
      file: './dist/meteor-runtime-node.js',
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
      prettier({
        singleQuote: true,
        trailingComma: 'all',
        arrowParens: 'avoid',
        htmlWhitespaceSensitivity: 'ignore',
        parser: 'espree',
      })
    ]
  }
]


