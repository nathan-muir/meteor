import { createServer } from 'http';
import { parse as parseUrl } from 'url';
import { connect } from './connect.js';
import compress from 'compression';
import cookieParser from 'cookie-parser';
import qs from 'qs';
import parseRequest from 'parseurl';

var SHORT_SOCKET_TIMEOUT = 5 * 1000;
var LONG_SOCKET_TIMEOUT = 120 * 1000;

// Serve app HTML for this URL?
var appUrl = function (url) {
  if (url === '/favicon.ico' || url === '/robots.txt') return false;

  // NOTE: app.manifest is not a web standard like favicon.ico and
  // robots.txt. It is a file name we have chosen to use for HTML5
  // appcache URLs. It is included here to prevent using an appcache
  // then removing it from poisoning an app permanently. Eventually,
  // once we have server side routing, this won't be needed as
  // unknown URLs with return a 404 automatically.
  if (url === '/app.manifest') return false;

  // Avoid serving app HTML for declared routes such as /sockjs/.
  if (RoutePolicy.classify(url)) return false;

  // we currently return app HTML on all URLs by default
  return true;
};


// When we have a request pending, we want the socket timeout to be long, to
// give ourselves a while to serve it, and to allow sockjs long polls to
// complete.  On the other hand, we want to close idle sockets relatively
// quickly, so that we can shut down relatively promptly but cleanly, without
// cutting off anyone's response.
export function _timeoutAdjustmentRequestCallback(req, res) {
  // this is really just req.socket.setTimeout(LONG_SOCKET_TIMEOUT);
  req.setTimeout(LONG_SOCKET_TIMEOUT);
  // Insert our new finish listener to run BEFORE the existing one which removes
  // the response from the socket.
  var finishListeners = res.listeners('finish');
  // XXX Apparently in Node 0.12 this event was called 'prefinish'.
  // https://github.com/joyent/node/commit/7c9b6070
  // But it has switched back to 'finish' in Node v4:
  // https://github.com/nodejs/node/pull/1411
  res.removeAllListeners('finish');
  res.on('finish', function () {
    res.setTimeout(SHORT_SOCKET_TIMEOUT);
  });
  _.each(finishListeners, function (l) {
    res.on('finish', l);
  });
}
let boilerplateHtml = '<html><body>Not setup</body></html>';
export function setBoilerplateHtml(newHtml) {
  boilerplateHtml = newHtml;
}

const meteorRuntimeConfig = JSON.stringify({
  ACCOUNTS_CONNECTION_URL: __meteor_runtime_config__.ACCOUNTS_CONNECTION_URL,
  DDP_DEFAULT_CONNECTION_URL: __meteor_runtime_config__.DDP_DEFAULT_CONNECTION_URL,
  DISABLE_SOCKJS: __meteor_runtime_config__.DISABLE_SOCKJS,
  PUBLIC_SETTINGS: __meteor_runtime_config__.PUBLIC_SETTINGS,
  ROOT_URL: __meteor_runtime_config__.ROOT_URL,
  ROOT_URL_PATH_PREFIX: __meteor_runtime_config__.ROOT_URL_PATH_PREFIX,
  gitCommitHash: __meteor_runtime_config__.gitCommitHash,
  meteorEnv: __meteor_runtime_config__.meteorEnv,
  meteorRelease: __meteor_runtime_config__.meteorRelease,
  isModern: true,
});
// A mapping from url path to architecture (e.g. "web.browser") to static
// file information with the following fields:
// - type: the type of file to be served
// - cacheable: optionally, whether the file should be cached or not
// - sourceMapUrl: optionally, the url of the source map
//
// Info also contains one of the following:
// - content: the stringified content that should be served at this path
// - absolutePath: the absolute path on disk to the file

// Serve static files from the manifest or added with
// `addStaticJs`. Exported for tests.
export async function staticFilesMiddleware(
  req,
  res,
  next
) {
  var pathname = parseRequest(req).pathname;
  try {
    pathname = decodeURIComponent(pathname);
  } catch (e) {
    next();
    return;
  }

  var serveStaticJs = function (s) {
    if (
      req.method === 'GET' ||
      req.method === 'HEAD'
    ) {
      res.writeHead(200, {
        'Content-type': 'application/javascript; charset=UTF-8',
        'Content-Length': Buffer.byteLength(s),
      });
      res.write(s);
      res.end();
    } else {
      const status = req.method === 'OPTIONS' ? 200 : 405;
      res.writeHead(status, {
        Allow: 'OPTIONS, GET, HEAD',
        'Content-Length': '0',
      });
      res.end();
    }
  };

  if (
    pathname === '/meteor_runtime_config.js'
  ) {

    serveStaticJs(
      `globalThis.__meteor_runtime_config__ = ${meteorRuntimeConfig};`
    );
    return;
  }

  if (
    req.method === 'GET' ||
    req.method === 'HEAD'
  ) {
    res.writeHead(200, {
      'Content-type': 'text/html; charset=UTF-8',
      'Content-Length': Buffer.byteLength(boilerplateHtml),
    });
    res.write(boilerplateHtml);
    res.end();
  } else {
    const status = req.method === 'OPTIONS' ? 200 : 405;
    res.writeHead(status, {
      Allow: 'OPTIONS, GET, HEAD',
      'Content-Length': '0',
    });
    res.end();
  }
}

// Parse the passed in port value. Return the port as-is if it's a String
// (e.g. a Windows Server style named pipe), otherwise return the port as an
// integer.
//
// DEPRECATED: Direct use of this function is not recommended; it is no
// longer used internally, and will be removed in a future release.
export function parsePort(port) {
  let parsedPort = parseInt(port);
  if (Number.isNaN(parsedPort)) {
    parsedPort = port;
  }
  return parsedPort;
}


var shuttingDown = false;
// webserver
var app = connect();
export { app as connectApp };

// Packages and apps can add handlers that run before any other Meteor
// handlers via WebApp.rawConnectHandlers.
export const rawConnectHandlers = connect();
app.use(rawConnectHandlers);

// Auto-compress any json, javascript, or text.
app.use(compress());

// parse cookies into an object
app.use(cookieParser());

// We're not a proxy; reject (without crashing) attempts to treat us like
// one. (See #1212.)
app.use(function (req, res, next) {
  if (RoutePolicy.isValidUrl(req.url)) {
    next();
    return;
  }
  res.writeHead(400);
  res.write('Not a proxy');
  res.end();
});

// Parse the query string into res.query. Used by oauth_server, but it's
// generally pretty handy..
//
// Do this before the next middleware destroys req.url if a path prefix
// is set to close #10111.
app.use(function (request, response, next) {
  request.query = qs.parse(parseUrl(request.url).query);
  next();
});

function getPathParts(path) {
  const parts = path.split('/');
  while (parts[0] === '') parts.shift();
  return parts;
}

function isPrefixOf(prefix, array) {
  return (
    prefix.length <= array.length &&
    prefix.every((part, i) => part === array[i])
  );
}

// Strip off the path prefix, if it exists.
app.use(function (request, response, next) {
  const pathPrefix = __meteor_runtime_config__.ROOT_URL_PATH_PREFIX;
  const { pathname, search } = parseUrl(request.url);

  // check if the path in the url starts with the path prefix
  if (pathPrefix) {
    const prefixParts = getPathParts(pathPrefix);
    const pathParts = getPathParts(pathname);
    if (isPrefixOf(prefixParts, pathParts)) {
      request.url = '/' + pathParts.slice(prefixParts.length).join('/');
      if (search) {
        request.url += search;
      }
      return next();
    }
  }

  if (pathname === '/favicon.ico' || pathname === '/robots.txt') {
    return next();
  }

  if (pathPrefix) {
    response.writeHead(404);
    response.write('Unknown path');
    response.end();
    return;
  }

  next();
});

// Serve static files from the manifest.
// This is inspired by the 'static' middleware.
app.use(function (req, res, next) {
  staticFilesMiddleware(
    req,
    res,
    next
  );
});

export const meteorInternalHandlers = connect()
// Core Meteor packages like dynamic-import can add handlers before
// other handlers added by package and application code.
app.use(meteorInternalHandlers);

/**
 * @name connectHandlersCallback(req, res, next)
 * @locus Server
 * @isprototype true
 * @summary callback handler for `WebApp.connectHandlers`
 * @param {Object} req
 * a Node.js
 * [IncomingMessage](https://nodejs.org/api/http.html#http_class_http_incomingmessage)
 * object with some extra properties. This argument can be used
 *  to get information about the incoming request.
 * @param {Object} res
 * a Node.js
 * [ServerResponse](http://nodejs.org/api/http.html#http_class_http_serverresponse)
 * object. Use this to write data that should be sent in response to the
 * request, and call `res.end()` when you are done.
 * @param {Function} next
 * Calling this function will pass on the handling of
 * this request to the next relevant handler.
 *
 */

/**
 * @method connectHandlers
 * @memberof WebApp
 * @locus Server
 * @summary Register a handler for all HTTP requests.
 * @param {String} [path]
 * This handler will only be called on paths that match
 * this string. The match has to border on a `/` or a `.`.
 *
 * For example, `/hello` will match `/hello/world` and
 * `/hello.world`, but not `/hello_world`.
 * @param {connectHandlersCallback} handler
 * A handler function that will be called on HTTP requests.
 * See `connectHandlersCallback`
 *
 */
  // Packages and apps can add handlers to this via WebApp.connectHandlers.
  // They are inserted before our default handler.

const packageAndAppHandlers = connect();
export { packageAndAppHandlers as connectHandlers };
app.use(packageAndAppHandlers);

var suppressConnectErrors = false;
// connect knows it is an error handler because it has 4 arguments instead of
// 3. go figure.  (It is not smart enough to find such a thing if it's hidden
// inside packageAndAppHandlers.)
app.use(function (err, req, res, next) {
  if (!err || !suppressConnectErrors || !req.headers['x-suppress-error']) {
    next(err);
    return;
  }
  res.writeHead(err.status, { 'Content-Type': 'text/plain' });
  res.end('An error message');
});

app.use(async function (req, res, next) {
  if (!appUrl(req.url)) {
    return next();
  } else if (
    req.method !== 'HEAD' &&
    req.method !== 'GET'
  ) {
    const status = req.method === 'OPTIONS' ? 200 : 405;
    res.writeHead(status, {
      Allow: 'OPTIONS, GET, HEAD',
      'Content-Length': '0',
    });
    res.end();
  } else {
    var headers = {
      'Content-Type': 'text/html; charset=utf-8',
    };

    if (shuttingDown) {
      headers['Connection'] = 'Close';
    }

    if (req.url.query && req.url.query['meteor_css_resource']) {
      // In this case, we're requesting a CSS resource in the meteor-specific
      // way, but we don't have it.  Serve a static css file that indicates that
      // we didn't have it, so we can detect that and refresh.  Make sure
      // that any proxies or CDNs don't cache this error!  (Normally proxies
      // or CDNs are smart enough not to cache error pages, but in order to
      // make this hack work, we need to return the CSS file as a 200, which
      // would otherwise be cached.)
      headers['Content-Type'] = 'text/css; charset=utf-8';
      headers['Cache-Control'] = 'no-cache';
      res.writeHead(200, headers);
      res.write('.meteor-css-not-found-error { width: 0px;}');
      res.end();
      return;
    }

    if (req.url.query && req.url.query['meteor_js_resource']) {
      // Similarly, we're requesting a JS resource that we don't have.
      // Serve an uncached 404. (We can't use the same hack we use for CSS,
      // because actually acting on that hack requires us to have the JS
      // already!)
      headers['Cache-Control'] = 'no-cache';
      res.writeHead(404, headers);
      res.end('404 Not Found');
      return;
    }

    if (req.url.query && req.url.query['meteor_dont_serve_index']) {
      // When downloading files during a Cordova hot code push, we need
      // to detect if a file is not available instead of inadvertently
      // downloading the default index page.
      // So similar to the situation above, we serve an uncached 404.
      headers['Cache-Control'] = 'no-cache';
      res.writeHead(404, headers);
      res.end('404 Not Found');
      return;
    }

    // TODO!!

    next();
  }
});

// Return 404 by default, if no other handlers serve this URL.
app.use(function (req, res) {
  res.writeHead(404);
  res.end();
});

export const httpServer = createServer(app);
var onListeningCallbacks = [];

// After 5 seconds w/o data on a socket, kill it.  On the other hand, if
// there's an outstanding request, give it a higher timeout instead (to avoid
// killing long-polling requests)
httpServer.setTimeout(SHORT_SOCKET_TIMEOUT);

// Do this here, and then also in livedata/stream_server.js, because
// stream_server.js kills all the current request handlers when installing its
// own.
httpServer.on('request', WebApp._timeoutAdjustmentRequestCallback);

// If the client gave us a bad request, tell it instead of just closing the
// socket. This lets load balancers in front of us differentiate between "a
// server is randomly closing sockets for no reason" and "client sent a bad
// request".
//
// This will only work on Node 6; Node 4 destroys the socket before calling
// this event. See https://github.com/nodejs/node/pull/4557/ for details.
httpServer.on('clientError', (err, socket) => {
  // Pre-Node-6, do nothing.
  if (socket.destroyed) {
    return;
  }

  if (err.message === 'Parse Error') {
    socket.end('HTTP/1.1 400 Bad Request\r\n\r\n');
  } else {
    // For other errors, use the default behavior as if we had no clientError
    // handler.
    socket.destroy(err);
  }
});

export function onListening(f) {
  if (onListeningCallbacks) onListeningCallbacks.push(f);
  else f();
}


const startHttpServer = listenOptions =>
  httpServer.listen(
    listenOptions,
    Meteor.bindEnvironment(
      () => {
        if (process.env.METEOR_PRINT_ON_LISTEN) {
          console.log('LISTENING');
        }
        const callbacks = onListeningCallbacks;
        onListeningCallbacks = null;
        callbacks.forEach(callback => {
          callback();
        });
      },
      e => {
        console.error('Error listening:', e);
        console.error(e && e.stack);
      }
    )
  )

export function listen() {
  const localPort = Number(process.env.PORT) || 0;
  if (Number.isSafeInteger(localPort)) {
    // Start the HTTP server using TCP.
    return startHttpServer({
      port: localPort,
      host: process.env.BIND_IP || '0.0.0.0',
    });
  } else {
    throw new Error('Invalid PORT specified');
  }
}
