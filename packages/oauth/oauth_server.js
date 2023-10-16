import crypto from 'crypto';
import bodyParser from 'body-parser';
import { OAuth$_storageTokenPrefix, OAuth$_redirectUri } from "./oauth_common.js";
import { _storePendingCredential, _retrievePendingCredential } from './pending_credentials.js';

export * from './pending_credentials.js';

RoutePolicy.declare('/_oauth/', 'network');

const registeredServices = {};

// Internal: Maps from service version to handler function. The
// 'oauth1' and 'oauth2' packages manipulate this directly to register
// for callbacks.
const OAuth$_requestHandlers = {};


/**
/* Register a handler for an OAuth service. The handler will be called
/* when we get an incoming http request on /_oauth/{serviceName}. This
/* handler should use that information to fetch data about the user
/* logging in.
/*
/* @param name {String} e.g. "google", "facebook"
/* @param version {Number} OAuth version (1 or 2)
/* @param urls   For OAuth1 only, specify the service's urls
/* @param handleOauthRequest {Function(oauthBinding|query)}
/*   - (For OAuth1 only) oauthBinding {OAuth1Binding} bound to the appropriate provider
/*   - (For OAuth2 only) query {Object} parameters passed in query string
/*   - return value is:
/*     - {serviceData:, (optional options:)} where serviceData should end
/*       up in the user's services[name] field
/*     - `null` if the user declined to give permissions
*/
const OAuth$registerService = (name, version, urls, handleOauthRequest) => {
  if (registeredServices[name])
    throw new Error(`Already registered the ${name} OAuth service`);

  registeredServices[name] = {
    serviceName: name,
    version,
    urls,
    handleOauthRequest,
  };
};


// The state parameter is normally generated on the client using
// `btoa`, but for tests we need a version that runs on the server.
//
const OAuth$_generateState = (loginStyle, credentialToken, redirectUrl) => {
  return Buffer.from(JSON.stringify({
    loginStyle: loginStyle,
    credentialToken: credentialToken,
    redirectUrl: redirectUrl})).toString('base64');
};

const OAuth$_stateFromQuery = query => {
  let string;
  try {
    string = Buffer.from(query.state, 'base64').toString('binary');
  } catch (e) {
    Log.warn(`Unable to base64 decode state from OAuth query: ${query.state}`);
    throw e;
  }

  try {
    return JSON.parse(string);
  } catch (e) {
    Log.warn(`Unable to parse state from OAuth query: ${string}`);
    throw e;
  }
};

const OAuth$_loginStyleFromQuery = query => {
  let style;
  // For backwards-compatibility for older clients, catch any errors
  // that result from parsing the state parameter. If we can't parse it,
  // set login style to popup by default.
  try {
    style = OAuth$_stateFromQuery(query).loginStyle;
  } catch (err) {
    style = "popup";
  }
  if (style !== "popup" && style !== "redirect") {
    throw new Error(`Unrecognized login style: ${style}`);
  }
  return style;
};

const OAuth$_credentialTokenFromQuery = query => {
  let state;
  // For backwards-compatibility for older clients, catch any errors
  // that result from parsing the state parameter. If we can't parse it,
  // assume that the state parameter's value is the credential token, as
  // it used to be for older clients.
  try {
    state = OAuth$_stateFromQuery(query);
  } catch (err) {
    return query.state;
  }
  return state.credentialToken;
};

const OAuth$_isCordovaFromQuery = query => {
  try {
    return !! OAuth$_stateFromQuery(query).isCordova;
  } catch (err) {
    // For backwards-compatibility for older clients, catch any errors
    // that result from parsing the state parameter. If we can't parse
    // it, assume that we are not on Cordova, since older Meteor didn't
    // do Cordova.
    return false;
  }
};

// Checks if the `redirectUrl` matches the app host.
// We export this function so that developers can override this
// behavior to allow apps from external domains to login using the
// redirect OAuth flow.
const OAuth$_checkRedirectUrlOrigin = redirectUrl => {
  const appHost = Meteor.absoluteUrl();
  const appHostReplacedLocalhost = Meteor.absoluteUrl(undefined, {
    replaceLocalhost: true
  });
  return (
    redirectUrl.substr(0, appHost.length) !== appHost &&
    redirectUrl.substr(0, appHostReplacedLocalhost.length) !== appHostReplacedLocalhost
  );
};

const middleware = async (req, res, next) => {
  let requestData;

  // Make sure to catch any exceptions because otherwise we'd crash
  // the runner
  try {
    const serviceName = oauthServiceName(req);
    if (!serviceName) {
      // not an oauth request. pass to next middleware.
      next();
      return;
    }

    const service = registeredServices[serviceName];

    // Skip everything if there's no service set by the oauth middleware
    if (!service) {
      res.writeHead(500, {
        'Content-Type': 'text/plain'
      });
      res.end(`Unexpected OAuth service`)
      return
    }

    // Make sure we're configured
    ensureConfigured(serviceName);

    const handler = OAuth$_requestHandlers[service.version];
    if (!handler) {
      res.writeHead(500, {
        'Content-Type': 'text/plain'
      });
      res.end(`Unexpected OAuth version`)
      return
    }

    if (req.method === 'GET') {
      requestData = req.query;
    } else {
      requestData = req.body;
    }

    await handler(service, requestData, res);
  } catch (err) {
    // if we got thrown an error, save it off, it will get passed to
    // the appropriate login call (if any) and reported there.
    //
    // The other option would be to display it in the popup tab that
    // is still open at this point, ignoring the 'close' or 'redirect'
    // we were passed. But then the developer wouldn't be able to
    // style the error or react to it in any way.
    if (requestData?.state && err instanceof Error) {
      try { // catch any exceptions to avoid crashing runner
        _storePendingCredential(OAuth$_credentialTokenFromQuery(requestData), err);
      } catch (err) {
        // Ignore the error and just give up. If we failed to store the
        // error, then the login will just fail with a generic error.
        Log.warn("Error in OAuth Server while storing pending login result.\n" +
                 err.stack || err.message);
      }
    }

    // close the popup. because nobody likes them just hanging
    // there.  when someone sees this multiple times they might
    // think to check server logs (we hope?)
    // Catch errors because any exception here will crash the runner.
    try {
      OAuth$_endOfLoginResponse(res, {
        query: requestData,
        loginStyle: OAuth$_loginStyleFromQuery(requestData),
        error: err
      });
    } catch (err) {
      Log.warn("Error generating end of login response\n" +
               (err && (err.stack || err.message)));
    }
  }
};

// Listen to incoming OAuth http requests
WebApp.connectHandlers.use('/_oauth', bodyParser.json());
WebApp.connectHandlers.use('/_oauth', bodyParser.urlencoded({ extended: false }));
WebApp.connectHandlers.use(middleware);

// Handle /_oauth/* paths and extract the service name.
//
// @returns {String|null} e.g. "facebook", or null if this isn't an
// oauth request
const oauthServiceName = req => {
  // req.url will be "/_oauth/<service name>" with an optional "?close".
  const i = req.url.indexOf('?');
  let barePath;
  if (i === -1)
    barePath = req.url;
  else
    barePath = req.url.substring(0, i);
  const splitPath = barePath.split('/');

  // Any non-oauth request will continue down the default
  // middlewares.
  if (splitPath[1] !== '_oauth')
    return null;

  // Find service based on url
  const serviceName = splitPath[2];
  return serviceName;
};

// Make sure we're configured
const ensureConfigured = serviceName => {
  if (!ServiceConfiguration.configurations.findOne({service: serviceName})) {
    throw new ServiceConfiguration.ConfigError();
  }
};

const isSafe = value => {
  // This matches strings generated by `Random.secret` and
  // `Random.id`.
  return typeof value === "string" &&
    /^[a-zA-Z0-9\-_]+$/.test(value);
};

// Internal: used by the oauth1 and oauth2 packages
const OAuth$_renderOauthResults = (res, query, credentialSecret) => {
  // For tests, we support the `only_credential_secret_for_test`
  // parameter, which just returns the credential secret without any
  // surrounding HTML. (The test needs to be able to easily grab the
  // secret and use it to log in.)
  //
  // XXX only_credential_secret_for_test could be useful for other
  // things beside tests, like command-line clients. We should give it a
  // real name and serve the credential secret in JSON.

  if (query.only_credential_secret_for_test) {
    res.writeHead(200, {'Content-Type': 'text/html'});
    res.end(credentialSecret, 'utf-8');
  } else {
    const details = {
      query,
      loginStyle: OAuth$_loginStyleFromQuery(query)
    };
    if (query.error) {
      details.error = query.error;
    } else {
      const token = OAuth$_credentialTokenFromQuery(query);
      const secret = credentialSecret;
      if (token && secret &&
          isSafe(token) && isSafe(secret)) {
        details.credentials = { token: token, secret: secret};
      } else {
        details.error = "invalid_credential_token_or_secret";
      }
    }

    OAuth$_endOfLoginResponse(res, details);
  }
};

// This "template" (not a real Spacebars template, just an HTML file
// with some ##PLACEHOLDER##s) communicates the credential secret back
// to the main window and then closes the popup.
const OAuth$_endOfPopupResponseTemplate = (config, nonce) => `<!DOCTYPE html>
<html>
<body>
<p id="completedText" style="display:none;">
    Login completed. <a href="#" id="loginCompleted">
      Click here</a> to close this window.
</p>
<script type="text/javascript" nonce="${nonce}">
var config = JSON.parse(${JSON.stringify(JSON.stringify(config))});

if (config.setCredentialToken) {
  var credentialToken = config.credentialToken;
  var credentialSecret = config.credentialSecret;

  try {
    localStorage[config.storagePrefix + credentialToken] = credentialSecret;
  } catch (err) {
    // We can't do much else, but at least close the popup instead
    // of having it hang around on a blank page.
  }
}

document.getElementById("completedText").style.display = "block";
document.getElementById("loginCompleted").onclick = function() { window.close() };
window.close();
</script>
</body>
</html>
`
const OAuth$_endOfRedirectResponseTemplate = (config, nonce) => `<!DOCTYPE html>
<html>
<body>
<script type="text/javascript" nonce="${nonce}">
var config = JSON.parse(${JSON.stringify(JSON.stringify(config))});

if (config.setCredentialToken) {
  try {
    sessionStorage[config.storagePrefix + config.credentialToken] =
      config.credentialSecret;
  } catch (err) {
    // We can't do much else, but at least the redirects goes on.
  }
}
window.location =
  config.redirectUrl
    ? config.redirectUrl.replace(/&amp;/g, "&")
    : config.redirectUrl;
</script>
</body>
</html>
`;

// Renders the end of login response template into some HTML and JavaScript
// that closes the popup or redirects at the end of the OAuth flow.
//
// options are:
//   - loginStyle ("popup" or "redirect")
//   - setCredentialToken (boolean)
//   - credentialToken
//   - credentialSecret
//   - redirectUrl
//   - isCordova (boolean)
//
const renderEndOfLoginResponse = (options, nonce) => {
  const config = {
    setCredentialToken: !! options.setCredentialToken,
    credentialToken: options.credentialToken,
    credentialSecret: options.credentialSecret,
    storagePrefix: OAuth$_storageTokenPrefix,
    redirectUrl: options.redirectUrl,
    isCordova: !! options.isCordova
  };

  if (options.loginStyle === 'popup') {
    return OAuth$_endOfPopupResponseTemplate(config, nonce);
  } else if (options.loginStyle === 'redirect') {
    return OAuth$_endOfRedirectResponseTemplate(config, nonce);
  } else {
    throw new Error(`invalid loginStyle: ${options.loginStyle}`);
  }
};

// Writes an HTTP response to the popup window at the end of an OAuth
// login flow. At this point, if the user has successfully authenticated
// to the OAuth server and authorized this app, we communicate the
// credentialToken and credentialSecret to the main window. The main
// window must provide both these values to the DDP `login` method to
// authenticate its DDP connection. After communicating these vaues to
// the main window, we close the popup.
//
// We export this function so that developers can override this
// behavior, which is particularly useful in, for example, some mobile
// environments where popups and/or `window.opener` don't work. For
// example, an app could override `OAuth._endOfPopupResponse` to put the
// credential token and credential secret in the popup URL for the main
// window to read them there instead of using `window.opener`. If you
// override this function, you take responsibility for writing to the
// request and calling `res.end()` to complete the request.
//
// Arguments:
//   - res: the HTTP response object
//   - details:
//      - query: the query string on the HTTP request
//      - credentials: { token: *, secret: * }. If present, this field
//        indicates that the login was successful. Return these values
//        to the client, who can use them to log in over DDP. If
//        present, the values have been checked against a limited
//        character set and are safe to include in HTML.
//      - error: if present, a string or Error indicating an error that
//        occurred during the login. This can come from the client and
//        so shouldn't be trusted for security decisions or included in
//        the response without sanitizing it first. Only one of `error`
//        or `credentials` should be set.
const OAuth$_endOfLoginResponse = (res, details) => {
  const nonce = crypto.randomBytes(16).toString("hex");
  res.writeHead(200, {
    'Content-Type': 'text/html',
    'Content-Security-Policy': `default-src 'none'; script-src 'nonce-${nonce}'; style-src 'self' 'unsafe-inline';`
  });

  let redirectUrl;
  if (details.loginStyle === 'redirect') {
    redirectUrl = OAuth$_stateFromQuery(details.query).redirectUrl;
    const appHost = Meteor.absoluteUrl();
    if (
      !Meteor.settings?.packages?.oauth?.disableCheckRedirectUrlOrigin &&
      OAuth$_checkRedirectUrlOrigin(redirectUrl)) {
      details.error = `redirectUrl (${redirectUrl}` +
        `) is not on the same host as the app (${appHost})`;
      redirectUrl = appHost;
    }
  }

  const isCordova = OAuth$_isCordovaFromQuery(details.query);

  if (details.error) {
    Log.warn("Error in OAuth Server: " +
             (details.error instanceof Error ?
              details.error.message : details.error));
    res.end(renderEndOfLoginResponse({
      loginStyle: details.loginStyle,
      setCredentialToken: false,
      redirectUrl,
      isCordova,
    }, nonce), "utf-8");
    return;
  }

  // If we have a credentialSecret, report it back to the parent
  // window, with the corresponding credentialToken. The parent window
  // uses the credentialToken and credentialSecret to log in over DDP.
  res.end(renderEndOfLoginResponse({
    loginStyle: details.loginStyle,
    setCredentialToken: true,
    credentialToken: details.credentials.token,
    credentialSecret: details.credentials.secret,
    redirectUrl,
    isCordova,
  }, nonce), "utf-8");
};


const OAuthEncryption = Package["oauth-encryption"] && Package["oauth-encryption"].OAuthEncryption;

const usingOAuthEncryption = () =>
  OAuthEncryption && OAuthEncryption.keyIsLoaded();

// Encrypt sensitive service data such as access tokens if the
// "oauth-encryption" package is loaded and the oauth secret key has
// been specified.  Returns the unencrypted plaintext otherwise.
//
// The user id is not specified because the user isn't known yet at
// this point in the oauth authentication process.  After the oauth
// authentication process completes the encrypted service data fields
// will be re-encrypted with the user id included before inserting the
// service data into the user document.
//
const OAuth$sealSecret = plaintext => {
  if (usingOAuthEncryption())
    return OAuthEncryption.seal(plaintext);
  else
    return plaintext;
};

// Unencrypt a service data field, if the "oauth-encryption"
// package is loaded and the field is encrypted.
//
// Throws an error if the "oauth-encryption" package is loaded and the
// field is encrypted, but the oauth secret key hasn't been specified.
//
const OAuth$openSecret = (maybeSecret, userId) => {
  if (!Package["oauth-encryption"] || !OAuthEncryption.isSealed(maybeSecret))
    return maybeSecret;

  return OAuthEncryption.open(maybeSecret, userId);
};

// Unencrypt fields in the service data object.
//
const OAuth$openSecrets = (serviceData, userId) => {
  const result = {};
  Object.keys(serviceData).forEach(key =>
    result[key] = OAuth$openSecret(serviceData[key], userId)
  );
  return result;
};

const OAuth$_addValuesToQueryParams = (
  values = {},
  queryParams = new URLSearchParams()
) => {
  Object.entries(values).forEach(([key, value]) => {
    queryParams.set(key, `${value}`);
  });
  return queryParams;
};

const OAuth$_fetch = async (
  url,
  method = 'GET',
  { headers = {}, queryParams = {}, body, ...options } = {}
) => {
  const urlWithParams = new URL(url);

  OAuth$_addValuesToQueryParams(queryParams, urlWithParams.searchParams);

  const requestOptions = {
    method: method.toUpperCase(),
    headers,
    ...(body ? { body } : {}),
    ...options,
  };
  return fetch(urlWithParams.toString(), requestOptions);
};

export {
  OAuth$_redirectUri as  _redirectUri,
  OAuth$_storageTokenPrefix as  _storageTokenPrefix,
  OAuth$_requestHandlers as _requestHandlers,
  OAuth$registerService as registerService,
  _retrievePendingCredential as retrieveCredential,
  OAuth$_generateState as _generateState,
  OAuth$_stateFromQuery as _stateFromQuery,
  OAuth$_loginStyleFromQuery as _loginStyleFromQuery,
  OAuth$_credentialTokenFromQuery as _credentialTokenFromQuery,
  OAuth$_isCordovaFromQuery as _isCordovaFromQuery,
  OAuth$_checkRedirectUrlOrigin as _checkRedirectUrlOrigin,
  OAuth$_renderOauthResults as _renderOauthResults,
  OAuth$_endOfPopupResponseTemplate as _endOfPopupResponseTemplate,
  OAuth$_endOfRedirectResponseTemplate as _endOfRedirectResponseTemplate,
  OAuth$_endOfLoginResponse as _endOfLoginResponse,
  OAuth$sealSecret as sealSecret,
  OAuth$openSecret as openSecret,
  OAuth$openSecrets as openSecrets,
  OAuth$_addValuesToQueryParams as _addValuesToQueryParams,
  OAuth$_fetch as _fetch,
};
