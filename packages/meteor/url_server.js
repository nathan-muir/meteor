import URL from 'url';

export function parsePathPrefix(rootUrl) {
  var parsedUrl = URL.parse(rootUrl);
  // Sometimes users try to pass, eg, ROOT_URL=mydomain.com.
  if (!parsedUrl.host || ['http:', 'https:'].indexOf(parsedUrl.protocol) === -1) {
    throw Error("$ROOT_URL, if specified, must be an URL");
  }
  var pathPrefix = parsedUrl.pathname;
  if (pathPrefix.slice(-1) === '/') {
    // remove trailing slash (or turn "/" into "")
    pathPrefix = pathPrefix.slice(0, -1);
  }
  return pathPrefix;
}
