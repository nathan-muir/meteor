import URL from 'url';
import { buildUrl } from './url_common.js';
export { encodeParams } from './url_common.js';

export function constructUrl(url, query, params) {
  var url_parts = URL.parse(url);
  return buildUrl(
    url_parts.protocol + "//" + url_parts.host + url_parts.pathname,
    url_parts.search,
    query,
    params
  );
}
