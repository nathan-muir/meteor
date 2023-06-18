import { buildUrl } from './url_common.js';
export { encodeParams } from './url_common.js';

export function constructUrl(url, query, params) {
  var query_match = /^(.*?)(\?.*)?$/.exec(url);
  return buildUrl(
    query_match[1],
    query_match[2],
    query,
    params
  );
}
