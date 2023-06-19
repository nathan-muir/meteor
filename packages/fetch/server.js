import fetch from 'node-fetch';
import { setMinimumBrowserVersions } from "meteor/modern-browsers";

export const Headers = fetch.Headers;
export const Request = fetch.Request;
export const Response = fetch.Response;

export { fetch };

// https://caniuse.com/#feat=fetch
setMinimumBrowserVersions({
  chrome: 42,
  edge: 14,
  firefox: 39,
  mobile_safari: [10, 3],
  opera: 29,
  safari: [10, 1],
  phantomjs: Infinity,
  // https://github.com/Kilian/electron-to-chromium/blob/master/full-versions.js
  electron: [0, 25],
}, module.id);
