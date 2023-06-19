export { URL, URLSearchParams } from 'url';
export * from './bc/url_server.js'
import { setMinimumBrowserVersions } from 'meteor/modern-browsers';

// https://caniuse.com/#feat=url
setMinimumBrowserVersions({
   // Since there is no IE12, this effectively excludes Internet Explorer
  // (pre-Edge) from the modern classification. #9818 #9839
  ie: 12,
  chrome: 32,
  edge: 12,
  firefox: 26,
  mobile_safari: 8,
  opera: 36,
  safari: [7, 1],
  phantomjs: Infinity,
  // https://github.com/Kilian/electron-to-chromium/blob/master/full-versions.js
  electron: [0, 20],
}, module.id);
