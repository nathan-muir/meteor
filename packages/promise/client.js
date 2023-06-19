import { makeCompatible } from 'meteor-promise';
import './extensions.js';
const Promise = globalThis.Promise;
makeCompatible(Promise);

export { Promise };
