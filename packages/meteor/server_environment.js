export const Meteor$isProduction =
  __meteor_runtime_config__.meteorEnv.NODE_ENV === "production";
export const Meteor$isDevelopment = __meteor_runtime_config__.meteorEnv.NODE_ENV !== "production";
export const Meteor$isClient = false;
export const Meteor$isServer = true;
export const Meteor$isCordova = false;
// Server code runs in Node 8+, which is decidedly "modern" by any
// reasonable definition.
export const Meteor$isModern = true

export { Meteor$settings } from './server_settings.js';

export const Meteor$gitCommitHash = __meteor_runtime_config__.gitCommitHash;
