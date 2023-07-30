import { parsePathPrefix } from './url_server.js';
import { Meteor$settings } from './server_settings.js';

export const ACCOUNTS_CONNECTION_URL = undefined;
export const DDP_DEFAULT_CONNECTION_URL = undefined;
export const DISABLE_SOCKJS = process.env.DISABLE_SOCKJS;
export const PUBLIC_SETTINGS = Meteor$settings.public;
export const ROOT_URL = process.env.ROOT_URL;
export const ROOT_URL_PATH_PREFIX = ROOT_URL ? parsePathPrefix(ROOT_URL) : "";
export const gitCommitHash = undefined;
export const meteorEnv = {
  NODE_ENV: process.env.NODE_ENV || "production",
  TEST_METADATA: process.env.TEST_METADATA || "{}"
}
export const meteorRelease = undefined;
