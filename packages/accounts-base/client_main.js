import { Meteor } from "meteor/meteor";
import {
  AccountsClient,
} from "./accounts_client.js";

/**
 * @namespace Accounts
 * @summary The namespace for all client-side accounts-related methods.
 */
const Accounts = new AccountsClient();

/**
 * @summary A [Mongo.Collection](#collections) containing user documents.
 * @locus Anywhere
 * @type {Mongo.Collection}
 * @importFromPackage meteor
 */
Meteor.users = Accounts.users;

/**
 * @summary Get the current user id, or `null` if no user is logged in. A reactive data source.
 * @locus Anywhere but publish functions
 * @importFromPackage meteor
 */
Meteor.userId = () => Accounts.userId();

/**
 * @summary Get the current user record, or `null` if no user is logged in. A reactive data source.
 * @locus Anywhere but publish functions
 * @importFromPackage meteor
 * @param {Object} [options]
 * @param {MongoFieldSpecifier} options.fields Dictionary of fields to return or exclude.
 */
Meteor.user = options => Accounts.user(options);

/**
 * @summary Get the current user record, or `null` if no user is logged in. A reactive data source.
 * @locus Anywhere but publish functions
 * @importFromPackage meteor
 * @param {Object} [options]
 * @param {MongoFieldSpecifier} options.fields Dictionary of fields to return or exclude.
 */
Meteor.userAsync = options => Accounts.userAsync(options);

/**
 * @summary True if a login method (such as `Meteor.loginWithPassword`,
 * `Meteor.loginWithFacebook`, or `Accounts.createUser`) is currently in
 * progress. A reactive data source.
 * @locus Client
 * @importFromPackage meteor
 */
Meteor.loggingIn = () => Accounts.loggingIn();

/**
 * @summary True if a logout method (such as `Meteor.logout`) is currently in
 * progress. A reactive data source.
 * @locus Client
 * @importFromPackage meteor
 */
Meteor.loggingOut = () => Accounts.loggingOut();

/**
 * @summary Log the user out.
 * @locus Client
 * @param {Function} [callback] Optional callback. Called with no arguments on success, or with a single `Error` argument on failure.
 * @importFromPackage meteor
 */
Meteor.logout = callback => Accounts.logout(callback);

/**
 * @summary Log out other clients logged in as the current user, but does not log out the client that calls this function.
 * @locus Client
 * @param {Function} [callback] Optional callback. Called with no arguments on success, or with a single `Error` argument on failure.
 * @importFromPackage meteor
 */
Meteor.logoutOtherClients = callback => Accounts.logoutOtherClients(callback);

/**
 * @summary Login with a Meteor access token.
 * @locus Client
 * @param {Object} [token] Local storage token for use with login across
 * multiple tabs in the same browser.
 * @param {Function} [callback] Optional callback. Called with no arguments on
 * success.
 * @importFromPackage meteor
 */
Meteor.loginWithToken = (token, callback) =>
  Accounts.loginWithToken(token, callback);

///
/// HANDLEBARS HELPERS
///

// If our app has a Blaze, register the {{currentUser}} and {{loggingIn}}
// global helpers.
if (Package.blaze) {
  const { Template } = Package.blaze.Blaze;

  /**
   * @global
   * @name  currentUser
   * @isHelper true
   * @summary Calls [Meteor.user()](#meteor_user). Use `{{#if currentUser}}` to check whether the user is logged in.
   */
  Template.registerHelper('currentUser', () => Meteor.user());

  // TODO: the code above needs to be changed to Meteor.userAsync() when we have
  // a way to make it reactive using async.
  // Template.registerHelper('currentUserAsync',
  //  async () => await Meteor.userAsync());

  /**
   * @global
   * @name  loggingIn
   * @isHelper true
   * @summary Calls [Meteor.loggingIn()](#meteor_loggingin).
   */
  Template.registerHelper('loggingIn', () => Meteor.loggingIn());

  /**
   * @global
   * @name  loggingOut
   * @isHelper true
   * @summary Calls [Meteor.loggingOut()](#meteor_loggingout).
   */
  Template.registerHelper('loggingOut', () => Meteor.loggingOut());

  /**
   * @global
   * @name  loggingInOrOut
   * @isHelper true
   * @summary Calls [Meteor.loggingIn()](#meteor_loggingin) or [Meteor.loggingOut()](#meteor_loggingout).
   */
  Template.registerHelper(
    'loggingInOrOut',
    () => Meteor.loggingIn() || Meteor.loggingOut()
  );
}

export {
  Accounts,
  AccountsClient,
};
