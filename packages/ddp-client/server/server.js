import { CurrentMethodInvocation, CurrentPublicationInvocation } from '../common/environment.js';
import { ConnectionError, ForcedReconnectError } from '../common/errors.js';
import { randomStream } from '../common/random_stream.js';

/**
 * @namespace DDP
 * @summary Namespace for DDP-related methods/classes.
 */
export const DDP = {
  // This is private but it's used in a few places. accounts-base uses
  // it to get the current user. Meteor.setTimeout and friends clear
  // it. We can probably find a better way to factor this.
  _CurrentMethodInvocation: CurrentMethodInvocation,
  _CurrentPublicationInvocation: CurrentPublicationInvocation,
  // XXX: Keep DDP._CurrentInvocation for backwards-compatibility.
  _CurrentInvocation: CurrentMethodInvocation,
  ConnectionError,
  ForcedReconnectError,
  randomStream,
};
