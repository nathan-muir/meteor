// This is passed into a weird `makeErrorType` function that expects its thing
// to be a constructor
function connectionErrorConstructor(message) {
  this.message = message;
}

export const ConnectionError = Meteor.makeErrorType(
  'DDP.ConnectionError',
  connectionErrorConstructor
);

export const ForcedReconnectError = Meteor.makeErrorType(
  'DDP.ForcedReconnectError',
  () => {}
);
