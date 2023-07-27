import RandomStream from 'meteor/ddp-common/random_stream.js';
import { CurrentMethodInvocation } from './environment.js';

// Returns the named sequence of pseudo-random values.
// The scope will be DDP._CurrentMethodInvocation.get(), so the stream will produce
// consistent values for method calls on the client and server.
export const randomStream = name => {
  const scope = CurrentMethodInvocation.get();
  return RandomStream.get(scope, name);
};
