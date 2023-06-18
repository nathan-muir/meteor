import { Accounts } from "meteor/accounts-base";

const attemptToMatchHash = (accounts, hash, success) => {
  // All of the special hash URLs we support for accounts interactions
  ["reset-password", "verify-email", "enroll-account"].forEach(urlPart => {
    let token;

    const tokenRegex = new RegExp(`^\\#\\/${urlPart}\\/(.*)$`);
    const match = hash.match(tokenRegex);

    if (match) {
      token = match[1];

      // XXX COMPAT WITH 0.9.3
      if (urlPart === "reset-password") {
        accounts._resetPasswordToken = token;
      } else if (urlPart === "verify-email") {
        accounts._verifyEmailToken = token;
      } else if (urlPart === "enroll-account") {
        accounts._enrollAccountToken = token;
      }
    } else {
      return;
    }

    // If no handlers match the hash, then maybe it's meant to be consumed
    // by some entirely different code, so we only clear it the first time
    // a handler successfully matches. Note that later handlers reuse the
    // savedHash, so clearing window.location.hash here will not interfere
    // with their needs.
    window.location.hash = "";

    // Do some stuff with the token we matched
    success.call(accounts, token, urlPart);
  });
}

Tinytest.add("accounts - parse urls for accounts-password", test => {
    const actions = ["reset-password", "verify-email", "enroll-account"];

    // make sure the callback was called the right number of times
    const actionsParsed = [];

    actions.forEach(hashPart => {
      const fakeToken = "asdf";

      const hashTokenOnly = `#/${hashPart}/${fakeToken}`;
      attemptToMatchHash(hashTokenOnly, (token, action) => {
        test.equal(token, fakeToken);
        test.equal(action, hashPart);

        // XXX COMPAT WITH 0.9.3
        if (hashPart === "reset-password") {
          test.equal(Accounts._resetPasswordToken, fakeToken);
        } else if (hashPart === "verify-email") {
          test.equal(Accounts._verifyEmailToken, fakeToken);
        } else if (hashPart === "enroll-account") {
          test.equal(Accounts._enrollAccountToken, fakeToken);
        }

        // Reset variables for the next test
        Accounts._resetPasswordToken = null;
        Accounts._verifyEmailToken = null;
        Accounts._enrollAccountToken = null;

        actionsParsed.push(action);
      });
    });

    // make sure each action is called once, in order
    test.equal(actionsParsed, actions);
  });
