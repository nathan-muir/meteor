import { Tinytest } from "meteor/tinytest";
import '../bc/url_tests';

Tinytest.add("url - sanity", function (test) {
  test.equal(typeof URL, "function");
  test.equal(typeof URLSearchParams, "function");
});
