# delve.rs

A crate search engine powered by Rust and [BonsaiDb][bonsaidb].

## Why?

Try searching for `proc-macro` or `proc macro` on
[crates.io](https://crates.io/search?q=proc-macro) or
[lib.rs](https://lib.rs/search?q=proc-macro), and review the top results. One of
the most popular crates related to procedural macros is `proc-macro2`, yet as of
2023-03-23 it isn't anywhere in the top results on either engine.

I ([@ecton][ecton]) also was looking for interesting projects to
[dogfood][dogfood] my primary project, [BonsaiDb][bonsaidb].

This seemed like a good opportunity to use BonsaiDb and potentially provide a
good tool for the Rust community.

[ecton]: https://github.com/ecton
[dogfood]: https://en.wikipedia.org/wiki/Eating_your_own_dog_food
[bonsaidb]: https://github.com/khonsulabs/bonsaidb
