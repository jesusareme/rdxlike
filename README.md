# reduxlike

A generic redux-like runtime written in Rust (`rdxlib`), an example expenses-tracking core built on top of it
(`monilib`), and a small iOS app that consumes that core as its engine (`moniapp`).

It started as a learning project: a way to practice Rust on familiar conceptual ground, and to find out how
practical it is today to keep an app's logic in a shared Rust library while leaving the UI to each platform's
native framework.

## Who are you and why did you do this?
For the last couple of years I've been using (mostly) Max/MSP and gen~ to prototype audio effects and instruments
that I later use as part of Ableton Live sessions. But my discomfort with established DAWs has only increased as
time passes, so I decided to do something about it and start solidifying some of my ideas in a way other people
could potentially enjoy them.

With that in mind, I realized I would need to learn a so-called "systems programming language" if I was going to
pursue this goal. Swift, my main programming language for 8 years, has been increasingly adding lower level
capabilities, but they feel like an afterthought (and they really are) and, anyway, I felt like I needed some
fresh air and a new language to learn.

So I started learning C++. As much as I was amazed by its extensive set of features, I just could not deal with
how many things felt wrong to me about it (looking at you, undefined behavior everywhere!). After four
months I realized C++ felt much more like the past than the present or the future of programming, especially
coming from a modern, neat language such as Swift.

At that point I was already curious about Rust, and yes! that was, in fact, the language I had been looking and
hoping for. Smaller, mostly very well-designed, verbose about all the correct things to be verbose about, no
undefined behavior, memory safe, etc. It's been a pleasure to learn it and use it, it just feels like a
more-explicit-about-what's-happening-underneath version of Swift (one of my main criticisms about Swift is that,
in a very Apple way, I always felt like it was abstracting everything too much for the experienced developer).

## Why this project?
So, at some point halfway through the **Programming Rust** book (just before diving into async) I thought it was
time to start exercising what I had learnt so far, only with one foot on known territory. This brings me back to
2023, when I had the chance to join a small startup building a widely used and successful mobile app that
followed redux architecture principles. I had already dabbled a little bit with it on some small pet projects and
really loved the experience. And the whole idea of deriving complete app state from previous state plus an action
fits the way my brain works in a way no other conceptual idea about development had until that point.

Therefore, creating a small generic runtime in the spirit of that app, and applying what I learnt back then, all
in Rust, made complete sense for this project.

## Makes sense, but why am I seeing some expenses tracking logic all over the repo?
As a second order goal, I wanted to find out how hard it could possibly be today to write an app's shared logic
in Rust and use it across platforms. It turned out to be very easy! I find this approach of relying on the
platform UI frameworks while keeping app logic abstract and shareable between targets in the shape of a library
is not only totally viable, but also a much better option than the more established "just do a web app" approach
(or the "let's use a multiplatform framework for everything" one)[^1].

At the same time, I was about to start a small expenses tracking app for personal use, tired of not finding one
that worked the way I wanted, so the example logic in `monilib` is basically the seed of it, using the redux-like
`Runtime`. You can call it dogfooding the runtime myself as a way to check its adequacy for mobile app
development.

## Ok, what about all these folders filled with code?
This project is a Rust workspace with 2 crates, plus the iOS integration example app.

### `rdxlib`
The crate that holds the `Runtime`, or redux-like engine. It's generic over the `Client` trait, which works as a
marker gathering all the required type names and the constraints they need to satisfy to work correctly as part
of it. It also includes utilities for `Runtime` creation and communication, a small set of concurrency-related
primitives I implemented as exercises to understand some basic concurrency constructs a little better, and other
utilities, such as a `Subscriber` reference implementation called `OutputSubscriber`, ready to feed a
view-oriented output such as the ones `BoltFFI` (see [dependencies](#its-a-rust-project-so-which-are-your-favorite-dependencies-out-of-the-1589-you-surely-included)) provides.

### `monilib`
An example implementation of the `rdxlib` runtime applied to a multiplatform expenses core app idea, with some
CRUD-like flows already implemented, and a small number of view outputs used by `moniapp` to display data. Some
of these flows are included as an excuse to show different ways the runtime works and can be used, rather than to
depict real implementations **per se** (e.g. calculating the mean expense concurrently, as the example does, is
probably overkill for an expenses mobile app, but it makes sense in this context as a way to show an Async
Command at work). In the same way, implementation details for some of the flows are simplified, such as
everything persistence related.

### `moniapp`
A small example iOS app consuming `monilib` as the app engine. It shows several things at once: how to build an
app around the library, how to interact with it, and how to consume what the library produces as an
`AsyncStream`. The app only stores some minor view model state and dispatches actions to the library — that's all
it does, and that's the beauty of it.

## It's a Rust project, so, which are your favorite dependencies out of the 1589 you surely included?
I tried to stay within the `std` library bounds as much as possible; as I said above, the main goal was to start
learning the language and knowing what it provides and what it doesn't. But, as a contradictory goal, I also
wanted to start getting familiar with some crates I'll use again in the future:
- `serde`, the serialization crate, used in `monilib` to save/read state to/from disk.
- `boltffi`, a modern, relatively young FFI crate able to generate bindings and frameworks for multiple platforms
  at once, with very ergonomic results for the most part.
- `tracing` and `log` for logging.
- `jiff` as a modern datetime crate; I instantly missed `Calendar` (from `Foundation`) when I started this
  project.
- `rstest` for testing utilities such as fixtures and easy test parametrization.
- `proptest` for property testing.

## But surely all of this was an afternoon's worth of vibecoding
This project has taken me weeks of spare time writing source code by hand, and, in spite of its simplicity (or
maybe thanks to my focus on it being as simple as possible while still fully functional and usable) I had to
rewrite some parts several times until I was happy with the result. So no, it hasn't been vibecoded. I'm
convinced you cannot learn anything mildly hard if you include an LLM as part of the
**do - evaluate - fail - try again** loop[^2]. So my use of LLMs in this project has been:
- As a seldom used augmented search engine for some of the more complex Rust doubts.
- As an advanced search-and-replace tool. For instance: to help me unify the 100+ test names I had.

## Known limitations
> A work of art is never finished, only abandoned.[^3]

Not that this is a work of art at all, but at some point you have to stop, move on, and start applying new things
to new projects.

- Test coverage is over 90% on critical paths of `rdxlib`, but most of the rest is completely untested.
- The `VersionedArc` solution to avoid unnecessary clone operations of the expenses vector in `monilib` is
  relatively naive. It works well in this context, in the sense that it avoids most state clone operations while
  at the same time sharing state with subscribers or the persistence layer. But a complex application needs a
  more solid solution to this issue.
- The `Threadpool` implementation is very basic. Bring your own `JobsDispatcher`!
- I should have written an ADR (Architecture Decision Record); it would have been great to document the whole
  experience as a side effect. I will use them on my next project for sure.
- The project consciously doesn't use any async runtime. I hadn't gone deep into async when the project started
  and just wanted to practice with classic threading primitives, which I'm much less familiar with, coming from
  Swift.
- Logging should include the amazing `tracing` spans feature. It should not be too difficult to refactor the engine to include it as a first class citizen, but it's out of scope for this project.
- As I said before, the example implementations in `monilib` are not representative of a real world
  implementation. The persistence layer is shamelessly basic.
- The primitives should be their own crate, but I decided to keep them inside `rdxlib` for simplicity's sake.
- I'm leaving performance profiling for the next project, where it makes more sense. In the same way, I didn't get
to exercise snapshot testing.

## Are you retiring after this **magnum opus**?
Not yet, and I'm still young, but thanks. I have a long list of topics I'm eager to investigate to improve my
Rust skills and, above all, to find ways to apply them to audio application development.
- Explore more advanced concurrency approaches for the library architecture.
- Explore interesting ways to use the advantages of this kind of architecture, such as state branching.
- This specific example doesn't really show many of the advantages of isolating state this way, such as easy time
  travel between states.
- Go deeper into other concepts I have only used superficially (or not used at all) and that may or may not have
  interesting consequences for the approach I'm currently following: views, lenses, immutable data structures,
  etc.

## Where did you steal your ideas from?
- Redux documentation.
- Elm documentation.
- TCA documentation.
- You have to love every [Juan Pedro Bolivar's C++ talks](https://youtu.be/_oBx_NbLghY?si=xwsBtiFaqU4rYWKD).
- You may frown at my use of `&mut State`. It was my logical conclusion after first weeks using Rust, but [Niko Matsakis explains the underlying reason better than me.](https://smallcultfollowing.com/babysteps/blog/2018/02/01/in-rust-ordinary-vectors-are-values/)
- I intentionally tried to keep similar Rust projects out of sight during development as a way to force myself to find my own solutions.

## Why moni-things everywhere?
Read in Spanish, `moni` sounds roughly the same as `money`.

## License
MIT, see [LICENSE](LICENSE). Unless you state otherwise, any contribution you intentionally submit for
inclusion in this project is licensed under the same terms.

[^1]: Unless you want to do your bit for the worldwide quality slippery slope we're suffering, in which case
please continue using the one-solution-for-every-problem approach.

[^2]: Well, you can cheat yourself and think you have learnt something, but you
didn't and eventually you'll realize.

[^3]: I was unable to attribute the quote with complete certainty to anyone.