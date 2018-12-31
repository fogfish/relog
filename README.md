# relog 

[Datalog](https://en.wikipedia.org/wiki/Datalog) query support for Redis using the [parser](https://github.com/fogfish/datalog) implemented on Erlang.

## Inspiration 

Declarative logic programs is an alternative approach to extract knowledge facts in deductive databases. It is designed for applications that uses a large number of ground facts persisted in external storage. The library implements experimental support of datalog language to query knowledge facts stored in Redis.

## Getting started

The latest version of the library is available at its `master` branch. All development, including new features and bug fixes, take place on the `master` branch using forking and pull requests as described in contribution guidelines.

### Installation

If you use `rebar3` you can include the library in your project with

```erlang
{relog, ".*",
   {git, "https://github.com/fogfish/relog", {branch, master}}
}
```

### Usage

The library requires Redis as a storage back-end. Use the docker container for development purpose.

```bash
docker run -p 6379:6379 -d redis
```
