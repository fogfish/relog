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

Build library and run the development console

```bash
make && make run
```

Let's **deploy** example linked [movie dataset](https://github.com/fogfish/datalog/blob/master/priv/imdb.nt).

```erlang
relog:start().

%% 
%% establish connection with redis
{ok, Sock} = relog:socket("localhost", 6379).

%%
%% open a stream to example dataset
Stream = semantic:nt(filename:join([code:priv_dir(datalog), "imdb.nt"])).

%%
%% upload dataset
stream:foreach(
   fun(Fact) -> {ok, _} = relog:append(Sock, Fact) end,
   stream:map(fun semantic:typed/1, Stream)
).
```

The library support only subject-predicate-object relation

#### Basic queries

Match a person from dataset using query goals

```erlang
%%
%% define a query goal to match a person with `name` equal to `Ridley Scott`.
%% An identity rule is used to produce stream of tuples 
Q = "
   ?- person(_, \"Ridley Scott\").

   f(s, p, o).

   person(id, name) :- f(id, schema:name, name).
".

%%
%% parse and compile a query into executable function
F = datalog:c(relog, datalog:p(Q)).

%%
%% apply the function to dataset and materialize a stream of tuple, it returns
%% [
%%    [{iri,<<"http://example.org/person/137">>}, <<"Ridley Scott">>]
%% ]
stream:list(F(Sock)).
```

**Data patterns**

```erlang
%%
%% define a query to discover all movies produces in 1987
Q = "
   ?- movie(_, _).

   f(s, p, o).

   movie(id, title) :- 
      f(id, schema:year, 1987),
      f(id, schema:title, title).
".

%%
%% parse and compile a query into executable function
F = datalog:c(relog, datalog:p(Q)).

%%
%% apply the function to dataset and materialize a stream of tuple, it returns
%% [
%%    [{iri,<<"http://example.org/movie/202">>}, <<"Predator">>],
%%    [{iri,<<"http://example.org/movie/203">>}, <<"Lethal Weapon">>],
%%    [{iri,<<"http://example.org/movie/204">>}, <<"RoboCop">>]
%% ]
stream:list(F(Sock)).
```

**Infix Predicates**

```erlang
%%
%% define a query to discover all movies produces  before 1984
Q = "
   ?- movie(_, _).

   f(s, p, o).

   movie(title, year) :- 
      f(id, schema:year, year),
      f(id, schema:title, title),
      year < 1984.
".

%%
%% parse and compile a query into executable function
F = datalog:c(relog, datalog:p(Q)).

%%
%%
%% apply the function to dataset and materialize a stream of tuple, it returns
%% [
%%    [<<"First Blood">>, 1982],
%%    [<<"Alien">>, 1979],
%%    [<<"Mad Max">>, 1979],
%%    [<<"Mad Max 2">>, 1981]
%% ]
stream:list(F(Sock)).
```

**Join relations**

```erlang
%%
%% define a query to discover actors of all movies produced before 1984
Q = "
   ?- actors(_, _).

   f(s, p, o).

   actors(title, name) :- 
      f(id, schema:year, year),
      f(id, schema:cast, cast),
      f(id, schema:title, title),
      f(cast, schema:name, name),
      year < 1984.
".

%%
%% parse and compile a query into executable function
F = datalog:c(relog, datalog:p(Q)).

%%
%%
%% apply the function to dataset and materialize a stream of tuple, it returns
%% [
%%    [<<"First Blood">>,<<"Sylvester Stallone">>],
%%    [<<"First Blood">>,<<"Richard Crenna">>],
%%    [<<"First Blood">>,<<"Brian Dennehy">>],
%%    [<<"Mad Max">>,<<"Mel Gibson">>],
%%    [<<"Mad Max">>,<<"Steve Bisley">>],
%%    [<<"Mad Max">>,<<"Joanne Samuel">>],
%%    [<<"Mad Max 2">>,<<"Mel Gibson">>],
%%    [<<"Mad Max 2">>,<<"Michael Preston">>],
%%    [<<"Mad Max 2">>,<<"Bruce Spence">>],
%%    [<<"Alien">>,<<"Tom Skerritt">>],
%%    [<<"Alien">>,<<"Sigourney Weaver">>],
%%    [<<"Alien">>,<<"Veronica Cartwright">>]
%% ]
stream:list(F(Sock)).
```

```erlang
%%
%% define a query to discover all movies with Sylvester Stallone
Q = "
   ?- movie(_).

   f(s, p, o).

   movie(title) :-
      f(cast, schema:name, \"Sylvester Stallone\"),
      f(film, schema:cast, cast),
      f(film, schema:title, title).
".

%%
%% parse and compile a query into executable function
F = datalog:c(relog, datalog:p(Q)).

%%
%%
%% apply the function to dataset and materialize a stream of tuple, it returns
%% [
%%    [<<"First Blood">>],
%%    [<<"Rambo: First Blood Part II">>],
%%    [<<"Rambo III">>]
%% ]
stream:list(F(Sock)).
```

### More Information

* study datalog language and its [Erlang implementation](https://github.com/fogfish/datalog)


## How to Contribute

`elasticlog` is Apache 2.0 licensed and accepts contributions via GitHub pull requests.

### getting started

* Fork the repository on GitHub
* Read the README.md for build instructions
* Make pull request

### commit message

The commit message helps us to write a good release note, speed-up review process. The message should address two question what changed and why. The project follows the template defined by chapter [Contributing to a Project](http://git-scm.com/book/ch5-2.html) of Git book.

>
> Short (50 chars or less) summary of changes
>
> More detailed explanatory text, if necessary. Wrap it to about 72 characters or so. In some contexts, the first line is treated as the subject of an email and the rest of the text as the body. The blank line separating the summary from the body is critical (unless you omit the body entirely); tools like rebase can get confused if you run the two together.
> 
> Further paragraphs come after blank lines.
> 
> Bullet points are okay, too
> 
> Typically a hyphen or asterisk is used for the bullet, preceded by a single space, with blank lines in between, but conventions vary here
>

## Bugs

If you detect a bug, please bring it to our attention via GitHub issues. Please make your report detailed and accurate so that we can identify and replicate the issues you experience:
- specify the configuration of your environment, including which operating system you're using and the versions of your runtime environments
- attach logs, screen shots and/or exceptions if possible
- briefly summarize the steps you took to resolve or reproduce the problem


## License

Copyright 2016 Dmitry Kolesnikov

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
