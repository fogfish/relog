%% @doc
%%   relog integration test suite (requires redis at localhost)
%%
%%   docker run -p 6379:6379 -d redis
%%
-module(relog_IT_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1]).

-export([
   basic_facts_query/1
,  basic_facts_match/1
,  basic_facts_infix/1
,  join_facts_with_bag/1
,  join_facts/1
]).

all() -> 
   [Test || {Test, NAry} <- ?MODULE:module_info(exports), 
      Test =/= module_info,
      Test =/= init_per_suite,
      Test =/= end_per_suite,
      NAry =:= 1
   ].

init_per_suite(Config) ->
   {ok, _} = relog:start(),
   intake(Config),
   Config.

end_per_suite(_Config) ->
   ok.

%%
%%
datalog(Datalog) ->
   {ok, Sock} = relog:socket("localhost", 6379),
   List = stream:list(relog:q(datalog:c(relog, datalog:p(Datalog)), Sock)),
   relog:close(Sock),
   List.

%%
%%
intake(_) ->
   {ok, Sock} = relog:socket("localhost", 6379),

   Stream = semantic:nt(filename:join([code:priv_dir(datalog), "imdb.nt"])),
   stream:foreach(
      fun(Fact)->
         {ok, _} = relog:append(Sock, Fact)
      end,
      stream:map(fun semantic:typed/1, Stream)
   ),

   relog:close(Sock).


%%
%%
basic_facts_query(_) ->
   [
      [{iri,<<"http://example.org/person/137">>}, <<"Ridley Scott">>]
   ] = datalog("
      ?- person(_, \"Ridley Scott\").

      f(s, p, o).

      person(id, name) :- f(id, schema:name, name). 
   ").

%%
%%
basic_facts_match(_) ->
   [
      [{iri,<<"http://example.org/movie/202">>}, <<"Predator">>]
   ,  [{iri,<<"http://example.org/movie/203">>}, <<"Lethal Weapon">>]
   ,  [{iri,<<"http://example.org/movie/204">>}, <<"RoboCop">>]
   ] = datalog("
      ?- movie(_, _).

      f(s, p, o).

      movie(id, title) :- 
         f(id, schema:year, 1987),
         f(id, schema:title, title). 
   ").

%%
%%
basic_facts_infix(_) ->
   [
      [<<"First Blood">>, 1982]
   ,  [<<"Alien">>, 1979]
   ,  [<<"Mad Max">>, 1979]
   ,  [<<"Mad Max 2">>, 1981]
   ] = datalog("
      ?- movie(_, _).

      f(s, p, o).

      movie(title, year) :- 
         f(id, schema:year, year),
         f(id, schema:title, title),
         year < 1984. 
   ").

%%
%%
join_facts(_) ->
   [
      [<<"First Blood">>]
   ,  [<<"Rambo: First Blood Part II">>]
   ,  [<<"Rambo III">>]
   ] = datalog("
      ?- movie(_).

      f(s, p, o).

      movie(title) :-
         f(cast, schema:name, \"Sylvester Stallone\"),
         f(film, schema:cast, cast),
         f(film, schema:title, title).
   ").


%%
%%
join_facts_with_bag(_) ->
   [
      [<<"First Blood">>,<<"Sylvester Stallone">>]
   ,  [<<"First Blood">>,<<"Richard Crenna">>]
   ,  [<<"First Blood">>,<<"Brian Dennehy">>]
   ,  [<<"Alien">>,<<"Tom Skerritt">>]
   ,  [<<"Alien">>,<<"Sigourney Weaver">>]
   ,  [<<"Alien">>,<<"Veronica Cartwright">>]
   ,  [<<"Mad Max">>,<<"Mel Gibson">>]
   ,  [<<"Mad Max">>,<<"Steve Bisley">>]
   ,  [<<"Mad Max">>,<<"Joanne Samuel">>]
   ,  [<<"Mad Max 2">>,<<"Mel Gibson">>]
   ,  [<<"Mad Max 2">>,<<"Michael Preston">>]
   ,  [<<"Mad Max 2">>,<<"Bruce Spence">>]
   ] = datalog("
      ?- actors(_, _).

      f(s, p, o).

      actors(title, name) :- 
         f(id, schema:year, year),
         f(id, schema:cast, cast),
         f(id, schema:title, title),
         f(cast, schema:name, name),
         year < 1984. 
   ").



