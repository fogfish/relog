%% @doc
%%   relog integration test suite (requires redis at localhost)
-module(relog_IT_SUITE).

-export([all/0]).

-export([
   intake/1
,  basic_match_string/1
,  basic_match_integer/1
,  basic_predicate_integer/1
]).

all() -> 
   [Test || {Test, NAry} <- ?MODULE:module_info(exports), 
      Test =/= module_info,
      Test =/= init_per_suite,
      Test =/= end_per_suite,
      NAry =:= 1
   ].

socket() ->
   relog:socket("localhost", 6379).

datalog(Expr) ->
   datalog:c(relog, datalog:p(Expr)).

%%
%%
intake(_) ->
   {ok, Sock} = socket(),

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
basic_match_string(_) ->
   {ok, Sock} = socket(),

   F = datalog("
      ?- person(_, \"Ridley Scott\").

      f(s, p, o) :- .stream().

      person(id, name) :- f(id, \"schema:name\", name). 
   "),
   
   [
      [{iri,<<"http://example.org/person/137">>}, <<"Ridley Scott">>]
   ] = stream:list(relog:q(F, Sock)),

   relog:close(Sock).

%%
%%
basic_match_integer(_) ->
   {ok, Sock} = socket(),

   F = datalog("
      ?- movie(_, _).

      f(s, p, o) :- .stream().

      movie(id, title) :- 
         f(id, \"schema:year\", 1987),
         f(id, \"schema:title\", title). 
   "),

   [
      [{iri,<<"http://example.org/movie/202">>}, <<"Predator">>]
   ,  [{iri,<<"http://example.org/movie/203">>}, <<"Lethal Weapon">>]
   ,  [{iri,<<"http://example.org/movie/204">>}, <<"RoboCop">>]
   ] = stream:list(relog:q(F, Sock)),

   relog:close(Sock).

%%
%%
basic_predicate_integer(_) ->
   {ok, Sock} = socket(),

   F = datalog("
      ?- movie(_, _).

      f(s, p, o) :- .stream().

      movie(title, year) :- 
         f(id, \"schema:year\", year),
         f(id, \"schema:title\", title),
         year < 1984. 
   "),

   [
      [<<"First Blood">>, 1982]
   ,  [<<"Alien">>, 1979]
   ,  [<<"Mad Max">>, 1979]
   ,  [<<"Mad Max 2">>, 1981]
   ] = stream:list(relog:q(F, Sock)),

   relog:close(Sock).


