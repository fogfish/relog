%% @doc
%%   integration test suite (requires redis at localhost)
%%
%%   docker run -p 6379:6379 -d redis
%%
-module(relog_types_IT_SUITE).
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
   xsd_anyuri/1
,  xsd_string/1
,  xsd_integer/1
,  xsd_decimal/1
,  xsd_boolean/1
,  xsd_dateTime/1
,  xsd_date/1
,  xsd_time/1
,  xsd_yearmonth/1
,  xsd_monthday/1
,  xsd_year/1
,  xsd_month/1
,  xsd_day/1
,  georss_point/1
,  georss_hash/1
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
%% Unit tests
%%
intake(Config) ->
   {ok, Sock} = relog:socket("localhost", 6379),

   Stream = semantic:nt(filename:join([?config(data_dir, Config), "datatypes.nt"])),
   stream:foreach(
      fun(Fact)->
         {ok, _} = relog:append(Sock, Fact)
      end,
      stream:map(fun semantic:typed/1, Stream)
   ),

   relog:close(Sock).

%%
%%
xsd_anyuri(_Config) ->
   [
      [_, _, {iri, <<"http://example.com/a">>}]
   ] = datalog("
      ?- datatypes(xsd:anyURI, _, _).

      datatypes(s, p, o).
   ").

%%
%%
xsd_string(_Config) ->
   [
      [_, _, <<"example">>]
   ] = datalog("
      ?- datatypes(xsd:string, _, _).

      datatypes(s, p, o).
   ").


xsd_integer(_Config) ->
   [
      [_, _, 123]
   ] = datalog("
      ?- datatypes(xsd:integer, _, _).

      datatypes(s, p, o).
   ").


xsd_decimal(_Config) ->
   [
      [_, _, 12.3]
   ] = datalog("
      ?- datatypes(xsd:decimal, _, _).

      datatypes(s, p, o).
   ").


xsd_boolean(_Config) ->
   [
      [_, _, true]
   ] = datalog("
      ?- datatypes(xsd:boolean, _, _).

      datatypes(s, p, o).
   ").


xsd_dateTime(_Config) ->
   [
      [_, _, {997, 265730, 0}]
   ] = datalog("
      ?- datatypes(xsd:dateTime, _, _).

      datatypes(s, p, o).
   ").


xsd_date(_Config) ->
   [
      [_, _, {{2001, 8, 8}, {0, 0, 0}}]
   ] = datalog("
      ?- datatypes(xsd:date, _, _).

      datatypes(s, p, o).
   ").


xsd_time(_Config) ->
   [
      [_, _, {{0, 0, 0}, {10, 15, 30}}]
   ] = datalog("
      ?- datatypes(xsd:time, _, _).

      datatypes(s, p, o).
   ").


xsd_yearmonth(_Config) ->
   [
      [_, _, {{2001, 8, 0}, {0, 0, 0}}]
   ] = datalog("
      ?- datatypes(xsd:gYearMonth, _, _).

      datatypes(s, p, o).
   ").


xsd_monthday(_Config) ->
   [
      [_, _, {{0, 8, 8}, {0, 0, 0}}]
   ] = datalog("
      ?- datatypes(xsd:gMonthDay, _, _).

      datatypes(s, p, o).
   ").


xsd_year(_Config) ->
   [
      [_, _, {{2001, 0, 0}, {0, 0, 0}}]
   ] = datalog("
      ?- datatypes(xsd:gYear, _, _).

      datatypes(s, p, o).
   ").


xsd_month(_Config) ->
   [
      [_, _, {{0, 8, 0}, {0, 0, 0}}]
   ] = datalog("
      ?- datatypes(xsd:gMonth, _, _).

      datatypes(s, p, o).
   ").


xsd_day(Config) ->
   [
      [_, _, {{0, 0, 8}, {0, 0, 0}}]
   ] = datalog("
      ?- datatypes(xsd:gDay, _, _).

      datatypes(s, p, o).
   ").

georss_point(Config) ->
   [
      [_, _, {60.1, 23.2}]
   ] = datalog("
      ?- datatypes(georss:point, _, _).

      datatypes(s, p, o).
   ").


georss_hash(Config) ->
   [
      [_, _, <<"ueh6xcb">>]
   ] = datalog("
      ?- datatypes(georss:hash, _, _).

      datatypes(s, p, o).
   ").
