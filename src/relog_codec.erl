%%
%%   Copyright 2016 Dmitry Kolesnikov, All Rights Reserved
%%
%%   Licensed under the Apache License, Version 2.0 (the "License");
%%   you may not use this file except in compliance with the License.
%%   You may obtain a copy of the License at
%%
%%       http://www.apache.org/licenses/LICENSE-2.0
%%
%%   Unless required by applicable law or agreed to in writing, software
%%   distributed under the License is distributed on an "AS IS" BASIS,
%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%   See the License for the specific language governing permissions and
%%   limitations under the License.
%%
%% @doc
%%   serialization protocol
-module(relog_codec).

-compile({parse_transform, category}).

-include("relog.hrl").
-include_lib("semantic/include/semantic.hrl").

-export([
   % typeof/1
   create/3
,  encode/3
,  decode/2
]).

-define(whildcard(X), (X =:= '_' orelse X =:= undefined orelse is_list(X))).

type_code(?XSD_ANYURI) -> ?_XSD_ANYURI;
type_code(?XSD_STRING) -> ?_XSD_STRING;
type_code(?XSD_INTEGER) -> ?_XSD_INTEGER;
type_code(?XSD_BYTE) -> ?_XSD_BYTE;
type_code(?XSD_SHORT) -> ?_XSD_SHORT;
type_code(?XSD_INT) -> ?_XSD_INT;
type_code(?XSD_LONG) -> ?_XSD_LONG;
type_code(?XSD_DECIMAL) -> ?_XSD_DECIMAL;
type_code(?XSD_FLOAT) -> ?_XSD_FLOAT;
type_code(?XSD_DOUBLE) -> ?_XSD_DOUBLE;
type_code(?XSD_BOOLEAN) -> ?_XSD_BOOLEAN;
type_code(?XSD_DATETIME) -> ?_XSD_DATETIME;
type_code(?XSD_DATE) -> ?_XSD_DATE;
type_code(?XSD_TIME) -> ?_XSD_TIME;
type_code(?XSD_YEARMONTH) -> ?_XSD_YEARMONTH;
type_code(?XSD_MONTHDAY) -> ?_XSD_MONTHDAY;
type_code(?XSD_YEAR) -> ?_XSD_YEAR;
type_code(?XSD_MONTH) -> ?_XSD_MONTH;
type_code(?XSD_DAY) -> ?_XSD_DAY;
type_code(?GEORSS_POINT) -> ?_GEORSS_POINT;
type_code(?GEORSS_HASH) -> ?_GEORSS_HASH;
type_code(_) -> undefined.

%%
%%
create(Sock, ?XSD_ANYURI, {iri, _} = IRI) ->
   [either ||
      relog_writer:uid(Sock, IRI)
   ,  cats:unit(<<?_XSD_ANYURI:8, _/binary>>)
   ];
create(Sock, ?XSD_ANYURI, {iri, _, _} = IRI) ->
   [either ||
      relog_writer:uid(Sock, IRI)
   ,  cats:unit(<<?_XSD_ANYURI:8, _/binary>>)
   ];
create(Sock, Type, Val) ->
   encode(Sock, Type, Val).

%%
%%
encode(_, undefined, X) when ?whildcard(X) ->
   {ok, <<>>};

encode(_, Type, X) when ?whildcard(X) ->
   case type_code(Type) of
      undefined -> {ok, <<>>};
      Code -> {ok, <<Code:8>>}
   end;

encode(Sock, ?XSD_ANYURI, {iri, _} = IRI) ->
   [either ||
      relog_reader:uid(Sock, IRI)
   ,  cats:unit(<<?_XSD_ANYURI:8, _/binary>>)
   ];
encode(Sock, ?XSD_ANYURI, {iri, _, _} = IRI) ->
   [either ||
      relog_reader:uid(Sock, IRI)
   ,  cats:unit(<<?_XSD_ANYURI:8, _/binary>>)
   ];

encode(_, ?XSD_STRING, Val)
 when is_binary(Val) ->
   {ok, <<?_XSD_STRING:8, Val/binary>>};

encode(_, ?XSD_INTEGER, Val)
 when is_integer(Val) ->
   {ok, <<?_XSD_INTEGER:8, Val:64>>};

encode(_, ?XSD_BYTE, Val)
 when is_integer(Val) ->
   {ok, <<?_XSD_BYTE:8, Val:8>>};

encode(_, ?XSD_SHORT, Val)
 when is_integer(Val) ->
   {ok,<<?_XSD_SHORT:8, Val:16>>};

encode(_, ?XSD_INT, Val)
 when is_integer(Val) ->
   {ok,<<?_XSD_INT:8, Val:32>>};

encode(_, ?XSD_LONG, Val)
 when is_integer(Val) ->
   {ok,<<?_XSD_LONG:8, Val:64>>};

encode(_, ?XSD_DECIMAL, Val)
 when is_float(Val) ->
   {ok,<<?_XSD_DECIMAL:8, Val:64/float>>};

encode(_, ?XSD_FLOAT, Val)
 when is_float(Val) ->
   {ok,<<?_XSD_FLOAT:8, Val:32/float>>};

encode(_, ?XSD_DOUBLE, Val)
 when is_float(Val) ->
   {ok,<<?_XSD_DOUBLE:8, Val:64/float>>};

encode(_, ?XSD_BOOLEAN, true) ->
   {ok,<<?_XSD_BOOLEAN:8, 1:8>>};

encode(_, ?XSD_BOOLEAN, false) ->
   {ok,<<?_XSD_BOOLEAN:8, 0:8>>};

encode(_, ?XSD_DATETIME, {A, B, C}) ->
   {ok,<<?_XSD_DATETIME:8, A:32, B:32, C:32>>};

encode(_, ?XSD_YEAR, {{Y, 0, 0}, {0, 0, 0}}) ->
   {ok,<<?_XSD_YEAR:8, Y:32>>};

encode(_, ?XSD_MONTH, {{0, M, 0}, {0, 0, 0}}) ->
   {ok,<<?_XSD_MONTH:8, M:8>>};

encode(_, ?XSD_DAY, {{0, 0, D}, {0, 0, 0}}) ->
   {ok,<<?_XSD_DAY:8, D:8>>};

encode(_, ?XSD_YEARMONTH, {{Y, M, 0}, {0, 0, 0}}) ->
   {ok,<<?_XSD_YEARMONTH:8, Y:32, M:8>>};

encode(_, ?XSD_MONTHDAY, {{0, M, D}, {0, 0, 0}}) ->
   {ok,<<?_XSD_MONTHDAY:8, M:8, D:8>>};

encode(_, ?XSD_DATE, {{Y, M, D}, {0, 0, 0}}) ->
   {ok,<<?_XSD_DATE:8, Y:32, M:8, D:8>>};

encode(_, ?XSD_TIME, {{0, 0, 0}, {T, N, S}}) ->
   {ok,<<?_XSD_TIME:8, T:8, N:8, S:8>>};

encode(_, ?GEORSS_POINT, {Lat, Lng}) ->
   {ok,<<?_GEORSS_POINT:8, Lat:64/float, Lng:64/float>>};

encode(_, ?GEORSS_HASH, Hash) ->
   {ok,<<?_GEORSS_HASH:8, Hash/binary>>};

encode(_, Type, Val) ->
   {error, {unsupported, Type, Val}}.


%%
%%
decode(Sock, <<?_XSD_ANYURI:8, Uid/binary>>) ->
   {ok, IRI} = relog:iri(Sock, Uid),
   {ok, ?XSD_ANYURI, IRI};

decode(_, <<?_XSD_STRING:8, Val/binary>>) ->
   {ok, ?XSD_STRING, Val};

decode(_, <<?_XSD_INTEGER:8, Val:64>>) ->
   {ok, ?XSD_INTEGER, Val};

decode(_, <<?_XSD_BYTE:8, Val:8>>) ->
   {ok, ?XSD_BYTE, Val};

decode(_, <<?_XSD_SHORT:8, Val:16>>) ->
   {ok, ?XSD_SHORT, Val};

decode(_, <<?_XSD_INT:8, Val:32>>) ->
   {ok, ?XSD_INT, Val};

decode(_, <<?_XSD_LONG:8, Val:64>>) ->
   {ok, ?XSD_LONG, Val};

decode(_, <<?_XSD_DECIMAL:8, Val:64/float>>) ->
   {ok, ?XSD_DECIMAL, Val};

decode(_, <<?_XSD_FLOAT:8, Val:32/float>>) ->
   {ok, ?XSD_FLOAT, Val};

decode(_, <<?_XSD_DOUBLE:8, Val:64/float>>) ->
   {ok, ?XSD_DOUBLE, Val};

decode(_, <<?_XSD_BOOLEAN:8, 1:8>>) ->
   {ok, ?XSD_BOOLEAN, true};

decode(_, <<?_XSD_BOOLEAN:8, 0:8>>) ->
   {ok, ?XSD_BOOLEAN, false};

decode(_, <<?_XSD_DATETIME:8, A:32, B:32, C:32>>) ->
   {ok, ?XSD_DATETIME, {A, B, C}};

decode(_, <<?_XSD_YEAR:8, Y:32>>) ->
   {ok, ?XSD_YEAR, {{Y, 0, 0}, {0, 0, 0}}};

decode(_, <<?_XSD_MONTH:8, M:8>>) ->
   {ok, ?XSD_MONTH, {{0, M, 0}, {0, 0, 0}}};

decode(_, <<?_XSD_DAY:8, D:8>>) ->
   {ok, ?XSD_DAY, {{0, 0, D}, {0, 0, 0}}};

decode(_, <<?_XSD_YEARMONTH:8, Y:32, M:8>>) ->
   {ok, ?XSD_YEARMONTH, {{Y, M, 0}, {0, 0, 0}}};

decode(_, <<?_XSD_MONTHDAY:8, M:8, D:8>>) ->
   {ok, ?XSD_MONTHDAY, {{0, M, D}, {0, 0, 0}}};

decode(_, <<?_XSD_DATE:8, Y:32, M:8, D:8>>) ->
   {ok, ?XSD_DATE, {{Y, M, D}, {0, 0, 0}}};

decode(_, <<?_XSD_TIME:8, T:8, N:8, S:8>>) ->
   {ok, ?XSD_TIME, {{0, 0, 0}, {T, N, S}}};

decode(_, <<?_GEORSS_POINT:8, Lat:64/float, Lng:64/float>>) ->
   {ok, ?GEORSS_POINT, {Lat, Lng}};

decode(_, <<?_GEORSS_HASH:8, Hash/binary>>) ->
   {ok, ?GEORSS_HASH, Hash};

decode(_, <<Type:8, Val/binary>>) ->
   {error, {unsupported, Type, Val}}.
