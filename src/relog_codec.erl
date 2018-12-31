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
-include_lib("semantic/include/semantic.hrl").

-export([
   encode/2
,  encode/3
,  decode/2
]).

%%
%%
encode(Sock, ?XSD_ANYURI, {iri, _} = IRI) ->
   [either ||
      relog:uid(Sock, IRI)
   ,  cats:unit(<<0:8, _/binary>>)
   ];

encode(Sock, ?XSD_ANYURI, {iri, _, _} = IRI) ->
   [either ||
      relog:uid(Sock, IRI)
   ,  cats:unit(<<0:8, _/binary>>)
   ];

encode(Sock, ?XSD_ANYURI, IRI)
 when is_binary(IRI) ->
   [either ||
      relog:uid(Sock, IRI)
   ,  cats:unit(<<0:8, _/binary>>)
   ];

encode(_, ?XSD_ANYURI, IRI)
 when IRI =:= '_' orelse is_list(IRI) ->
   {ok, <<0:8>>};


encode(_, ?XSD_STRING, Val)
 when is_binary(Val) ->
   {ok, <<1:8, Val/binary>>};

encode(_, ?XSD_STRING, Val)
 when Val =:= '_' orelse is_list(Val) ->
   {ok, <<1:8>>};


encode(_, ?XSD_INTEGER, Val)
 when is_integer(Val) ->
   {ok,<<2:8, Val:64>>};

encode(_, ?XSD_INTEGER, Val)
 when Val =:= '_' orelse is_list(Val) ->
   {ok, <<2:8>>};


encode(_, Type, Val) ->
   {error, {unsupported, Type, Val}}.

%%
%%
encode(_, '_') ->
   {ok, <<>>};

encode(_, Val)
 when is_list(Val) ->
   {ok, <<>>};

encode(Sock, <<"iri:", IRI/binary>>) -> 
   encode(Sock, ?XSD_ANYURI, IRI);

encode(Sock, <<"iri+", IRI/binary>>) ->
   encode(Sock, ?XSD_ANYURI, IRI);

encode(Sock, Val)
 when is_binary(Val) ->
   encode(Sock, ?XSD_STRING, Val);

encode(Sock, Val)
 when is_integer(Val) ->
   encode(Sock, ?XSD_INTEGER, Val);

encode(_, Val) ->
   {error, {unsupported, Val}}.


%%
%%
decode(Sock, <<0:8, Uid/binary>>) ->
   {ok, IRI} = relog:iri(Sock, Uid),
   {ok, ?XSD_ANYURI, IRI};

decode(_, <<1:8, Val/binary>>) ->
   {ok, ?XSD_STRING, Val};

decode(_, <<2:8, Val:64>>) ->
   {ok, ?XSD_INTEGER, Val};

decode(_, <<Type:8, Val/binary>>) ->
   {error, {unsupported, Type, Val}}.


% %%
% %%
% decode(Sock, ?XSD_ANYURI, Uid) ->
%    {ok, IRI} = relog:iri(Sock, Uid),
%    IRI;

% decode(_, ?XSD_STRING, Val) ->
%    Val;

% decode(_, _, Val) ->
%    erlang:binary_to_term(Val).
