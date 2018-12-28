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
-include_lib("semantic/include/semantic.hrl").

-export([
   encode_type/1
,  decode_type/1
,  encode/3
,  encode_iri/1
,  decode/3
,  decode_iri/1
]).

%%
%%
encode_type(?XSD_ANYURI) -> 0;
encode_type(?XSD_STRING) -> 1;
encode_type(?XSD_INTEGER) -> 2.

%%
%%
decode_type(0) -> ?XSD_ANYURI;
decode_type(1) -> ?XSD_STRING;
decode_type(2) -> ?XSD_INTEGER.


%%
%%
encode(_, _, '_') ->
   <<>>;

encode(_, _, Val)
 when is_list(Val) ->
   <<>>;

encode(Sock, ?XSD_ANYURI, {iri, _} = IRI) ->
   {ok, Uid} = relog_writer:uid(Sock, IRI),
   Uid;

encode(Sock, ?XSD_ANYURI, {iri, _, _} = IRI) ->
   {ok, Uid} = relog_writer:uid(Sock, IRI),
   Uid;

encode(Sock, ?XSD_ANYURI, IRI)
 when is_binary(IRI) ->
   {ok, Uid} = relog_writer:uid(Sock, {iri, IRI}),
   Uid;

encode(_, ?XSD_STRING, Val) ->
   Val;

encode(_, _, Val) ->
   erlang:term_to_binary(Val, [{minor_version, 2}]).


%%
%%
encode_iri({iri, Prefix, Suffix}) ->
   <<Prefix/binary, $:, Suffix/binary>>;
encode_iri({iri, Urn}) ->
   Urn;
encode_iri(IRI)
 when is_binary(IRI) ->
   IRI.

%%
%%
decode(Sock, ?XSD_ANYURI, Uid) ->
   {ok, IRI} = relog:iri(Sock, Uid),
   IRI;

decode(_, ?XSD_STRING, Val) ->
   Val;

decode(_, _, Val) ->
   erlang:binary_to_term(Val).


%%
%%
decode_iri(<<Iri/binary>>) ->
   case semantic:compact(Iri) of
      undefined ->
         semantic:absolute(Iri);
      Semantic ->
         Semantic
   end;
decode_iri(Iri) ->
   semantic:absolute(Iri).

