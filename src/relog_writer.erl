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
-module(relog_writer).
-include_lib("semantic/include/semantic.hrl").

-export([
   uid/2
,  append/3
]).

%%
%%
uid(Sock, {iri, _} = IRI) ->
   uid(Sock, relog_codec:encode_iri(IRI));
uid(Sock, {iri, _, _} = IRI) ->
   uid(Sock, relog_codec:encode_iri(IRI));
uid(Sock, IRI)
 when is_binary(IRI) ->
   case eredis:q(Sock, ["HGET", "iri", IRI]) of
      {ok, undefined} ->
         uid(Sock, IRI, uid:encode(uid:g()));
      {ok, Uid} ->
         {ok, Uid}
   end.

uid(Sock, IRI, Uid) ->
   case 
      eredis:qp(Sock, [
         ["HSETNX", "iri", IRI, Uid],
         ["HSETNX", "uid", Uid, IRI]
      ]) 
   of
      [{ok, undefined} | _] ->
         uid(Sock, IRI);
      [{ok, _} | _] ->
         {ok, Uid}
   end.

%%
%%
append(Sock, {_, _, _} = Fact, Timeout) ->
   append(Sock, semantic:typed(Fact), Timeout);

append(Sock, #{s := S, p := P, o:= O, type := Type} = Fact, Timeout) ->
   ReS = relog_codec:encode(Sock, ?XSD_ANYURI, S),
   ReP = relog_codec:encode(Sock, ?XSD_ANYURI, P),
   ReO = relog_codec:encode(Sock, Type, O),
   Len = erlang:byte_size(ReO),
   ReT = relog_codec:encode_type(Type),
   %% @todo: validate that each index written 
   eredis:qp(Sock, [
      ["ZADD", <<$1, ReS/binary>>, "0", <<ReP:12/binary, ReT:8, ReO/binary>>],
      ["ZADD", <<$2, ReS/binary>>, "0", <<ReT:8, Len:32, ReO/binary, ReP:12/binary>>],

      ["ZADD", <<$3, ReP/binary>>, "0", <<ReS:12/binary, ReT:8, ReO/binary>>],
      ["ZADD", <<$4, ReP/binary>>, "0", <<ReT:8, Len:32, ReO/binary, ReS:12/binary>>],

      ["ZADD", <<$5, ReT:8, ReO/binary>>, "0", <<ReP:12/binary, ReS:12/binary>>],
      ["ZADD", <<$6, ReT:8, ReO/binary>>, "0", <<ReS:12/binary, ReP:12/binary>>]      
   ]).
