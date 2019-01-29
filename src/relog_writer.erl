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

-compile({parse_transform, category}).
-include_lib("semantic/include/semantic.hrl").

-export([
   uid/2
,  append/3
]).

%%
%%
uid(Sock, {iri, Prefix, Suffix}) ->
   uid(Sock, <<Prefix/binary, $:, Suffix/binary>>);
uid(Sock, {iri, IRI}) ->
   uid(Sock, IRI);
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

append(Sock, #{s := S, p := P, o := O, type := T}, Timeout) ->
   [either ||
      Sx <- relog_codec:create(Sock, ?XSD_ANYURI, S),
      Px <- relog_codec:create(Sock, ?XSD_ANYURI, P),
      Ox <- relog_codec:create(Sock, T, O),
      cats:sequence(
         eredis:qp(Sock, [
            ["ZADD", <<$1, Sx/binary>>, "0", <<Px:13/binary, Ox/binary>>],
            ["ZADD", <<$2, Sx/binary>>, "0", <<Ox/binary, Px:13/binary>>],

            ["ZADD", <<$3, Px/binary>>, "0", <<Sx:13/binary, Ox/binary>>],
            ["ZADD", <<$4, Px/binary>>, "0", <<Ox/binary, Sx:13/binary>>],

            ["ZADD", <<$5, Ox/binary>>, "0", <<Px:13/binary, Sx:13/binary>>],
            ["ZADD", <<$6, Ox/binary>>, "0", <<Sx:13/binary, Px:13/binary>>]
         ], Timeout)
      )
   ].
