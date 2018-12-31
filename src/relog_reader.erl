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
-module(relog_reader).

-compile({parse_transform, category}).
-include_lib("semantic/include/semantic.hrl").

-export([
   iri/2
,  match/2
,  stream/2
]).

%%
%%
iri(Sock, Uid)
 when is_binary(Uid) ->
   case eredis:q(Sock, ["HGET", "uid", Uid]) of
      {ok, undefined} ->   
         {ok, {iri, Uid}};
      {ok, IRI} ->
         case semantic:compact(IRI) of
            undefined ->
               {ok, semantic:absolute(IRI)};
            Semantic ->
               {ok, Semantic}
         end
   end.

%%
%%
match(Sock, #{s := S, p := P, o := O, type := Type} = Pattern) ->
   Uid = key(Pattern),
   {ok, Sx} = relog_codec:encode(Sock, ?XSD_ANYURI, S),
   {ok, Px} = relog_codec:encode(Sock, ?XSD_ANYURI, P),
   {ok, Ox} = relog_codec:encode(Sock, Type, O),
   {Prefix, Query} = pattern(Uid, Sx, Px, Ox),
   maps:fold(fun filter/3,
      stream:map(
         fun(X) -> decode(Sock, Uid, Prefix, X) end,
         relog_stream:new(Sock, Prefix, Query)
      ),
      Pattern
   );

match(Sock, #{s := S, p := P, o := O} = Pattern) ->
   Uid = key(Pattern),
   {ok, Sx} = relog_codec:encode(Sock, ?XSD_ANYURI, S),
   {ok, Px} = relog_codec:encode(Sock, ?XSD_ANYURI, P),
   {ok, Ox} = relog_codec:encode(Sock, O),
   {Prefix, Query} = pattern(Uid, Sx, Px, Ox),
   maps:fold(fun filter/3,
      stream:map(
         fun(X) -> decode(Sock, Uid, Prefix, X) end,
         relog_stream:new(Sock, Prefix, Query)
      ),
      Pattern
   ).

%%
%% encode match prefix
pattern(spo, S, P, O) ->
   {<<$1, S/binary>>, join(P, O)};
pattern(sop, S, P, O) ->
   {<<$2, S/binary>>, join(O, P)};
pattern(pso, S, P, O) ->
   {<<$3, P/binary>>, join(S, O)};
pattern(pos, S, P, O) ->
   {<<$4, P/binary>>, join(O, S)};
pattern(ops, S, P, O) ->
   {<<$5, O/binary>>, join(P, S)};
pattern(osp, S, P, O) ->
   {<<$6, O/binary>>, join(S, P)}.

join(X, <<0>>) -> X;
join(<<0>>, Y) -> Y;
join(X,     Y) -> <<X/binary, Y/binary>>.

%%
%%
decode(Sock, spo, <<$1, S:13/binary>>, <<P:13/binary, O/binary>>) ->
   decode1(Sock, S, P, O);

decode(Sock, sop, <<$2, S:13/binary>>, Value) ->
   Len = byte_size(Value) - 13,
   <<O:Len/binary, P:13/binary>> = Value,
   decode1(Sock, S, P, O);

decode(Sock, pso, <<$3, P:13/binary>>, <<S:13/binary, O:13/binary>>) ->
   decode1(Sock, S, P, O);

decode(Sock, sop, <<$4, P:13/binary>>, Value) ->
   Len = byte_size(Value) - 13,
   <<O:Len/binary, S:13/binary>> = Value,
   decode1(Sock, S, P, O);

decode(Sock, ops, <<$5, O/binary>>, <<P:13/binary, S:13/binary>>) ->
   decode1(Sock, S, P, O);

decode(Sock, osp, <<$6, O/binary>>, <<S:13/binary, P:13/binary>>) ->
   decode1(Sock, S, P, O).

decode1(Sock, Sx, Px, Ox) ->
   {ok, _, S} = relog_codec:decode(Sock, Sx),
   {ok, _, P} = relog_codec:decode(Sock, Px),
   {ok, T, O} = relog_codec:decode(Sock, Ox),
   #{s => S, p => P, o => O, c => 1.0, k => uid:encode64(uid:l()), type => T}.

%%
%%
filter(Key, Val, Stream)
 when is_list(Val) ->
   lists:foldl(fun(F, Acc) -> fstream(F, Key, Acc) end, Stream, Val);

filter(_, _, Stream) ->
   Stream.

fstream({F, Val}, Key, Stream) ->
   stream:filter(
      fun(X) ->
         check(F, maps:get(Key, X), Val) 
      end, 
      Stream
   ).

check('>',  A, B) -> A >  B;
check('>=', A, B) -> A >= B;
check('<',  A, B) -> A  < B;
check('=<', A, B) -> A =< B.


%%
%% index selector
-define(is_pat(X), (not is_list(X) andalso X /= '_')).
-define(ord(X, Y), (is_list(X) andalso is_list(Y) andalso length(X) > length(Y))).

key(#{s := S, p := P})
 when ?is_pat(S), ?is_pat(P) -> 
   spo;
key(#{s := S, o := O})
 when ?is_pat(S), ?is_pat(O) ->
   sop;
key(#{p := P, o := O})
 when ?is_pat(P), ?is_pat(O) -> 
   ops;

key(#{s := S, p := P, o := O})
 when ?is_pat(S), ?ord(P, O) ->
   spo;
key(#{s := S, p := P, o := O})
 when ?is_pat(S), ?ord(O, P) ->
   sop;
key(#{s := S, p := _})
 when ?is_pat(S) ->
   spo;
key(#{s := S, o := _})
 when ?is_pat(S) ->
   sop;

key(#{s := S, p := P, o := O})
 when ?is_pat(P), ?ord(S, O) ->
   pso;
key(#{s := S, p := P, o := O})
 when ?is_pat(P), ?ord(O, S) ->
   pos;
key(#{s := _, p := P})
 when ?is_pat(P) ->
   pso;
key(#{p := P, s := _})
 when ?is_pat(P) ->
   pos;

key(#{s := S, p := P, o := O})
 when ?is_pat(O), ?ord(S, P) ->
   osp;
key(#{s := S, p := P, o := O})
 when ?is_pat(O), ?ord(P, S) ->
   ops;
key(#{s := _, o := O})
 when ?is_pat(O) ->
   osp;
key(#{p := _, o := O})
 when ?is_pat(O) ->
   ops;

key(#{s := _}) ->
   spo;

key(#{p := _}) ->
   pso;

key(#{o := _}) ->
   ops;

key(_) ->
   spo.


%%
%%
stream([], [S, P, O]) ->
   fun(Sock) ->
      stream:map(
         fun(#{s := Xs, p := Xp, o := Xo}) -> [Xs, Xp, Xo] end,
         relog:match(Sock, #{s => S, p => P, o => O})
      )
   end;

stream([Type], [S, P, O]) ->
   fun(Sock) ->
      stream:map(
         fun(#{s := Xs, p := Xp, o := Xo}) -> [Xs, Xp, Xo] end,
         relog:match(Sock, #{s => S, p => P, o => O, type => semantic:compact(Type)})
      )
   end.
