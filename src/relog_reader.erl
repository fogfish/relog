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
-include_lib("semantic/include/semantic.hrl").

-export([
   iri/2
,  match/2
,  stream/2
]).

%%
%%
iri(Sock, {uid, _, _, _} = Uid) ->
   iri(Sock, uid:encode(Uid));

iri(Sock, Uid)
 when is_binary(Uid) ->
   case eredis:q(Sock, ["HGET", "uid", Uid]) of
      {ok, undefined} ->   
         {ok, Uid};
      {ok, IRI} ->
         {ok, IRI}
   end.

%%
%%
match(Sock, #{s := S, p := P, o := O, type := Type} = Pattern) ->
   Uid = index_of(Pattern),
   {Prefix, Query} = pattern(Uid,
      relog_codec:encode(Sock, ?XSD_ANYURI, S),
      relog_codec:encode(Sock, ?XSD_ANYURI, P),
      relog_codec:encode(Sock, Type, O),
      relog_codec:encode_type(Type)
   ),
   maps:fold(fun filter/3,
      stream:map(
         fun(X) -> decode(Sock, Uid, Prefix, X) end,
         relog_stream:new(Sock, Prefix, Query)
      ),
      Pattern
   ).

%%
%% encode match prefix
pattern(spo, S, P, O, T) ->
   {<<$1, S/binary>>, <<P/binary, T:8, O/binary>>};
pattern(sop, S, P, O, T) ->
   {<<$2, S/binary>>, <<(object(T, O))/binary, P/binary>>};
pattern(pso, S, P, O, T) ->
   {<<$3, P/binary>>, <<S/binary, T:8, O/binary>>};
pattern(pos, S, P, O, T) ->
   {<<$4, P/binary>>, <<(object(T, O))/binary, S/binary>>};
pattern(ops, S, P, O, T) ->
   {<<$5, T, O/binary>>, <<P/binary, S/binary>>};
pattern(osp, S, P, O, T) ->
   {<<$6, T, O/binary>>, <<S/binary, P/binary>>}.

object(T, <<>>) -> <<T:8>>;
object(T, O) -> <<T:8, (erlang:byte_size(O)):32, O/binary>>.


%%
%%
decode(Sock, spo, <<$1, S/binary>>, <<P:12/binary, T:8, O/binary>>) ->
   decode(Sock, S, P, O, relog_codec:decode_type(T));
decode(Sock, sop, <<$2, S/binary>>, <<T:8, Len:32, Suffix/binary>>) ->
   <<O:Len/binary, P:12/binary>> = Suffix,
   decode(Sock, S, P, O, relog_codec:decode_type(T));
decode(Sock, pso, <<$3, P/binary>>, <<S:12/binary, T:8, O:12/binary>>) ->
   decode(Sock, S, P, O, relog_codec:decode_type(T));
decode(Sock, sop, <<$4, P/binary>>, <<T:8, Len:32, Suffix/binary>>) ->
   <<O:Len/binary, S:12/binary>> = Suffix,
   decode(Sock, S, P, O, relog_codec:decode_type(T));
decode(Sock, ops, <<$5, T:8, O/binary>>, <<P:12/binary, S/binary>>) ->
   decode(Sock, S, P, O, relog_codec:decode_type(T));
decode(Sock, osp, <<$6, T:8, O/binary>>, <<S:12/binary, P/binary>>) ->
   decode(Sock, S, P, O, relog_codec:decode_type(T)).

decode(Sock, S, P, O, Type) ->
   #{
      s => relog_codec:decode(Sock, ?XSD_ANYURI, S)
   ,  p => relog_codec:decode(Sock, ?XSD_ANYURI, P)
   ,  o => relog_codec:decode(Sock, Type, O)
   ,  c => 1.0
   ,  k => uid:encode64( uid:l() )
   ,  type => Type
   }.

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


index_of(#{s := S, p := P})
 when ?is_pat(S), ?is_pat(P) -> 
   spo;
index_of(#{s := S, o := O})
 when ?is_pat(S), ?is_pat(O) ->
   sop;
index_of(#{p := P, o := O})
 when ?is_pat(P), ?is_pat(O) -> 
   ops;

index_of(#{s := S, p := P, o := O})
 when ?is_pat(S), ?ord(P, O) ->
   sop;
index_of(#{s := S, p := P, o := O})
 when ?is_pat(S), ?ord(O, P) ->
   sop;
index_of(#{s := S, o := _})
 when ?is_pat(S) ->
   sop;
index_of(#{s := S, p := _})
 when ?is_pat(S) ->
   spo;

index_of(#{s := S, p := P, o := O})
 when ?is_pat(P), ?ord(S, O) ->
   pos;
index_of(#{s := S, p := P, o := O})
 when ?is_pat(P), ?ord(O, S) ->
   pos;
index_of(#{p := P, o := _})
 when ?is_pat(P) ->
   pos;
index_of(#{s := _, p := P})
 when ?is_pat(P) ->
   pso;

index_of(#{s := S, p := P, o := O})
 when ?is_pat(O), ?ord(S, P) ->
   osp;
index_of(#{s := S, p := P, o := O})
 when ?is_pat(O), ?ord(P, S) ->
   ops;
index_of(#{s := _, o := O})
 when ?is_pat(O) ->
   osp;
index_of(#{p := _, o := O})
 when ?is_pat(O) ->
   ops;

index_of(#{s := _}) ->
   spo;

index_of(#{p := _}) ->
   pso;

index_of(#{o := _}) ->
   ops;

index_of(_) ->
   spo.

%%
%%
stream([Type], [S, P, O]) ->
   fun(Sock) ->
      stream:map(
         fun(#{s := Xs, p := Xp, o := Xo}) -> 
            [Xs, Xp, Xo] 
         end,
         relog:match(Sock, #{s => S, p => P, o => O, type => semantic:compact(Type)})
      )
   end.
