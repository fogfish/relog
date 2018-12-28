%%
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
   {<<$1, S/binary>>, add(P, <<T, O/binary>>)};
pattern(sop, S, P, O, T) ->
   {<<$2, S/binary>>, add(<<T, O/binary>>, P)};
pattern(pso, S, P, O, T) ->
   {<<$3, P/binary>>, add(S, <<T, O/binary>>)};
pattern(pos, S, P, O, T) ->
   {<<$4, P/binary>>, add(<<T, O/binary>>, S)};
pattern(ops, S, P, O, T) ->
   {<<$5, T, O/binary>>, add(P, S)};
pattern(osp, S, P, O, T) ->
   {<<$6, T, O/binary>>, add(S, P)}.

add(<<>>, X) -> X;
add(X, <<>>) -> X;
add(X, Y) -> <<X/binary, 16#ff, Y/binary>>.

%%
%%
decode(Sock, spo, <<$1, S/binary>>, <<P:12/binary, 16#ff, T:8, O/binary>>) ->
   decode(Sock, S, P, O, relog_codec:decode_type(T));
decode(Sock, sop, <<$2, S/binary>>, Value) ->
   [<<T:8, O/binary>>, P] = binary:split(Value, <<16#ff>>),
   decode(Sock, S, P, O, relog_codec:decode_type(T));
decode(Sock, pso, <<$3, P/binary>>, <<S:12/binary, 16#ff, T:8, O:12/binary>>) ->
   decode(Sock, S, P, O, relog_codec:decode_type(T));
decode(Sock, sop, <<$4, P/binary>>, Value) ->
   [<<T:8, O/binary>>, S] = binary:split(Value, <<16#ff>>),
   decode(Sock, S, P, O, relog_codec:decode_type(T));
decode(Sock, ops, <<$5, T:8, O/binary>>, <<P:12/binary, 16#ff, S/binary>>) ->
   decode(Sock, S, P, O, relog_codec:decode_type(T));
decode(Sock, osp, <<$6, T:8, O/binary>>, <<S:12/binary, 16#ff, P/binary>>) ->
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
