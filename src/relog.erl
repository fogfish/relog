-module(relog).

-export([start/0]).
-export([
   socket/2,
   uid/2,
   iri/2,
   put/2,
   match/2,
   q/2,
   f/1
]).


-type sock() :: _.

start() ->
   applib:boot(?MODULE, []).


socket(Host, Port) ->
   eredis:start_link(Host, Port).


%%
%% associate iri with unique identity or return existed one
-spec uid(sock(), semantic:iri()) -> {ok, uid:g()} | {error, _}.

uid(Sock, IRI) ->
   relog_writer:uid(Sock, IRI).


%%
%% lookup iri associated with unique identity
-spec iri(sock(), uid:g()) -> {ok, semantic:iri()} | {error, _}.

iri(Sock, Uid) ->
   relog_reader:iri(Sock, Uid).


%%
%%
-spec put(sock(), semantic:spock()) -> {ok, uid:l()}.

put(Sock, Spock) ->
   relog_writer:put(Sock, Spock).


%%
%%
q(Expr, Sock) ->
   stream:map(fun(X) -> relog_codec:decode(Sock, X) end, datalog:q(Expr, Sock)).   


%%
%% sigma function   
%% @todo:
%%   * detect iri and replace them to compact form
f(Expr) ->
   relog_reader:sigma(Expr).


%%
%% encode key 
% enkey(spo, #{s := S, p := P, o := O}) -> 
%    [encode(key, S), 16#ff, encode(key, P), 16#ff, encode(key, O)];
% enkey(sop, #{s := S, p := P, o := O}) -> 
%    [encode(key, S), 16#ff, encode(key, O), 16#ff, encode(key, P)];
% enkey(pso, #{s := S, p := P, o := O}) -> 
%    [encode(key, P), 16#ff, encode(key, S), 16#ff, encode(key, O)];
% enkey(pos, #{s := S, p := P, o := O}) -> 
%    [encode(key, P), 16#ff, encode(key, O), 16#ff, encode(key, S)];
% enkey(ops, #{s := S, p := P, o := O}) -> 
%    [encode(key, O), 16#ff, encode(key, P), 16#ff, encode(key, S)];
% enkey(osp, #{s := S, p := P, o := O}) -> 
%    [encode(key, O), 16#ff, encode(key, S), 16#ff, encode(key, P)].

% dekey(spo, Key) ->
%    [S, P, O] = decode(key, Key),
%    #{s => S, p => P, o => O};
% dekey(sop, Key) ->
%    [S, O, P] = decode(key, Key),
%    #{s => S, p => P, o => O};
% dekey(pso, Key) ->
%    [P, S, O] = decode(key, Key),
%    #{s => S, p => P, o => O};
% dekey(pos, Key) ->
%    [P, O, S] = decode(key, Key),
%    #{s => S, p => P, o => O};
% dekey(ops, Key) ->
%    [O, P, S] = decode(key, Key),
%    #{s => S, p => P, o => O};
% dekey(osp, Key) ->
%    [O, S, P] = decode(key, Key),
%    #{s => S, p => P, o => O}.


%%
%% encode
% encode(key, Val) ->
%    %% key is uid: use uid:encode( uid:g() ) to safe memory ()
%    escape(erlang:term_to_binary(Val, [{minor_version, 1}]));
% encode(val, Val) ->
%    erlang:term_to_binary(Val, [{minor_version, 1}]).

% escape(Val) ->
%    << <<(if X >= 16#f0, X =< 16#ff -> <<16#f0, (16#f0 bxor X)>>; true -> <<X>> end)/binary>> 
%       || <<X:8>> <= Val >>.

%%
%% decode
% decode(key, Key) ->
%    [erlang:binary_to_term(unescape(X)) 
%       || X <- binary:split(Key, <<16#ff>>, [global])
%    ];

% decode(val, Val) ->
%    erlang:binary_to_term(Val).

%%
%% 
% unescape(Val) ->
%    erlang:iolist_to_binary(unescape1(Val)).

% unescape1(Val) ->
%    case binary:split(Val, <<16#f0>>) of
%       [Head, <<H:8, T/binary>>] ->
%          [Head, (16#f0 bxor H) | unescape1(T)];
%       List ->
%          List
%    end.

%%
%% encode statement, replace urn with unique identity
fact(FD, Spock) ->
   maps:map(
      fun
      (_, {iri, _} = IRI) ->
         {ok, Uid} = uid(FD, IRI), 
         Uid;
      (_, {iri, _, _} = IRI) ->
         {ok, Uid} = uid(FD, IRI), 
         Uid;
      (_, Val) ->
         Val
      end,
      Spock
   ).




match(Sock, Spock) ->
   Fact = fact(Sock, Spock),
   Uid  = key(Fact),
   maps:fold(fun filter/3,
      stream:map(
         fun(X) ->
            relog_codec:decode_spo(Uid, X)
         end,
         relog_stream:new(Sock, Uid, scalar:s(pattern(Uid, Fact)))
      ),
      Fact
   ).

% sin(Sock, Uid, Pat) ->
%    {ok, L} = eredis:q(Sock, ["ZRANGEBYLEX", Uid, <<$[,Pat/binary>>, "+"]),
%    % io:format("==> ~p ~p~n", [Uid, R]),
%    stream:build([{X, undefined} || X <- L]).


%%
%% encode match prefix
pattern(spo, Pattern) ->
   join(16#ff, [pat(s, Pattern), pat(p, Pattern), pat(o, Pattern)]);
pattern(sop, Pattern) ->
   join(16#ff, [pat(s, Pattern), pat(o, Pattern), pat(p, Pattern)]);
pattern(pso, Pattern) ->
   join(16#ff, [pat(p, Pattern), pat(s, Pattern), pat(o, Pattern)]);
pattern(pos, Pattern) ->
   join(16#ff, [pat(p, Pattern), pat(o, Pattern), pat(s, Pattern)]);
pattern(ops, Pattern) ->
   join(16#ff, [pat(o, Pattern), pat(p, Pattern), pat(s, Pattern)]);
pattern(osp, Pattern) ->
   join(16#ff, [pat(o, Pattern), pat(s, Pattern), pat(p, Pattern)]).

join(X, [H|T]) ->
   [H | [[X,Y] || Y <- T, Y =/= [] ]].

%%
%%
pat(Key, Pattern) ->
   case maps:get(Key, Pattern, []) of
      X when is_list(X) ->
         [];
      X ->
         relog_codec:encode_term(X)
   end.




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
-define(is_pat(X), (not is_list(X))).
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




