%%
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
      ["ZADD", <<$1, ReS/binary>>, "0", <<ReP/binary, 16#ff, ReT:8, ReO/binary>>],
      ["ZADD", <<$2, ReS/binary>>, "0", <<ReT:8, ReO/binary, 16#ff, ReP/binary>>],

      ["ZADD", <<$3, ReP/binary>>, "0", <<ReS/binary, 16#ff, ReT:8, ReO/binary>>],
      ["ZADD", <<$4, ReP/binary>>, "0", <<ReT:8, ReO/binary, 16#ff, ReS/binary>>],

      ["ZADD", <<$5, ReT:8, ReO/binary>>, "0", <<ReP/binary, 16#ff, ReS/binary>>],
      ["ZADD", <<$6, ReT:8, ReO/binary>>, "0", <<ReS/binary, 16#ff, ReP/binary>>]      
   ]).







%%
%%
put(Sock, Spock) ->
   % @todo: use smart type detection
   T = scalar:s( semantic:typeof(Spock) ),
   % T = <<"lit">>,
   #{s := Sx, p := Px, o := Ox} = relog_codec:encode(Sock, Spock),
   S = relog_codec:encode_term(Sx),
   P = relog_codec:encode_term(Px),
   O = relog_codec:encode_term(T, Ox),
   % io:format("=> ~p ~p ~p~n", [S, P, O]),
   clue:inc(put),
   clue:inc(n),
   %% @todo: write object rel to special index
try
   case T of
      <<"rel">> ->
         eredis:qp(Sock, [
            ["ZADD", <<$1, S/binary>>, "0", <<P/binary, O/binary>>],
            ["ZADD", <<$2, S/binary>>, "0", <<O/binary, P/binary>>],
            % ["ZADD", <<$1, S/binary>>, "0", <<O/binary, P/binary>>],

            ["ZADD", <<$3, P/binary>>, "0", <<S/binary, O/binary>>],
            ["ZADD", <<$4, P/binary>>, "0", <<O/binary, S/binary>>],
            % ["ZADD", <<$3, P/binary>>, "0", <<O/binary, S/binary>>],

            ["ZADD", <<$5, O/binary>>, "0", <<P/binary, S/binary>>],
            ["ZADD", <<$6, O/binary>>, "0", <<S/binary, P/binary>>]      
            % ["ZADD", <<$5, O/binary>>, "0", <<S/binary, P/binary>>]      
         ]); 
      _ -> 
         eredis:qp(Sock, [
            ["ZADD", <<$1, S/binary>>, "0", <<P/binary, O/binary>>],
            ["ZADD", <<$2, S/binary>>, "0", <<O/binary, P/binary>>],
            % ["ZADD", <<$1, S/binary>>, "0", <<O/binary, P/binary>>],

            ["ZADD", <<$3, P/binary>>, "0", <<S/binary, O/binary>>],
            ["ZADD", <<$4, P/binary>>, "0", <<O/binary, S/binary>>],
            % ["ZADD", <<$3, P/binary>>, "0", <<O/binary, S/binary>>],

            ["ZADD", <<$5, T/binary>>, "0", <<O/binary, P/binary, S/binary>>],
            ["ZADD", <<$6, T/binary>>, "0", <<O/binary, S/binary, P/binary>>]      
            % ["ZADD", <<$5, T/binary>>, "0", <<O/binary, S/binary, P/binary>>]      
         ])
   end
catch _:_ ->
   io:format("==> ~p ~p ~p~n", [S, P, O])
end.
   % eredis:q_noreply(Sock, ["ZADD", scalar:c(spo), "0", relog_codec:encode_spo(spo, Fact)]),
   % eredis:q_noreply(Sock, ["ZADD", scalar:c(sop), "0", relog_codec:encode_spo(sop, Fact)]),
   % eredis:q_noreply(Sock, ["ZADD", scalar:c(pso), "0", relog_codec:encode_spo(pso, Fact)]),
   % eredis:q_noreply(Sock, ["ZADD", scalar:c(pos), "0", relog_codec:encode_spo(pos, Fact)]),
   % eredis:q_noreply(Sock, ["ZADD", scalar:c(ops), "0", relog_codec:encode_spo(ops, Fact)]),
   % eredis:q_noreply(Sock, ["ZADD", scalar:c(osp), "0", relog_codec:encode_spo(osp, Fact)]).
   % eredis:qp(Sock, [
   %    ["ZADD", scalar:c(X), "0", relog_codec:encode_spo(X, Fact)] || X <- [spo, sop, pso, pos, ops, osp]
   % ]).   

   %% use redis pipeline for hex index update
   %% use zadd with 0 see lex index
   %% http://redis.io/topics/indexes
   %% how to handle c - k ?




