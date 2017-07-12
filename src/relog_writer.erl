%%
%%
-module(relog_writer).

-export([
   uid/2,
   put/2
]).

%%
%%
uid(Sock, IRI)
 when is_tuple(IRI) ->
   uid(Sock, relog_codec:encode_iri(IRI));

uid(Sock, IRI)
 when is_binary(IRI) ->
   % Len = size(IRI) - 1,
   % <<A:Len/binary, B/binary>> = IRI,
   case eredis:q(Sock, ["HGET", "iri", IRI]) of
   % case eredis:q(Sock, ["HGET", A, B]) of
      {ok, undefined} ->   
         % uid(Sock, IRI, uid:encode(uid:g()));
         uid(Sock, IRI, uid:encode(uid:l()));
      {ok, Uid} ->
         {ok, uid:decode(Uid)}
   end.

uid(Sock, IRI, Uid) ->
   % Leni = size(IRI) - 1,
   Lenu = size(Uid) - 2,
   % <<Ai:Leni/binary, Bi/binary>> = IRI,
   <<Au:Lenu/binary, Bu/binary>> = Uid,
   case 
      eredis:qp(Sock, [
         ["HSETNX", "iri", IRI, Uid],
         ["HSETNX", "uid", Uid, IRI]
         % ["SET", IRI, Uid, "NX"],
         % ["HSETNX", Au, Bu, IRI]
      ]) 
   of
   % case 
   %    eredis:qp(Sock, [
   %       ["SET", IRI, Uid, "NX"],
   %       ["SET", Uid, IRI, "NX"]
   %    ]) 
   % of
      % already exists
      [{ok, undefined} | _] ->
         uid(Sock, IRI);
      [{ok, _} | _] ->
         {ok, Uid}
   end.

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




