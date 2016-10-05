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
   case eredis:q(Sock, ["GET", IRI]) of
      {ok, undefined} ->   
         uid(Sock, IRI, uid:g());
      {ok, Uid} ->
         {ok, uid:decode(Uid)}
   end.

uid(Sock, IRI, Uid) ->
   case 
      eredis:qp(Sock, [
         ["SET", IRI, uid:encode(Uid), "NX"],
         ["SET", uid:encode(Uid), IRI, "NX"]
      ]) 
   of
      % already exists
      [{ok, undefined} | _] ->
         uid(Sock, IRI);
      [{ok, _} | _] ->
         {ok, Uid}
   end.

%%
%%
put(Sock, Spock) ->
   #{s := Uid} = Fact = fact(Sock, Spock),
   eredis:qp(Sock, [
      ["ZADD", scalar:c(X), "0", relog_codec:encode_spo(X, Fact)] || X <- [spo, sop, pso, pos, ops, osp]
   ]).   

   %% use redis pipeline for hex index update
   %% use zadd with 0 see lex index
   %% http://redis.io/topics/indexes
   %% how to handle c - k ?

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




