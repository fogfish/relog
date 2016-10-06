%% @doc
%%   serialization protocol
-module(relog_codec).

-export([
   encode/2,
   decode/2,
   encode_iri/1,
   decode_iri/1,
   encode_spo/2,
   decode_spo/2,
   encode_term/1,
   decode_term/1
]).

%%
%% encode statement, replace urn with unique identity
encode(Sock, Spock) ->
   maps:map(
      fun
      (_, {iri, _} = IRI) ->
         {ok, Uid} = relog:uid(Sock, IRI), 
         Uid;
      (_, {iri, _, _} = IRI) ->
         {ok, Uid} = relog:uid(Sock, IRI), 
         Uid;
      (_, Any) ->
         Any
      end,
      Spock
   ).

decode(Sock, Spock) ->
   maps:map(
      fun
      (_, {uid, _, _, _} = Uid) ->
         {ok, IRI} = relog:iri(Sock, Uid),
         IRI;
      (_, Any) ->
         Any
      end,
      Spock
   ).

%%
%%
encode_iri({iri, Url}) ->
   Url;   
encode_iri({iri, Prefix, Suffix}) ->
   <<"urn:", Prefix/binary, $:, Suffix/binary>>.

decode_iri(<<"urn:", IRI/binary>>) ->
   [Prefix, Suffix] = binary:split(IRI, <<$:>>),
   {iri, Prefix, Suffix};

decode_iri(IRI) ->
   {iri, IRI}.

%%
%%
encode_spo(spo, #{s := S, p := P, o := O}) -> 
   [encode_term(S), 16#ff, encode_term(P), 16#ff, encode_term(O)];
encode_spo(sop, #{s := S, p := P, o := O}) -> 
   [encode_term(S), 16#ff, encode_term(O), 16#ff, encode_term(P)];
encode_spo(pso, #{s := S, p := P, o := O}) -> 
   [encode_term(P), 16#ff, encode_term(S), 16#ff, encode_term(O)];
encode_spo(pos, #{s := S, p := P, o := O}) -> 
   [encode_term(P), 16#ff, encode_term(O), 16#ff, encode_term(S)];
encode_spo(ops, #{s := S, p := P, o := O}) -> 
   [encode_term(O), 16#ff, encode_term(P), 16#ff, encode_term(S)];
encode_spo(osp, #{s := S, p := P, o := O}) -> 
   [encode_term(O), 16#ff, encode_term(S), 16#ff, encode_term(P)].

decode_spo(spo, Val) ->
   [S, P, O] = binary:split(Val, <<16#ff>>, [global]),
   #{s => decode_term(S), p => decode_term(P), o => decode_term(O)};
decode_spo(sop, Val) ->
   [S, O, P] = binary:split(Val, <<16#ff>>, [global]),
   #{s => decode_term(S), p => decode_term(P), o => decode_term(O)};
decode_spo(pso, Val) ->
   [P, S, O] = binary:split(Val, <<16#ff>>, [global]),
   #{s => decode_term(S), p => decode_term(P), o => decode_term(O)};
decode_spo(pos, Val) ->
   [P, O, S] = binary:split(Val, <<16#ff>>, [global]),
   #{s => decode_term(S), p => decode_term(P), o => decode_term(O)};
decode_spo(ops, Val) ->
   [O, P, S] = binary:split(Val, <<16#ff>>, [global]),
   #{s => decode_term(S), p => decode_term(P), o => decode_term(O)};
decode_spo(osp, Val) ->
   [O, S, P] = binary:split(Val, <<16#ff>>, [global]),
   #{s => decode_term(S), p => decode_term(P), o => decode_term(O)}.


%%
%%
encode_term({uid, _, _, _} = Uid) ->
   escape(<<0, (uid:encode(Uid))/binary>>);

encode_term(Any) ->
   <<131, Val/binary>> = erlang:term_to_binary(Any, [{minor_version, 1}]),
   escape(Val).


decode_term(<<0, Uid/binary>>) ->
   uid:decode( unescape(Uid) );

decode_term(Val) ->
   erlang:binary_to_term(<<131, (unescape(Val))/binary>>).


%%
%%
escape(Val) ->
   << <<(if X >= 16#f0, X =< 16#ff -> <<16#f0, (16#f0 bxor X)>>; true -> <<X>> end)/binary>> 
      || <<X:8>> <= Val >>.

unescape(Val) ->
   erlang:iolist_to_binary(unescape1(Val)).

unescape1(Val) ->
   case binary:split(Val, <<16#f0>>) of
      [Head, <<H:8, T/binary>>] ->
         [Head, (16#f0 bxor H) | unescape1(T)];
      List ->
         List
   end.
