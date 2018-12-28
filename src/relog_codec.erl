%% @doc
%%   serialization protocol
-module(relog_codec).
-include_lib("semantic/include/semantic.hrl").

-export([
   encode_type/1
,  decode_type/1
,  encode/3
,  encode_iri/1
,  decode/3
,  decode_iri/1

% ,  encode/2
   % decode/2,
   % decode_iri/1,
   % encode_spo/2,
   % decode_spo/2,
   % encode_term/1,
   % decode_term/1,

   % encode_term/2
]).

%%
%%
encode_type(?XSD_ANYURI) -> 0;
encode_type(?XSD_INTEGER) -> 2.

%%
%%
decode_type(0) -> ?XSD_ANYURI;
decode_type(2) -> ?XSD_INTEGER.


%%
%%
encode(_, _, '_') ->
   <<>>;

encode(_, _, Val)
 when is_list(Val) ->
   <<>>;

encode(Sock, ?XSD_ANYURI, {iri, _} = IRI) ->
   {ok, Uid} = relog_writer:uid(Sock, IRI),
   Uid;

encode(Sock, ?XSD_ANYURI, {iri, _, _} = IRI) ->
   {ok, Uid} = relog_writer:uid(Sock, IRI),
   Uid;

encode(Sock, ?XSD_ANYURI, IRI)
 when is_binary(IRI) ->
   {ok, Uid} = relog_writer:uid(Sock, {iri, IRI}),
   Uid;

encode(_, _, Val) ->
   escape(erlang:term_to_binary(Val, [{minor_version, 2}])).


%%
%%
encode_iri({iri, Prefix, Suffix}) ->
   <<Prefix/binary, $:, Suffix/binary>>;
encode_iri({iri, Urn}) ->
   Urn;
encode_iri(IRI)
 when is_binary(IRI) ->
   IRI.

%%
%%
decode(Sock, ?XSD_ANYURI, Uid) ->
   {ok, IRI} = relog:iri(Sock, Uid),
   IRI;

decode(_, _, Val) ->
   erlang:binary_to_term(unescape(Val)).


%%
%%
escape(Val) ->
   << <<(if X >= 16#f0, X =< 16#ff -> <<16#f0, (16#f0 bxor X)>>; true -> <<X>> end)/binary>> 
      || <<X:8>> <= Val >>.

%%
%% 
unescape(Val) ->
   erlang:iolist_to_binary(unescape1(Val)).

unescape1(Val) ->
   case binary:split(Val, <<16#f0>>) of
      [Head, <<H:8, T/binary>>] ->
         [Head, (16#f0 bxor H) | unescape1(T)];
      List ->
         List
   end.

%%
%%
decode_iri(<<Iri/binary>>) ->
   case semantic:compact(Iri) of
      undefined ->
         semantic:absolute(Iri);
      Semantic ->
         Semantic
   end;
decode_iri(Iri) ->
   semantic:absolute(Iri).






% %%
% %% encode statement, replace urn with unique identity
% encode(Sock, Spock) ->
%    maps:map(
%       fun
%       (_, {iri, _} = IRI) ->
%          {ok, Uid} = relog:uid(Sock, IRI), 
%          Uid;
%       (_, {iri, _, _} = IRI) ->
%          {ok, Uid} = relog:uid(Sock, IRI), 
%          Uid;
%       (_, Any) ->
%          Any
%       end,
%       Spock
%    ).

% decode(Sock, Spock) ->
%    maps:map(
%       fun
%       (_, {uid, _, _, _} = Uid) ->
%          {ok, IRI} = relog:iri(Sock, Uid),
%          IRI;
%       (_, Any) ->
%          Any
%       end,
%       Spock
%    ).



% %%
% %% @deprecated: encode - decode is redundant in context of new indexes
% encode_spo(spo, #{s := S, p := P, o := O}) -> 
%    [encode_term(S), 16#ff, encode_term(P), 16#ff, encode_term(O)];
% encode_spo(sop, #{s := S, p := P, o := O}) -> 
%    [encode_term(S), 16#ff, encode_term(O), 16#ff, encode_term(P)];
% encode_spo(pso, #{s := S, p := P, o := O}) -> 
%    [encode_term(P), 16#ff, encode_term(S), 16#ff, encode_term(O)];
% encode_spo(pos, #{s := S, p := P, o := O}) -> 
%    [encode_term(P), 16#ff, encode_term(O), 16#ff, encode_term(S)];
% encode_spo(ops, #{s := S, p := P, o := O}) -> 
%    [encode_term(O), 16#ff, encode_term(P), 16#ff, encode_term(S)];
% encode_spo(osp, #{s := S, p := P, o := O}) -> 
%    [encode_term(O), 16#ff, encode_term(S), 16#ff, encode_term(P)].

% decode_spo(spo, Val) ->
%    [S, P, O] = binary:split(Val, <<16#ff>>, [global]),
%    #{s => decode_term(S), p => decode_term(P), o => decode_term(O)};
% decode_spo(sop, Val) ->
%    [S, O, P] = binary:split(Val, <<16#ff>>, [global]),
%    #{s => decode_term(S), p => decode_term(P), o => decode_term(O)};
% decode_spo(pso, Val) ->
%    [P, S, O] = binary:split(Val, <<16#ff>>, [global]),
%    #{s => decode_term(S), p => decode_term(P), o => decode_term(O)};
% decode_spo(pos, Val) ->
%    [P, O, S] = binary:split(Val, <<16#ff>>, [global]),
%    #{s => decode_term(S), p => decode_term(P), o => decode_term(O)};
% decode_spo(ops, Val) ->
%    [O, P, S] = binary:split(Val, <<16#ff>>, [global]),
%    #{s => decode_term(S), p => decode_term(P), o => decode_term(O)};
% decode_spo(osp, Val) ->
%    [O, S, P] = binary:split(Val, <<16#ff>>, [global]),
%    #{s => decode_term(S), p => decode_term(P), o => decode_term(O)}.


% %%
% %%
% encode_term({uid, _, _} = Uid) ->
%    <<0, (uid:encode(Uid))/binary>>;
% encode_term({uid, _, _, _} = Uid) ->
%    % escape(<<0, (uid:encode(Uid))/binary>>);
%    <<0, (uid:encode(Uid))/binary>>;

% encode_term(Any) ->
%    <<131, Val/binary>> = erlang:term_to_binary(Any, [{minor_version, 1}]),
%    % escape(Val).
%    Val.

% encode_term(<<"binary">>, X) -> scalar:s(X);
% encode_term(<<"en">>, X) -> scalar:s(X);
% encode_term(<<"integer">>, X) -> <<X:32>>;
% encode_term(<<"datetime">>, {A, B, C}) -> <<A:20, B:20, C:24>>;
% encode_term(<<"rel">>, X) -> encode_term(X).


% decode_term(<<0, Uid/binary>>) ->
%    % uid:decode( unescape(Uid) );
%    uid:decode( Uid );

% decode_term(Val) ->
%    % erlang:binary_to_term(<<131, (unescape(Val))/binary>>).
%    erlang:binary_to_term(<<131, Val/binary>>).


% %% @todo: escape violate an order
% %%
% escape(Val) ->
%    << <<(if X >= 16#f0, X =< 16#ff -> <<16#f0, (16#f0 bxor X)>>; true -> <<X>> end)/binary>> 
%       || <<X:8>> <= Val >>.

% unescape(Val) ->
%    erlang:iolist_to_binary(unescape1(Val)).

% unescape1(Val) ->
%    case binary:split(Val, <<16#f0>>) of
%       [Head, <<H:8, T/binary>>] ->
%          [Head, (16#f0 bxor H) | unescape1(T)];
%       List ->
%          List
%    end.
