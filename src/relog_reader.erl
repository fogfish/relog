%%
%%
-module(relog_reader).

-export([
   iri/2,
   sigma/1
]).

%%
%%
iri(Sock, {uid, _, _, _} = Uid) ->
   case eredis:q(Sock, ["GET", uid:encode(Uid)]) of
      {ok, undefined} ->   
         {ok, Uid};
      {ok, IRI} ->
         {ok, relog_codec:decode_iri(IRI)}
   end.


%%
%%
sigma(Expr) ->
   fun(X) ->
      fun(Stream) ->
         stream(X, stream:head(Stream), Expr)
      end
   end.

stream(Sock, Heap, #{'_' := [S, P, O]} = Expr) ->
   stream:map(
      fun(Fact) -> unit(S, P, O, Fact, Heap) end,
      relog:match(Sock, pattern(S, P, O, datalog:bind(Heap, Expr)))
   ).

pattern(S, P, O, Heap) ->
   match(o, O, Heap, match(p, P, Heap, match(s, S, Heap, #{}))).

match(Pkey, Hkey, Heap, Pattern)
 when is_atom(Hkey) ->
   case maps:get(Hkey, Heap, undefined) of
      undefined ->
         Pattern;
      Val ->
         maps:put(Pkey, Val, Pattern)
   end;

match(Pkey, Val, _, Pattern) ->
   maps:put(Pkey, Val, Pattern).


unit(S, P, O, #{s := Sx, p := Px, o := Ox}, Heap) ->
   unit(S, Sx, unit(P, Px, unit(O, Ox, Heap))).

unit(Key, Val, Heap) 
 when is_atom(Key) ->
   maps:put(Key, Val, Heap);

unit(_, _, Heap) ->
   Heap.