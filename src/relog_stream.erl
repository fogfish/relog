%% @doc
%%    
-module(relog_stream).

-export([
   new/3
]).

%%
%%
new(Sock, Key, Pattern) ->
   Len = byte_size(Pattern),
   stream:takewhile(
      fun(X) -> case X of <<Pattern:Len/binary, _/binary>> -> true; _ -> false end end,
      stream:unfold(fun unfold/1, 
         #{
            state => [],
            sock  => Sock,
            q     => ["ZRANGEBYLEX", Key, <<$[, Pattern/binary>>, "+"],
            from  => 0,
            size  => 10000 
         }
      )
   ).

unfold(#{state := [H|T]} = Seed) ->
   {H, Seed#{state := T}};

unfold(#{state := [], sock := Sock, from := From} = Seed) ->
   case eredis:q(Sock, q(Seed)) of
      {ok, []} ->
         Seed;

      {ok, Hits} ->
         unfold(Seed#{state => Hits, from => From + length(Hits)})
   end.

%%
%%
q(#{q := Query, from := From, size := Size}) ->
   Query ++ ["LIMIT", scalar:s(From), scalar:s(Size)].   
