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
            size  => 1000 
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
