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
-module(relog).

-compile({parse_transform, category}).
-include_lib("datum/include/datum.hrl").

-export([start/0]).
-export([
   socket/2
,  uid/2
,  iri/2
,  append/2
,  append/3
,  match/2
,  stream/2
,  q/2
,  jsonify/2
]).

%%
%% @todo
%%   - protected against IRI/UID overflow attack

-type sock() :: _.

%%
%%
start() ->
   application:ensure_all_started(?MODULE).


%%
%%
-spec socket(string(), integer()) -> {ok, sock()} | {error, _}.

socket(Host, Port) ->
   eredis:start_link(Host, Port).

%%
%% associate urn with unique identity or return existed one
-spec uid(sock(), semantic:iri()) -> datum:either( binary() ).

uid(Sock, IRI) ->
   relog_reader:uid(Sock, IRI).

%%
%% lookup urn or uid associated with unique identity
-spec iri(sock(), binary()) -> datum:either( semantic:iri() ).

iri(Sock, Uid) ->
   relog_reader:iri(Sock, Uid).

%%
%% append knowledge fact 
-spec append(sock(), semantic:spo()) -> datum:either( semantic:iri() ).
-spec append(sock(), semantic:spo(), timeout()) -> datum:either( semantic:iri() ).

append(Sock, Fact) ->
   append(Sock, Fact, 30000).

append(Sock, Fact, Timeout) ->
   relog_writer:append(Sock, Fact, Timeout).

%%
%% match statements
-spec match(sock(), semantic:spock()) -> datum:stream().

match(Sock, Pattern) ->
   relog_reader:match(Sock, Pattern).

%%
%% datalog stream generator
-spec stream(_, _) -> _.

stream(Keys, Head) ->
   relog_reader:stream(Keys, Head).


%%
%% execute query
-spec q(_, _) -> _. 

q(Lp, Sock) ->
   Lp(Sock).

%%
%% encodes deducted fact(s) to json format
jsonify(_, ?stream() = Stream) ->
   Stream;
jsonify([_ | Schema], #stream{} = Stream) ->
   stream:map(
      fun(Fact) ->
         maps:from_list([
            {Key, json_val(Val)} || 
               {Key, Val} <- lists:zip(Schema, Fact), Val /= ?None
         ])
      end,
      Stream
   ).

json_val({iri, Uri}) -> 
   Uri;
json_val({iri, Prefix, Suffix}) -> 
   <<Prefix/binary, $:, Suffix/binary>>;
json_val({Lat, Lng}) ->
   <<(scalar:s(Lat))/binary, $ , (scalar:s(Lng))/binary>>;
json_val(#{<<"type">> := _, <<"coordinates">> := _} = GeoJson) ->
   GeoJson;
json_val(#{<<"key">> := _, <<"count">> := _} = Bucket) ->
   Bucket;
json_val(#{} = Json) ->
   Json;
json_val({_, _, _} = T) -> 
   scalar:s(tempus:encode(T));
json_val(Value) when is_atom(Value) -> 
   scalar:s(Value);
json_val(Value) when is_float(Value) -> 
   Value;
json_val(Value) when is_integer(Value) -> 
   Value;
json_val(Value) when is_binary(Value) -> 
   Value;
json_val(Value) when is_list(Value) ->
   [json_val(X) || X <- Value];
json_val(Value) ->
   scalar:s(Value).
