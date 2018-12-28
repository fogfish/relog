-module(relog).
-compile({parse_transform, category}).


-export([start/0]).
-export([
   uid/2
,  iri/2
,  append/2
,  append/3
,  match/2
,  stream/2

,  socket/2,
   put/2,
   q/2
]).


-type sock() :: _.


start() ->
   applib:boot(?MODULE, []),
   clue:define(meter, put, 10000),
   clue:define(counter, n),
   clue:define(counter, size).

%%
%% associate urn with unique identity or return existed one
-spec uid(sock(), semantic:iri()) -> datum:either( uid:l() ).

uid(Sock, IRI) ->
   [either ||
      relog_writer:uid(Sock, IRI),
      uid:decode(_)
   ].

%%
%% lookup urn or uid associated with unique identity
-spec iri(sock(), uid:l()) -> datum:either( semantic:iri() ).

iri(Sock, Uid) ->
   [either ||
      relog_reader:iri(Sock, Uid),
      cats:unit( relog_codec:decode_iri(_) )
   ].

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
%%
-spec socket(string(), integer()) -> {ok, sock()} | {error, _}.

socket(Host, Port) ->
   eredis:start_link(Host, Port).



%%
%% put type-safe knowledge statement to storage
-spec put(sock(), semantic:spock()) -> {ok, uid:l()}.

put(Sock, Spock) ->
   relog_writer:put(Sock, Spock).


%%
%% execute query
q(Expr, Sock) ->
   stream:map(fun(X) -> relog_codec:decode(Sock, X) end, datalog:q(Expr, Sock)).   
