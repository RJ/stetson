%% This Source Code Form is subject to the terms of
%% the Mozilla Public License, v. 2.0.
%% A copy of the MPL can be found in the LICENSE file or
%% you can obtain it at http://mozilla.org/MPL/2.0/.
%%
%% @author Brendan Hay
%% @copyright (c) 2012 Brendan Hay <brendan@soundcloud.com>
%% @doc
%%

-module(stetson_server).

-behaviour(gen_server).

-include("include/stetson.hrl").

%% API
-export([start_link/2,
         cast/1]).

%% Callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-type message() :: {connect, pid(), inet:socket(), erlang:timestamp()} |
                   {establish, pid(), node()} |
                   {counter | gauge | timer, atom(), integer()} |
                   {counter | gauge | timer, atom(), integer(), float()}.

-export_type([message/0]).

-record(s, {sock               :: gen_udp:socket(),
            host = "localhost" :: string(),
            port = 8126        :: inet:port_number(),
            ns = ""            :: string()}).

%%
%% API
%%

-spec start_link(string(), string()) -> ignore | {error, _} | {ok, pid()}.
%% @doc Start the stats process
start_link(Uri, Ns) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, {Uri, Ns}, []).

-spec cast(message()) -> ok.
cast(Msg) -> gen_server:cast(?SERVER, Msg).

%%
%% Callbacks
%%

-spec init({string(), string()}) -> {ok, #s{}}.
%% @hidden
init({Uri, Ns}) ->
    random:seed(now()),
    {Host, Port} = split_uri(Uri, 8126),
    %% We save lots of calls into inet_gethost_native by looking this up once:
    case convert_to_ip_tuple(Host) of
        undefined -> 
            {stop, cant_resolve_statsd_host};
        IP -> 
            error_logger:info_msg("stetson using statsd at ~s:~B (resolved to: ~w)", [Host, Port, IP]),
            {ok, Sock} = gen_udp:open(0, [binary]),
            {ok, #s{sock = Sock, host = IP, port = Port, ns = Ns}}
    end.

-spec handle_call(message(), _, #s{}) -> {reply, ok, #s{}}.
%% @hidden
handle_call(_Msg, _From, State) -> {reply, ok, State}.

-spec handle_cast(message(), #s{}) -> {noreply, #s{}}.
%% @hidden Send counter/gauge/timer
handle_cast({Type, Bucket, N}, State) ->
    ok = stat(State, Type, Bucket, N),
    {noreply, State};
handle_cast({Type, Bucket, N, Rate}, State) ->
    ok = stat(State, Type, Bucket, N, Rate),
    {noreply, State}.

-spec handle_info(_Info, #s{}) -> {noreply, #s{}}.
%% @hidden
handle_info(_Info, State) -> {noreply, State}.

-spec terminate(_, #s{}) -> ok.
%% @hidden
terminate(_Reason, #s{sock = Sock}) ->
    ok = gen_udp:close(Sock),
    ok.

-spec code_change(_, #s{}, _) -> {ok, #s{}}.
%% @hidden
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%
%% Private
%%

-spec stat(#s{}, counter | gauge | timer, string() | atom(), integer(), float()) -> ok.
%% @private Create a statistic entry with a sample rate
stat(State, Type, Bucket, N, Rate) when Rate < 1.0 ->
    case {Type, random:uniform() =< Rate} of
        {counter, true} -> send(State, "~s:~p|c|@~p",  [Bucket, N, Rate]);
        {gauge, true}   -> send(State, "~s:~p|g|@~p",  [Bucket, N, Rate]);
        {timer, true}   -> send(State, "~s:~p|ms|@~p", [Bucket, N, Rate]);
        _               -> ok
    end.

-spec stat(#s{}, counter | gauge | timer, string() | atom(), integer()) -> ok.
%% @doc Create a statistic entry with no sample rate
stat(State, counter, Bucket, N) ->
    send(State, "~s:~p|c", [Bucket, N]);
stat(State, gauge, Bucket, N) ->
    send(State, "~s:~p|g", [Bucket, N]);
stat(State, timer, Bucket, N) ->
    send(State, "~s:~p|ms", [Bucket, N]).

-spec send(#s{}, string(), [atom() | non_neg_integer()]) -> ok.
%% @private Send the formatted binary packet over the udp socket,
%% prepending the ns/namespace
send(#s{sock = Sock, host = Host, port = Port, ns = Ns}, Format, Args) ->
    %% iolist_to_bin even though gen_...:send variants accept deep iolists,
    %% since it makes logging and testing easier
    Msg = iolist_to_binary(io_lib:format("~s." ++ Format, [Ns|Args])),
    case gen_udp:send(Sock, Host, Port, Msg) of
        _Any -> ok
    end.

-spec split_uri(string(), inet:port_number()) -> {nonempty_string(), inet:port_number()}.
%% @private
split_uri(Uri, Default) ->
    case string:tokens(Uri, ":") of
        [H|P] when length(P) > 0 -> {H, list_to_integer(lists:flatten(P))};
        [H|_]                    -> {H, Default}
    end.

%% They provided an erlang tuple already:
convert_to_ip_tuple({_,_,_,_} = IPv4)          -> IPv4;
convert_to_ip_tuple({_,_,_,_,_,_,_,_} = IPv6)  -> IPv6;
%% Maybe they provided an IP as a string, otherwise do a DNS lookup
convert_to_ip_tuple(Hostname)                  ->
    case inet_parse:address(Hostname) of
        {ok, IP}   -> IP;
        {error, _} -> dns_lookup(Hostname)
    end.

%% We need an option to bind the UDP socket to a v6 addr before it's worth
%% trying to lookup AAAA records for the statsd host. Just v4 for now:
dns_lookup(Hostname) -> resolve_hostname_by_family(Hostname, inet).

%%dns_lookup(Hostname) ->
%%    case resolve_hostname_by_family(Hostname, inet6) of
%%        undefined -> resolve_hostname_by_family(Hostname, inet);
%%        IP        -> IP
%%    end.

resolve_hostname_by_family(Hostname, Family) ->
    case inet:getaddrs(Hostname, Family) of
        {ok, L}   -> random_element(L);
        {error,_} -> undefined
    end.

random_element(L) when is_list(L) -> 
    lists:nth(random:uniform(length(L)), L).

