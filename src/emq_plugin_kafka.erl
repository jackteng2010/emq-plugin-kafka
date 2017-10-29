%%--------------------------------------------------------------------
%% Copyright (c) 2013-2017 EMQ Enterprise, Inc. (http://emqtt.io)
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emq_plugin_kafka).

-include("emq_plugin_kafka.hrl").

-include_lib("emqttd/include/emqttd.hrl").

-export([load/1, unload/0]).

%% Hooks functions

-export([on_client_connected/3, on_client_disconnected/3]).

-export([on_session_created/3, on_session_terminated/4]).

-export([on_message_publish/2]).

%% Called when the plugin application start
load(Env) ->
	ekaf_init([Env]),
    emqttd:hook('client.connected', fun ?MODULE:on_client_connected/3, [Env]),
    emqttd:hook('client.disconnected', fun ?MODULE:on_client_disconnected/3, [Env]),
    emqttd:hook('session.created', fun ?MODULE:on_session_created/3, [Env]),
    emqttd:hook('session.terminated', fun ?MODULE:on_session_terminated/4, [Env]),
    emqttd:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]).

on_client_connected(ConnAck, Client = #mqtt_client{client_id  = ClientId, username = Username}, Env) ->
	lager:info("client(~s/~s) connected, connack: ~w.", [ClientId, Username, ConnAck]),
    Json = mochijson2:encode([{type, <<"connected">>},
								{clientid, ClientId},
								{username, Username},
								{ts, emqttd_time:now_secs()}]),
	produce_to_kafka(Json),
    {ok, Client}.

on_client_disconnected(Reason, _Client = #mqtt_client{client_id = ClientId, username = Username}, Env) ->
    lager:info("client(~s/~s) disconnected, reason: ~w.", [ClientId, Username, Reason]),
    Json = mochijson2:encode([{type, <<"disconnected">>},
								{clientid, ClientId},
								{username, Username},
							  	{reason, Reason},
								{ts, emqttd_time:now_secs()}]),
	produce_to_kafka(Json),
    ok.

on_session_created(ClientId, Username, _Env) ->
	lager:error("client(~s/~s) created session.", [ClientId, Username]),
    Json = mochijson2:encode([{type, <<"session_created">>},
								{clientid, ClientId},
								{username, Username},
								{ts, emqttd_time:now_secs()}]),
	produce_to_kafka(Json).

on_session_terminated(ClientId, Username, Reason, _Env) ->
	lager:info("client(~s/~s) terminated session, reason: ~w.", [ClientId, Username, Reason]),
    Json = mochijson2:encode([{type, <<"session_terminated">>},
								{clientid, ClientId},
								{username, Username},
							  	{reason, Reason},
								{ts, emqttd_time:now_secs()}]),
	produce_to_kafka(Json).
    
%% transform message and return
on_message_publish(Message = #mqtt_message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};

on_message_publish(Message = #mqtt_message{from = {ClientId, Username},
                        qos     = Qos,
                        retain  = Retain,
                        dup     = Dup,
                        topic   = Topic,
                        payload = Payload}, _Env) ->
    lager:info("client(~s/~s) publish message to topic: ~s.", [ClientId, Username, Topic]),
    Json = mochijson2:encode([{type, <<"publish">>},
								{clientid, ClientId},
								{username, Username},
							  	{qos, Qos},
							  	{retain, Retain},
							  	{dup, Dup},
							  	{topic, Topic},
							  	{payload, Payload},
								{ts, emqttd_time:now_secs()}]),
	produce_to_kafka(Json),
    {ok, Message}.

ekaf_init(_Env) ->
	{ok, KafkaValue} = application:get_env(emq_plugin_kafka, kafka),
	
	%%host port topic config from schema, the last line
	Host = proplists:get_value(host, KafkaValue),
	Port = proplists:get_value(port, KafkaValue),
	Topic = proplists:get_value(topic, KafkaValue),
	
	application:load(ekaf),
	application:set_env(ekaf, ekaf_bootstrap_broker, {Host, list_to_integer(Port)}),
	application:set_env(ekaf, ekaf_bootstrap_topics, Topic),
	{ok, _} = application:ensure_all_started(ekaf),
    io:format("Init ekaf server with ~s:~s, topic: ~s~n", [Host, Port, Topic]).
	
%% Called when the plugin application stop
unload() ->
    emqttd:unhook('client.connected', fun ?MODULE:on_client_connected/3),
    emqttd:unhook('client.disconnected', fun ?MODULE:on_client_disconnected/3),
    emqttd:unhook('session.created', fun ?MODULE:on_session_created/3),
    emqttd:unhook('session.terminated', fun ?MODULE:on_session_terminated/4),
    emqttd:unhook('message.publish', fun ?MODULE:on_message_publish/2).

%% Internal function, send message to kafka
produce_to_kafka(Json) ->
	{ok, KafkaValue} = application:get_env(emq_plugin_kafka, kafka),
	Topic = proplists:get_value(topic, KafkaValue),
	io:format("=====topic: ~s, json: ~s .~n", [Topic, Json]),
	try ekaf:produce_async(Topic, list_to_binary(Json)) of 
		_ -> io:format("send to kafka success. ~n")
    catch _:Error ->
        lager:error("can't send to kafka error: ~s", [Error])
    end.
