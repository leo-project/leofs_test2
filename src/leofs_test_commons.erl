%%======================================================================
%%
%% LeoFS
%%
%% Copyright (c) 2012-2015 Rakuten, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%%======================================================================
-module(leofs_test_commons).

-include("leofs_test.hrl").
-include("leo_redundant_manager.hrl").

-export([run/2]).

-define(ATTACH_NODE,  'storage_3@127.0.0.1').
-define(DETACH_NODE,  'storage_3@127.0.0.1').
-define(SUSPEND_NODE, 'storage_1@127.0.0.1').
-define(RESUME_NODE,  'storage_1@127.0.0.1').


%% @doc Execute tests
run(create_bucket, S3Conf) ->
    catch erlcloud_s3:create_bucket(?env_bucket(), S3Conf),
    ok;
run(put_objects, S3Conf) ->
    Keys = ?env_keys(),
    ok = put_object(S3Conf, Keys),
    ok;
run(del_objects, S3Conf) ->
    Keys = ?env_keys(),
    Keys_1 = case (Keys > 10000) of
                 true ->
                     round(Keys/100);
                 false ->
                     100
             end,
    ok = del_object(S3Conf, Keys_1, sets:new()),
    ok;
run(check_redundancies, S3Conf) ->
    Keys = ?env_keys(),
    ok = check_redundancies(S3Conf, Keys, []),
    ok;

%% Operate a node
run(attach_node,_S3Conf) ->
    ok = attach_node(?ATTACH_NODE),
    ok;
run(detach_node,_S3Conf) ->
    ok = detach_node(?DETACH_NODE),
    ok;
run(suspend_node,_S3Conf) ->
    ok = suspend_node(?SUSPEND_NODE),
    ok;
run(resume_node,_S3Conf) ->
    ok = resume_node(?RESUME_NODE),
    ok;
run(stop_and_restart,_S3Conf) ->
    ok = stop_and_restart(?SUSPEND_NODE),
    ok;
run(watch_mq,_S3Conf) ->
    ok = watch_mq(),
    ok;
run(_,_) ->
    ok.



%% @doc
%% @private
indicator(Index) ->
    indicator(Index, 100).
indicator(Index, Interval) ->
    case (Index rem Interval == 0) of
        true ->
            io:format("~s", ["-"]);
        false ->
            void
    end.


%% @doc
%% @private
gen_key(Index) ->
    lists:append(["test/", integer_to_list(Index)]).


%% @doc
%% @private
rnd_key(NumOfKeys) ->
    gen_key(
      erlang:phash2(
        erlang:crc32(crypto:rand_bytes(16)), NumOfKeys)).


%% @doc
%% @private
put_object(_Conf, 0) ->
    io:format("~s~n", ["<<"]),
    ok;
put_object(Conf, Index) ->
    indicator(Index),
    Key = gen_key(Index),
    Val = crypto:rand_bytes(16),
    erlcloud_s3:put_object(?env_bucket(), Key, Val, [], Conf),
    put_object(Conf, Index - 1).


%% @doc
del_object(_Conf, 0, Keys) ->
    io:format("~s~n", ["<<"]),
    Errors = lists:foldl(
               fun(K, Acc) ->
                       check_redundancies_1(?env_bucket() ++ "/" ++ K, Acc)
               end, [], sets:to_list(Keys)),
    case length(Errors) of
        0 ->
            ok;
        _ ->
            io:format("Error: ~p~n", [Errors]),
            erlang:error("del_object/3 - found inconsistent object")
    end;
del_object(Conf, Index, Keys) ->
    Key = rnd_key(?env_keys()),
    case sets:is_element(Key, Keys) of
        true ->
            del_object(Conf, Index, Keys);
        false ->
            Keys_1 = sets:add_element(Key, Keys),
            del_object_1(Conf, Index, Key, Keys_1)
    end.

del_object_1(Conf, Index, Key, Keys) ->
    indicator(Index, 1),
    case catch erlcloud_s3:delete_object(?env_bucket(), Key, Conf) of
        {'EXIT',_Cause} ->
            timer:sleep(timer:seconds(1)),
            del_object_1(Conf, Index, Key, Keys);
        _ ->
            del_object(Conf, Index - 1, Keys)
    end.


%% @doc
check_redundancies(_Conf, 0, Errors) ->
    io:format("~s~n", ["<<"]),
    case length(Errors) of
        0 ->
            void;
        _ ->
            lists:foreach(fun(E) ->
                                  io:format("~p~n", [E])
                          end, Errors)
    end,
    ok;
check_redundancies(Conf, Index, Errors) ->
    indicator(Index),
    Key = ?env_bucket() ++ "/" ++ gen_key(Index),
    Errors_1 = check_redundancies_1(Key, Errors),
    check_redundancies(Conf, Index - 1, Errors_1).

%% @private
check_redundancies_1(Key, Errors) ->
    case rpc:call(?env_manager(), 'leo_manager_api', 'whereis', [[Key], true]) of
        {ok, RetL} when length(RetL) == ?NUM_OF_REPLICAS ->
            L1 = lists:nth(1, RetL),
            L2 = lists:nth(2, RetL),
            case compare_1(Key, L1, L2) of
                ok ->
                    Errors;
                _ ->
                    [{Key, inconsistency}|Errors]
            end;
        {ok,_RetL} ->
            [{Key, inconsistency}|Errors];
        Other ->
            [{Key, Other}|Errors]
    end.


%% @private
compare_1(Key, L1, L2) ->
    %% node-name
    case (element(1, L1) /= element(1, L2)) of
        true ->
            compare_2(2, Key, L1, L2);
        false ->
            io:format("Error: ~p, ~p, ~p~n", [Key, L1, L2]),
            erlang:error("compare_1/2 - found invalid redundancies")
    end.

compare_2(9,_Key,_L1,_L2) ->
    ok;
compare_2(Index, Key, L1, L2) ->
    case (element(Index, L1) == element(Index, L2)) of
        true ->
            compare_2(Index + 1, Key, L1, L2);
        false ->
            io:format("Error: ~p, ~p, ~p~n", [Key, L1, L2]),
            erlang:error("compare_2/3 - found an inconsistent object")
    end.


%% ---------------------------------------------------------
%% Inner Function - Operat a node
%% ---------------------------------------------------------
attach_node(Node) ->
    case rpc:call(?env_manager(), leo_manager_mnesia,
                  get_storage_node_by_name, [Node]) of
        not_found ->
            Path = filename:join([?env_leofs_dir(), ?node_to_path(Node)]),
            os:cmd(Path ++ " start"),

            attach_node_1(Node, 0);
        _ ->
            io:format("ERROR: ~s ~w~n", ["Could not attach the node:", Node]),
            halt()
    end.

attach_node_1(Node, 3) ->
    io:format("ERROR: ~s ~w~n", ["Could not attach the node:", Node]),
    halt();
attach_node_1(Node, Times) ->
    case rpc:call(?env_manager(), leo_manager_mnesia,
                  get_storage_node_by_name, [Node]) of
        {ok, #node_state{state = ?STATE_ATTACHED}} ->
            ok;
        _ ->
            timer:sleep(timer:seconds(1)),
            attach_node_1(Node, Times + 1)
    end.


detach_node(Node) ->
    Manager = ?env_manager(),
    case rpc:call(Manager, leo_manager_api, detach, [Node]) of
        ok ->
            case rpc:call(Manager, leo_manager_api, rebalance, [null]) of
                ok ->
                    ok;
                _Error ->
                    io:format("ERROR: ~s~n", ["Fail rebalance (detach-node)"]),
                    halt()
            end;
        _Error ->
            io:format("ERROR: ~s ~w~n", ["Could not detach the node:", Node]),
            halt()
    end.

suspend_node(Node) ->
    case rpc:call(?env_manager(), leo_manager_api, suspend, [Node]) of
        ok ->
            ok;
        _Error ->
            io:format("ERROR: ~s ~w~n", ["Could not suspend the node:", Node]),
            halt()
    end.

resume_node(Node) ->
    case rpc:call(?env_manager(), leo_manager_api, resume, [Node]) of
        ok ->
            ok;
        _Error ->
            io:format("ERROR: ~s ~w~n", ["Could not resume the node:", Node]),
            halt()
    end.

stop_and_restart(Node) ->
    Path = filename:join([?env_leofs_dir(), ?node_to_path(Node)]),
    os:cmd(Path ++ " stop"),
    timer:sleep(timer:seconds(3)),
    os:cmd(Path ++ " start"),
    ok.

watch_mq() ->
    case rpc:call(?env_manager(),
                  leo_manager_mnesia, get_storage_nodes_all, []) of
        {ok, RetL} ->
            Nodes = [N || #node_state{node = N,
                                      state = ?STATE_RUNNING} <- RetL],
            watch_mq_1(Nodes);
        _ ->
            io:format("ERROR: ~s~n", ["Could not retrieve the running nodes"]),
            halt()
    end.

watch_mq_1([]) ->
    io:format("~s", ["|"]),
    timer:sleep(timer:seconds(5)),
    io:format("~s", ["<<"]),
    ok;
watch_mq_1([Node|Rest] = Nodes) ->
    io:format("~s", ["-"]),
    case rpc:call(?env_manager(),
                  leo_manager_api, mq_stats, [Node]) of
        {ok, RetL} ->
            case watch_mq_2(RetL) of
                ok ->
                    watch_mq_1(Rest);
                _ ->
                    timer:sleep(timer:seconds(5)),
                    watch_mq_1(Nodes)
            end;
        _ ->
            io:format("ERROR: ~s~n", ["Could not retrieve mq-state of the node"]),
            halt()
    end.

watch_mq_2([]) ->
    ok;
watch_mq_2([#mq_state{state = Stats}|Rest]) ->
    case (leo_misc:get_value('consumer_num_of_msgs', Stats, 0) == 0 andalso
          leo_misc:get_value('consumer_status', Stats) == 'idling') of
        true ->
            watch_mq_2(Rest);
        false ->
            still_running
    end.


%% % Retrieve list of objects from the LeoFS
%% Objs = erlcloud_s3:list_objects("erlang", Conf_1),
%% io:format("[debug]objects:~p~n", [Objs]),
%% % GET an object from the LeoFS
%% Obj = erlcloud_s3:get_object("erlang", "test-key", Conf_1),
%% io:format("[debug]inserted object:~p~n", [Obj]),
%% % GET an object metadata from the LeoFS
%% Meta = erlcloud_s3:get_object_metadata("erlang", "test-key", Conf_1),
%% io:format("[debug]metadata:~p~n", [Meta]),
%% % DELETE an object from the LeoFS
%% DeletedObj = erlcloud_s3:delete_object("erlang", "test-key", Conf_1),
%% io:format("[debug]deleted object:~p~n", [DeletedObj]),
