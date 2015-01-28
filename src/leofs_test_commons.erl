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
-define(RECOVER_NODE, 'storage_2@127.0.0.1').


%% @doc Execute tests
run(?F_CREATE_BUCKET, S3Conf) ->
    catch erlcloud_s3:create_bucket(?env_bucket(), S3Conf),
    ok;
run(?F_PUT_OBJ, S3Conf) ->
    Keys = ?env_keys(),
    ok = put_object(S3Conf, Keys),
    ok;
run(?F_DEL_OBJ, S3Conf) ->
    Keys = ?env_keys(),
    Keys_1 = case (Keys > 10000) of
                 true ->
                     round(Keys/100);
                 false ->
                     100
             end,
    ok = del_object(S3Conf, Keys_1, sets:new()),
    ok;
run(?F_CHECK_REPLICAS, S3Conf) ->
    Keys = ?env_keys(),
    ok = check_redundancies(S3Conf, Keys, []),
    ok;

%% Operate a node
run(?F_ATTACH_NODE,_S3Conf) ->
    ok = attach_node(?ATTACH_NODE),
    ok;
run(?F_DETACH_NODE,_S3Conf) ->
    ok = detach_node(?DETACH_NODE),
    ok;
run(?F_SUSPEND_NODE,_S3Conf) ->
    ok = suspend_node(?SUSPEND_NODE),
    ok;
run(?F_RESUME_NODE,_S3Conf) ->
    ok = resume_node(?RESUME_NODE),
    ok;
run(?F_START_NODE,_S3Conf) ->
    ok = start_node(?SUSPEND_NODE),
    ok;
run(?F_STOP_NODE,_S3Conf) ->
    ok = stop_node(?SUSPEND_NODE),
    ok;
run(?F_WATCH_MQ,_S3Conf) ->
    ok = watch_mq(),
    ok;
run(?F_COMPACTION,_S3Conf) ->
    ok = compaction(),
    ok;
run(?F_REMOVE_AVS,_S3Conf) ->
    ok = remove_avs(?RECOVER_NODE),
    ok;
run(?F_RECOVER_NODE,_S3Conf) ->
    ok = recover_node(?RECOVER_NODE),
    ok;
run(_F,_) ->
    ok.


%% @doc
%% @private
indicator(Index) ->
    indicator(Index, 100).
indicator(Index, Interval) ->
    case (Index rem Interval == 0) of
        true ->
            ?msg_progress_ongoing();
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
    ?msg_progress_finished(),
    ok;
put_object(Conf, Index) ->
    indicator(Index),
    Key = gen_key(Index),
    Val = crypto:rand_bytes(16),
    erlcloud_s3:put_object(?env_bucket(), Key, Val, [], Conf),
    put_object(Conf, Index - 1).


%% @doc
del_object(_Conf, 0, Keys) ->
    ?msg_progress_finished(),
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
    ?msg_progress_finished(),
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
    case rpc:call(?env_manager(), leo_manager_api, whereis, [[Key], true]) of
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
%% @doc Attach the node
attach_node(Node) ->
    case rpc:call(?env_manager(), leo_manager_mnesia,
                  get_storage_node_by_name, [Node]) of
        not_found ->
            ok = start_node(Node),
            attach_node_1(Node, 0);
        _ ->
            ?msg_error(["Could not attach the node:", Node]),
            halt()
    end.

attach_node_1(Node, ?THRESHOLD_ERROR_TIMES) ->
    ?msg_error(["Could not attach the node:", Node]),
    halt();
attach_node_1(Node, Times) ->
    case rpc:call(?env_manager(), leo_manager_mnesia,
                  get_storage_node_by_name, [Node]) of
        {ok, #node_state{state = ?STATE_ATTACHED}} ->
            rebalance();
        _ ->
            timer:sleep(timer:seconds(5)),
            attach_node_1(Node, Times + 1)
    end.


%% @doc Detach the node
detach_node(Node) ->
    case rpc:call(?env_manager(), leo_manager_api, detach, [Node]) of
        ok ->
            ok = stop_node(Node),
            rebalance();
        _Error ->
            ?msg_error(["Could not detach the node:", Node]),
            halt()
    end.


%% @doc Execute rebalance of data
rebalance() ->
    case rpc:call(?env_manager(), leo_manager_api, rebalance, [null]) of
        ok ->
            ok;
        _Error ->
            ?msg_error("Fail rebalance (detach-node)"),
            halt()
    end.


%% @doc Suspend the node
suspend_node(Node) ->
    case rpc:call(?env_manager(), leo_manager_api, suspend, [Node]) of
        ok ->
            ok;
        _Error ->
            ?msg_error(["Could not suspend the node:", Node]),
            halt()
    end.


%% @doc Resume the node
resume_node(Node) ->
    Manager = ?env_manager(),
    case catch leo_misc:node_existence(Node) of
        true ->
            case rpc:call(Manager, leo_manager_api, resume, [Node]) of
                ok ->
                    resume_node_1(Node);
                _Error ->
                    ?msg_error(["Could not resume the node:", Node]),
                    halt()
            end;
        _ ->
            timer:sleep(timer:seconds(5)),
            resume_node(Node)
    end.

resume_node_1(Node) ->
    case check_state_of_node(Node, ?STATE_RUNNING) of
        ok ->
            ok;
        {error, not_yet} ->
            timer:sleep(timer:seconds(3)),
            resume_node_1(Node);
        _ ->
            ?msg_error(["Could not resume the node:", Node]),
            halt()
    end.


%% @doc check state of the node
check_state_of_node(Node, State) ->
    case rpc:call(?env_manager(), leo_manager_mnesia,
                  get_storage_node_by_name, [Node]) of
        {ok, #node_state{state = State}} ->
            ok;
        {ok, _NodeState} ->
            {error, not_yet};
        Other ->
            Other
    end.


%% @doc Stop the node
start_node(Node) ->
    Path = filename:join([?env_leofs_dir(), ?node_to_path(Node)]),
    os:cmd(Path ++ " start"),
    ok.


%% @doc Stop the node
stop_node(Node) ->
    Path = filename:join([?env_leofs_dir(), ?node_to_path(Node)]),
    os:cmd(Path ++ " stop"),
    ok.


%% @doc Retrieve storage nodes
get_storage_nodes() ->
    case rpc:call(?env_manager(),
                  leo_manager_mnesia, get_storage_nodes_all, []) of
        {ok, RetL} ->
            [N || #node_state{node = N,
                              state = ?STATE_RUNNING} <- RetL];
        _ ->
            ?msg_error("Could not retrieve the running nodes"),
            halt()
    end.


%% @doc Watch mq-stats
watch_mq() ->
    Nodes = get_storage_nodes(),
    watch_mq_1(Nodes).

watch_mq_1([]) ->
    timer:sleep(timer:seconds(5)),
    ?msg_progress_finished(),
    ok;
watch_mq_1([Node|Rest] = Nodes) ->
    ?msg_progress_ongoing(),
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
            ?msg_error("Could not retrieve mq-state of the node"),
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


%% @doc Execute data-compcation
compaction() ->
    Nodes = get_storage_nodes(),
    compaction_1(Nodes).

%% @private
compaction_1([]) ->
    ?msg_progress_finished(),
    ok;
compaction_1([Node|Rest]) ->
    case rpc:call(?env_manager(), leo_manager_api, compact, ["start", Node, 'all', 3]) of
        ok ->
            ok = compaction_2(Node),
            compaction_1(Rest);
        _Other ->
            ?msg_error("Could not execute data-compaction"),
            halt()
    end.

%% @private
compaction_2(Node) ->
    ?msg_progress_ongoing(),
    case rpc:call(?env_manager(), leo_manager_api, compact, ["status", Node]) of
        {ok, #compaction_stats{status = ?ST_IDLING}} ->
            ok;
        {ok, #compaction_stats{}} ->
            timer:sleep(timer:seconds(3)),
            compaction_2(Node);
        _ ->
            ?msg_error("data-compaction failure"),
            halt()
    end.


%% @doc Remove avs of the node
remove_avs(Node) ->
    %% Stop the node
    ok = stop_node(Node),

    %% Remove avs of the node
    timer:sleep(timer:seconds(3)),
    Path = filename:join([?env_leofs_dir(), ?node_to_avs_dir(Node)]),
    case filelib:is_dir(Path) of
        true ->
            case (string:str(Path, "avs") > 0) of
                true ->
                    [] = os:cmd("rm -rf " ++ Path),
                    timer:sleep(timer:seconds(3)),

                    %% Start the node
                    start_node(Node);
                false ->
                    ok
            end;
        false ->
            ok
    end.


%% @doc Recover data of the node
recover_node(Node) ->
    Ret = case recover_node_1(Node, 0) of
              ok ->
                  case rpc:call(?env_manager(), leo_manager_api,
                                recover, ["node", Node, true]) of
                      ok ->
                          ok;
                      Error ->
                          Error
                  end;
              Error ->
                  Error
          end,

    case Ret of
        ok ->
            ok;
        _ ->
            ?msg_error(["recover-node failure", Node]),
            halt()
    end.

%% @private
recover_node_1(_Node, ?THRESHOLD_ERROR_TIMES) ->
    {error, over_threshold_error_times};
recover_node_1(Node, Times) ->
    case check_state_of_node(Node, ?STATE_RUNNING) of
        ok ->
            ok;
        {error, not_yet} ->
            timer:sleep(timer:seconds(10)),
            recover_node_1(Node, Times + 1);
        Other ->
            Other
    end.
