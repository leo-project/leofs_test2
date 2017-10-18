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
-include_lib("eunit/include/eunit.hrl").

-export([run/2]).

%% @doc Execute tests
run(?F_CREATE_BUCKET, S3Conf) ->
    catch erlcloud_s3:create_bucket(?env_bucket(), S3Conf),
    ok;
run(?F_PUT_OBJ, S3Conf) ->
    Keys = ?env_keys(),
    ok = put_object(S3Conf, Keys),
    ok;
run(?F_PUT_ZERO_BYTE_OBJ, S3Conf) ->
    Keys = ?env_keys(),
    ok = put_zero_byte_object(S3Conf, Keys),
    ok;
run(?F_GET_OBJ, S3Conf) ->
    Keys = ?env_keys(),
    ok = get_object(S3Conf, Keys, 200),
    ok;
run(?F_GET_OBJ_NOT_FOUND, S3Conf) ->
    Keys = gen_key_by_one_percent(?env_keys()),
    ok = get_object(S3Conf, Keys, 404),
    ok;
run(?F_DEL_OBJ, S3Conf) ->
    Keys = gen_key_by_one_percent(?env_keys()),
    ok = del_object(S3Conf, Keys, sets:new()),
    ok;
run(?F_CHECK_REPLICAS, S3Conf) ->
    Keys = ?env_keys(),
    ok = check_redundancies(S3Conf, Keys),
    ok;

%% Operate a node
run(?F_ATTACH_NODE,_S3Conf) ->
    control_cluster(attach),
    ok;
run(?F_TAKEOVER,_S3Conf) ->
    control_cluster(takeover),
    ok;
run(?F_DETACH_NODE,_S3Conf) ->
    control_cluster(detach),
    ok;
run(?F_SUSPEND_NODE,_S3Conf) ->
    control_cluster(suspend),
    ok;
run(?F_RESUME_NODE,_S3Conf) ->
    control_cluster(resume),
    ok;
run(?F_START_NODE,_S3Conf) ->
    control_cluster(start),
    ok;
run(?F_STOP_NODE,_S3Conf) ->
    control_cluster(stop),
    ok;
run(?F_WATCH_MQ,_S3Conf) ->
    ok = watch_mq(),
    ok;
run(?F_DIAGNOSIS,_S3Conf) ->
    ok = diagnosis(),
    ok;
run(?F_COMPACTION,_S3Conf) ->
    ok = compaction(),
    ok;
run(?F_REMOVE_AVS,_S3Conf) ->
    control_cluster(remove_avs),
    ok;
run(?F_RECOVER_NODE,_S3Conf) ->
    control_cluster(recover_node),
    ok;
run(_F,_) ->
    ok.

%% ---------------------------------------------------------
%% Inner Functions
%% ---------------------------------------------------------
%% @doc Generate keys
%% @private
-spec(gen_key_by_one_percent(Keys) ->
             pos_integer() when Keys::pos_integer()).
gen_key_by_one_percent(Keys) ->
    case (Keys > 10000) of
        true ->
            round(Keys/100);
        false ->
            100
    end.

%% @doc Generate a key by index
%% @private
gen_key(Index) ->
    lists:append(["test/", integer_to_list(Index)]).


%% @doc Output progress
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


%% @doc Generate a key at random
%% @private
rnd_key(NumOfKeys) ->
    gen_key(
      erlang:phash2(
        erlang:crc32(crypto:rand_bytes(16)), NumOfKeys)).


%% @doc Make partion of processing units
%% private
partitions(Index, MaxKeys, Acc) ->
    Start = (Index - 1) * ?UNIT_OF_PARTION + 1,
    case Start > MaxKeys of
        true ->
            Acc;
        false ->
            End = (Index - 1) * ?UNIT_OF_PARTION + ?UNIT_OF_PARTION,
            case (End > MaxKeys) of
                true  ->
                    [{Start, MaxKeys}|Acc];
                false ->
                    partitions(Index + 1, MaxKeys, [{Start, End}|Acc])
            end
    end.


%% @doc Execute function in paralell
%% @private
do_concurrent_exec(Conf, Keys, Fun) ->
    Partitions = partitions(1, Keys, []),
    From = self(),
    Ref  = make_ref(),

    lists:foreach(
      fun({Start, End}) ->
              spawn(fun() ->
                            Fun(Conf, From, Ref, Start, End)
                    end)
      end, Partitions),
    loop(Ref, length(Partitions)).


%% @doc Receive messages from clients
%% @private
loop(_,0) ->
    ?msg_progress_finished(),
    ok;
loop(Ref, NumOfPartitions) ->
    receive
        {Ref, ok} ->
            loop(Ref, NumOfPartitions - 1);
        _ ->
            loop(Ref, NumOfPartitions)
    after
        ?DEF_TIMEOUT ->
            {error, timeout}
    end.


%% @doc Put objects
%% @private
put_object(Conf, Keys) when Keys > ?UNIT_OF_PARTION ->
    do_concurrent_exec(Conf, Keys, fun put_object_1/5);
put_object(Conf, Keys) ->
    put_object_1(Conf, undefined, undefined, 1, Keys).


put_zero_byte_object(_,0) ->
    ?msg_progress_finished(),
    ok;
put_zero_byte_object(Conf, Keys) ->
    Key = gen_key(Keys),
    indicator(Keys),

    case catch erlcloud_s3:put_object(?env_bucket(), Key, <<>>, [], Conf) of
        {'EXIT',_Cause} ->
            timer:sleep(timer:seconds(1)),
            put_zero_byte_object(Conf, Keys);
        _ ->
            put_zero_byte_object(Conf, Keys - 1)
    end.


%% @private
put_object_1(_, From, Ref, Start, End) when Start > End ->
    case (From == undefined andalso
          Ref  == undefined) of
        true ->
            ?msg_progress_finished(),
            ok;
        false ->
            erlang:send(From, {Ref, ok})
    end;
put_object_1(Conf, From, Ref, Start, End) ->
    indicator(Start),
    Key = gen_key(Start),
    Val = crypto:rand_bytes(16),
    case catch erlcloud_s3:put_object(?env_bucket(), Key, Val, [], Conf) of
        {'EXIT',_Cause} ->
            timer:sleep(timer:seconds(1)),
            put_object_1(Conf, From, Ref, Start, End);
        _ ->
            put_object_1(Conf, From, Ref, Start + 1, End)
    end.


%% @doc Retrieve objects
%% @private
get_object(_Conf, 0,_ExpectedCode) ->
    ?msg_progress_finished(),
    ok;
get_object(Conf, Keys, 200) when Keys > ?UNIT_OF_PARTION ->
    do_concurrent_exec(Conf, Keys, fun get_object_1/5);
get_object(Conf, Keys, 200) ->
    get_object_1(Conf, undefined, undefined, 1, Keys);
get_object(Conf, Index, ExpectedCode) ->
    indicator(Index, 1),
    Key = rnd_key(?env_keys()),

    case catch erlcloud_s3:get_object(?env_bucket(), Key, Conf) of
        {'EXIT', Cause} ->
            El = element(1, Cause),
            case El of
                {aws_error,{http_error,404,_,_}} when ExpectedCode == 404 ->
                    get_object(Conf, Index - 1, ExpectedCode);
                _ ->
                    erlang:error(Cause)
            end;
        _Ret ->
            get_object(Conf, Index - 1, ExpectedCode)
    end.

%% @private
get_object_1(_, From, Ref, Start, End) when Start > End ->
    case (From == undefined andalso
          Ref  == undefined) of
        true ->
            ?msg_progress_finished(),
            ok;
        false ->
            erlang:send(From, {Ref, ok})
    end;
get_object_1(Conf, From, Ref, Start, End) ->
    indicator(Start),
    Key = gen_key(Start),

    case catch erlcloud_s3:get_object(?env_bucket(), Key, Conf) of
        {'EXIT', Cause} ->
            erlang:error(Cause);
        _ ->
            get_object_1(Conf, From, Ref, Start + 1, End)
    end.


%% @doc Remove objects
%% @private
del_object(_Conf, 0, Keys) ->
    ?msg_progress_finished(),
    lists:foreach(fun(K) ->
                          check_redundancies_2(?env_bucket() ++ "/" ++ K)
                  end, sets:to_list(Keys)),
    ok;
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


%% @doc Check replicated objects
%% @private
check_redundancies(Conf, Keys) when Keys > ?UNIT_OF_PARTION ->
    do_concurrent_exec(Conf, Keys, fun check_redundancies_1/5);
check_redundancies(Conf, Keys) ->
    check_redundancies_1(Conf, undefined, undefined, 1, Keys).

%% @private
check_redundancies_1(_, From, Ref, Start, End) when Start > End ->
    case (From == undefined andalso
          Ref  == undefined) of
        true ->
            ?msg_progress_finished(),
            ok;
        false ->
            erlang:send(From, {Ref, ok})
    end;
check_redundancies_1(Conf, From, Ref, Start, End) ->
    indicator(Start),
    Key = ?env_bucket() ++ "/" ++ gen_key(Start),
    ok = check_redundancies_2(Key),
    check_redundancies_1(Conf, From, Ref, Start + 1, End).

%% @private
check_redundancies_2(Key) ->
    Replicas = application:get_env(?APP, ?PROP_REPLICAS, ?NUM_OF_REPLICAS),
    case rpc:call(?env_manager(), leo_manager_api, whereis, [[Key], true]) of
        {ok, RetL} when length(RetL) == Replicas ->
            case Replicas > 1 of
                true ->
                    L1 = lists:nth(1, RetL),
                    L2 = lists:nth(2, RetL),
                    ok = compare_1(Key, L1, L2);
                false ->
                    ok
            end;
        {ok,_RetL} ->
            io:format("[ERROR] ~s, ~w~n", [Key, inconsistent_object]),
            halt(1);
        Other ->
            io:format("[ERROR] ~s, ~p~n", [Key, Other]),
            halt(1)
    end.


%% @private
compare_1(Key, L1, L2) ->
    %% node-name
    case (element(1, L1) /= element(1, L2)) of
        true ->
            ok;
        false ->
            io:format("[ERROR] ~s, ~p, ~p~n", [Key, L1, L2]),
            halt(1)
    end,
    compare_2(2, Key, L1, L2).

compare_2(Index, Key, L1, L2) when is_list(element(2, L1)) andalso
                                   is_list(element(2, L2)) ->
    compare_3(Index, Key, element(2, L1), element(2, L2));
compare_2(Index, Key, L1, L2) ->
    compare_3(Index, Key, L1, L2).

compare_3(9,_Key,_L1,_L2) ->
    ok;
compare_3(Index, Key, L1, L2) when is_list(L1) andalso
                                   is_list(L2) ->
    case (length(L1) >= Index - 1 andalso
          length(L2) >= Index - 1) of
        true ->
            case lists:nth(Index - 1, L1) == lists:nth(Index - 1, L2) of
                true ->
                    ok;
                false ->
                    io:format("[ERROR] ~s, ~p, ~p~n", [Key, L1, L2]),
                    halt(1)
            end;
        false ->
            io:format("[ERROR] ~s, ~p, ~p~n", [Key, L1, L2]),
            halt(1)
    end,
    compare_3(Index + 1, Key, L1, L2);
compare_3(Index, Key, L1, L2) ->
    case (erlang:size(L1) >= Index andalso
          erlang:size(L2) >= Index) of
        true ->
            case (element(Index, L1) == element(Index, L2)) of
                true ->
                    ok;
                false ->
                    io:format("[ERROR] ~s, ~p, ~p~n", [Key, L1, L2]),
                    halt(1)
            end;
        false ->
            io:format("[ERROR] ~s, ~p, ~p~n", [Key, L1, L2]),
            halt(1)
    end,
    compare_3(Index + 1, Key, L1, L2).


%% @doc Retrieve storage nodes
%% @private
get_storage_nodes() ->
    case rpc:call(?env_manager(),
                  leo_redundant_manager_api, get_members, []) of
        {ok, RetL} ->
            [N || #member{node = N,
                          state = ?STATE_RUNNING} <- RetL];
        _ ->
            ?msg_error("Could not retrieve the running nodes"),
            halt(1)
    end.


%% @doc Watch mq-stats
%% @private
watch_mq() ->
    Nodes = get_storage_nodes(),
    watch_mq_1(Nodes).

watch_mq_1([]) ->
    io:format("~n"),
    timer:sleep(timer:seconds(5)),
    ok;
watch_mq_1([Node|Rest]) ->
    io:format("~n            * node:~p", [Node]),
    case rpc:call(?env_manager(),
                  leo_manager_api, mq_stats, [Node]) of
        {ok, RetL} ->
            case watch_mq_2(RetL) of
                ok ->
                    watch_mq_1(Rest);
                _ ->
                    timer:sleep(timer:seconds(5)),
                    watch_mq()
            end;
        _ ->
            ?msg_error("Could not retrieve mq-state of the node"),
            halt(1)
    end.

watch_mq_2([]) ->
    ok;
watch_mq_2([#mq_state{id = Id,
                      state = Stats}|Rest]) ->
    NumOfMsgs = leo_misc:get_value('consumer_num_of_msgs', Stats, 0),
    io:format(", ~p:~p", [Id, NumOfMsgs]),
    case (NumOfMsgs == 0) of
        true ->
            watch_mq_2(Rest);
        false ->
            still_running
    end.


%% @doc Execute data-compcation
%% @private
diagnosis() ->
    Nodes = get_storage_nodes(),
    diagnosis_1(Nodes).

%% @private
diagnosis_1([]) ->
    ?msg_progress_finished(),
    ok;
diagnosis_1([Node|Rest]) ->
    case rpc:call(?env_manager(), leo_manager_api, diagnose_data, [Node]) of
        ok ->
            ok = compaction_2(Node),
            diagnosis_1(Rest);
        _Other ->
            ?msg_error("Could not execute data-diagnosis"),
            halt(1)
    end.


%% @doc Execute data-compcation
%% @private
compaction() ->
    Nodes = get_storage_nodes(),
    compaction_1(Nodes).

%% @private
compaction_1([]) ->
    ?msg_progress_finished(),
    ok;
compaction_1([Node|Rest]) ->
    case rpc:call(?env_manager(), leo_manager_api, compact, ["start", Node, 'all', 1]) of
        ok ->
            ok = compaction_2(Node),
            compaction_1(Rest);
        _Other ->
            ?msg_error("Could not execute data-compaction"),
            halt(1)
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
            ?msg_error("data-compaction/data-diagnosis failure"),
            halt(1)
    end.

%% @doc Control the cluster via ansible
%% @private
control_cluster_via_ansible(Inventory, PlayBook) ->
    {ok, WorkDir} = file:get_cwd(),
    AnsibleDir = filename:join([WorkDir, "leofs_ansible"]),
    InventoryPath = filename:join([AnsibleDir, Inventory]),
    PlayBookPath = filename:join([AnsibleDir, PlayBook]),
    User = application:get_env(?APP, ?PROP_ANSIBLE_USER, ?ANSIBLE_USER),
    PK = application:get_env(?APP, ?PROP_ANSIBLE_PK, ?ANSIBLE_PK),
    Cmd = lists:flatten(io_lib:format("ansible-playbook -i ~s -u ~s --private-key ~s ~s", [InventoryPath,  User, PK, PlayBookPath])),
    % @debug
    io:format(user, "[debug] Cmd: ~s~n", [Cmd]),
    Ret = os:cmd(Cmd),
    io:format(user, "[debug] Result: ~s~n", [Ret]),
    ok.

control_cluster(detach) ->
    control_cluster_via_ansible("detach_attach_node.hosts", "detach_node.yml");
control_cluster(attach) ->
    control_cluster_via_ansible("detach_attach_node.hosts", "attach_node.yml");
control_cluster(remove_avs) ->
    control_cluster_via_ansible("remove_avs.hosts", "remove_avs.yml");
control_cluster(stop) ->
    control_cluster_via_ansible("start_stop_node.hosts", "stop_node.yml");
control_cluster(start) ->
    control_cluster_via_ansible("start_stop_node.hosts", "start_node.yml");
control_cluster(takeover) ->
    control_cluster_via_ansible("takeover_node.hosts", "takeover_node.yml");
control_cluster(suspend) ->
    control_cluster_via_ansible("start_stop_node.hosts", "suspend_node.yml");
control_cluster(resume) ->
    control_cluster_via_ansible("start_stop_node.hosts", "resume_node.yml");
control_cluster(recover_node) ->
    control_cluster_via_ansible("remove_avs.hosts", "recover_node.yml");
control_cluster(UnknownOps) ->
    erlang:error("invalid a cluster operation: " ++ UnknownOps).
