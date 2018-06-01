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

-define(ATTACH_NODE,   'storage_3@127.0.0.1').
-define(DETACH_NODE,   'storage_3@127.0.0.1').
-define(DETACH_NODE_1, 'storage_0@127.0.0.1').
-define(SUSPEND_NODE,  'storage_1@127.0.0.1').
-define(RESUME_NODE,   'storage_1@127.0.0.1').
-define(RECOVER_NODE,  'storage_2@127.0.0.1').
-define(TAKEOVER_NODE, 'storage_4@127.0.0.1').


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
    ok = attach_node(?ATTACH_NODE),
    ok;
run(?F_TAKEOVER,_S3Conf) ->
    DetachedNode = ?DETACH_NODE_1,
    case rpc:call(?env_manager(), leo_manager_api,
                  detach, [DetachedNode]) of
        ok ->
            ok = stop_node(DetachedNode),

            Node = ?TAKEOVER_NODE,
            ok = start_node(Node),
            timer:sleep(timer:seconds(10)),
            ok = attach_node_1(Node, 0),
            ok;
        _Error ->
            ?msg_error(["Could not detach the node:", DetachedNode]),
            halt()
    end,
    ok;
run(?F_DETACH_NODE,_S3Conf) ->
    ok = detach_node(?DETACH_NODE),
    ok;
run(?F_SUSPEND_NODE,_S3Conf) ->
    ok = suspend_node(?SUSPEND_NODE),
    timer:sleep(timer:seconds(15)),
    ok;
run(?F_RESUME_NODE,_S3Conf) ->
    ok = resume_node(?RESUME_NODE),
    ok;
run(?F_START_NODE,_S3Conf) ->
    ok = start_node(?SUSPEND_NODE),
    ok;
run(?F_STOP_NODE,_S3Conf) ->
    ok = stop_node(?SUSPEND_NODE),
    timer:sleep(timer:seconds(15)),
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
    ok = remove_avs(?RECOVER_NODE),
    ok;
run(?F_RECOVER_NODE,_S3Conf) ->
    ok = recover_node(?RECOVER_NODE),
    ok;
%% MP(multipart upload) related
run(?F_MP_UPLOAD_NORMAL, S3Conf) ->
    Bucket = ?env_bucket(),
    Key = gen_key_for_multipart("normal"),
    PartBin = crypto:strong_rand_bytes(10 * 1024 * 1024),
    try
        {ok, PropList} = erlcloud_s3:start_multipart(Bucket, Key, [], [], S3Conf),
        UploadId = proplists:get_value(uploadId, PropList),
        PartIdList = lists:seq(1, 2),
        PartResults = [{ok, _} = erlcloud_s3:upload_part(Bucket, Key, UploadId, PartNum, PartBin, [], S3Conf) || PartNum <- PartIdList],
        ETagList = lists:zip(PartIdList, [proplists:get_value(etag, P) || {ok, P} <- PartResults]),
        ok = erlcloud_s3:complete_multipart(Bucket, Key, UploadId, ETagList, [], S3Conf)
    catch
        ErrType:Cause ->
            io:format("[ERROR] Multipart Upload failed due to ~p, ~p~n", [ErrType, Cause]),
            halt()
    end;
run(?F_MP_UPLOAD_NORMAL_IN_PARALLEL, S3Conf) ->
    Bucket = ?env_bucket(),
    Key = gen_key_for_multipart("in_parallel"),
    PartBin = crypto:strong_rand_bytes(10 * 1024 * 1024),
    try
        {ok, PropList} = erlcloud_s3:start_multipart(Bucket, Key, [], [], S3Conf),
        UploadId = proplists:get_value(uploadId, PropList),
        %% set the large number to make the race be likely to happen on the server side
        PartIdList = lists:seq(1, 8),
        Parent = self(),
        PidList = [spawn(fun() ->
                             {ok, Props} = erlcloud_s3:upload_part(Bucket, Key, UploadId, PartNum, PartBin, [], S3Conf),
                             Parent ! {self(), PartNum, proplists:get_value(etag, Props)}
                         end) || PartNum <- PartIdList],
        ETagList = wait_for_children(PidList),
        ok = erlcloud_s3:complete_multipart(Bucket, Key, UploadId, ETagList, [], S3Conf)
    catch
        ErrType:Cause ->
            io:format("[ERROR] Multipart Upload failed due to ~p, ~p~n", [ErrType, Cause]),
            halt()
    end;
run(?F_MP_UPLOAD_ABORT, S3Conf) ->
    Bucket = ?env_bucket(),
    Key = gen_key_for_multipart("abort"),
    PartBin = crypto:strong_rand_bytes(10 * 1024 * 1024),
    try
        {ok, PropList} = erlcloud_s3:start_multipart(Bucket, Key, [], [], S3Conf),
        UploadId = proplists:get_value(uploadId, PropList),
        PartIdList = lists:seq(1, 2),
        [{ok, _} = erlcloud_s3:upload_part(Bucket, Key, UploadId, PartNum, PartBin, [], S3Conf) || PartNum <- PartIdList],
        ok = erlcloud_s3:abort_multipart(Bucket, Key, UploadId, [], [], S3Conf),
        %% Confirm whether or not part objects have been removed as expected
        AllKeys = [
            lists:append([Bucket, "/", Key]),
            lists:append([Bucket, "/", Key, "\n1"]),
            lists:append([Bucket, "/", Key, "\n1\n1"]),
            lists:append([Bucket, "/", Key, "\n1\n2"]),
            lists:append([Bucket, "/", Key, "\n2"]),
            lists:append([Bucket, "/", Key, "\n2\n1"]),
            lists:append([Bucket, "/", Key, "\n2\n2"])
        ],
        %% Have to wait for aync delete bucket/directory queues consuming all messages
        timer:sleep(timer:seconds(5)), %% for safe
        watch_mq(),
        Results = [begin
                       {ok, Redundancies} = rpc:call(?env_manager(), leo_manager_api, whereis, [[Key4Chunk], true]),
                       case lists:any(
                           fun({_, not_found}) ->
                                  true;
                              ({_, Fields}) when is_list(Fields) ->
                                  case proplists:get_value(del, Fields, 0) of
                                      1 ->
                                          true;
                                      0 ->
                                          false
                                  end;
                              (_Other) ->
                                  false
                       end, Redundancies) of
                           true ->
                               ok;
                           false ->
                               {error, Key4Chunk}
                       end
                   end || Key4Chunk <- AllKeys],
        case lists:foldl(fun(ok, Acc) -> Acc;
                            ({error, K}, Acc) -> [K|Acc]
                         end, [], Results) of
            [] ->
                ok;
            Garbages ->
                io:format("[ERROR] Multipart Upload some part objects left. key:~p ~n", [Garbages]),
                halt()
        end
    catch
        ErrType:Cause ->
            io:format("[ERROR] Multipart Upload failed due to ~p, ~p~n", [ErrType, Cause]),
            halt()
    end;
run(?F_MP_UPLOAD_INVALID_COMPLETE, S3Conf) ->
    Bucket = ?env_bucket(),
    Key = gen_key_for_multipart("invalid_complete"),
    PartBin = crypto:strong_rand_bytes(10 * 1024 * 1024),
    try
        %% Wrong UploadId
        %% This test has not passed yet because leo_gateway has had a bug not returning the proper error code(404) according to the AWS S3 spec.
        %% Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadComplete.html
        {error, {http_error, 404, _, _}} = erlcloud_s3:complete_multipart(Bucket, Key, "no_exist", [1,12345678], [], S3Conf),

        {ok, PropList} = erlcloud_s3:start_multipart(Bucket, Key, [], [], S3Conf),
        UploadId = proplists:get_value(uploadId, PropList),
        PartIdList = lists:seq(1, 2),
        PartResults = [{ok, _} = erlcloud_s3:upload_part(Bucket, Key, UploadId, PartNum, PartBin, [], S3Conf) || PartNum <- PartIdList],
        ETagList = lists:zip(PartIdList, [proplists:get_value(etag, P) || {ok, P} <- PartResults]),
        %% Invalid part number
        InvalidETagList = ETagList ++ [{3, "12345678"}],
        %% Wrong UploadId
        %% This test has not passed yet because leo_gateway has had a bug not returning the proper error code(400) according to the AWS S3 spec.
        %% Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadComplete.html
        {error, {http_error, 400, _, _}} = erlcloud_s3:complete_multipart(Bucket, Key, UploadId, InvalidETagList, [], S3Conf),
        ok
    catch
        ErrType:Cause ->
            io:format("[ERROR] Multipart Upload failed due to ~p, ~p~n", [ErrType, Cause]),
            halt()
    end;
run(_F,_) ->
    ok.

%% @private
%% Waits for all processes uploading parts in parallel
%% Returns the list of the tuple {PartNum, ETag} to use in multipart complete request later
wait_for_children(PidList) ->
    wait_for_children(PidList, []).

wait_for_children([], Acc) ->
    Acc;
wait_for_children(PidList, Acc) ->
    receive
        {Pid, PartNum, ETag} ->
            case lists:member(Pid, PidList) of
                true ->
                    wait_for_children(lists:delete(Pid, PidList), [{PartNum, ETag}|Acc]);
                false ->
                    wait_for_children(PidList, Acc)
            end;
        _ ->
            wait_for_children(PidList, Acc)
    end.

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

%% @doc Generate a key for multipart uploads
%% @private
gen_key_for_multipart(Suffix) when is_list(Suffix) ->
    lists:append(["test/mp/", Suffix]).

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
        erlang:crc32(crypto:strong_rand_bytes(16)), NumOfKeys)).


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
    Val = crypto:strong_rand_bytes(16),
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

%% @private
compare_2(Index, Key, L1, L2) when is_list(element(2, L1)) andalso
                                   is_list(element(2, L2)) ->
    compare_3(Index, Key, element(2, L1), element(2, L2));
compare_2(Index, Key, L1, L2) ->
    compare_3(Index, Key, L1, L2).

%% @private
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


%% @doc Attach the node
%% @private
attach_node(Node) ->
    case rpc:call(?env_manager(), leo_redundant_manager_api,
                  get_member_by_node, [Node]) of
        {error, not_found} ->
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
    case rpc:call(?env_manager(), leo_redundant_manager_api,
                  get_member_by_node, [Node]) of
        {ok, #member{state = ?STATE_ATTACHED}} ->
            rebalance();
        _ ->
            timer:sleep(timer:seconds(5)),
            attach_node_1(Node, Times + 1)
    end.


%% @doc Detach the node
detach_node(Node) ->
    case rpc:call(?env_manager(), leo_manager_api, detach, [Node]) of
        ok ->
            %% Stop the node
            ok = stop_node(Node),

            %% remove the current data
            timer:sleep(timer:seconds(10)),
            LeoFSDir = ?env_leofs_dir(),
            Path_2 = filename:join([LeoFSDir, ?node_to_avs_dir(Node)]),
            case filelib:is_dir(Path_2) of
                true ->
                    case (string:str(Path_2, "avs") > 0) of
                        true ->
                            [] = os:cmd("rm -rf " ++ Path_2),
                            timer:sleep(timer:seconds(3)),
                            ok;
                        false ->
                            ok
                    end;
                false ->
                    ok
            end,

            %% Execute the rebalance-command
            rebalance();
        _Error ->
            ?msg_error(["Could not detach the node:", Node]),
            halt()
    end.


%% @doc Execute rebalance of data
%% @private
rebalance() ->
    case rpc:call(?env_manager(), leo_manager_api, rebalance, [null]) of
        ok ->
            ok;
        _Error ->
            ?msg_error("Fail rebalance (detach-node)"),
            halt()
    end.


%% @doc Suspend the node
%% @private
suspend_node(Node) ->
    case rpc:call(?env_manager(), leo_manager_api, suspend, [Node]) of
        ok ->
            ok;
        _Error ->
            ?msg_error(["Could not suspend the node:", Node]),
            halt()
    end.


%% @doc Resume the node
%% @private
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

%% @private
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
%% @private
check_state_of_node(Node, State) ->
    case rpc:call(?env_manager(), leo_redundant_manager_api,
                  get_member_by_node, [Node]) of
        {ok, #member{state = State}} ->
            ok;
        {ok, _NodeState} ->
            {error, not_yet};
        Other ->
            Other
    end.


%% @doc Stop the node
%% @private
start_node(Node) ->
    Path = filename:join([?env_leofs_dir(), ?node_to_path(Node)]),
    os:cmd(Path ++ " start"),
    ok.


%% @doc Stop the node
%% @private
stop_node(Node) ->
    LeoFSDir = ?env_leofs_dir(),
    Path_1 = filename:join([LeoFSDir, ?node_to_path(Node)]),
    os:cmd(Path_1 ++ " stop"),
    ok.


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
            halt()
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
            halt()
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
            halt()
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
            ?msg_error("data-compaction/data-diagnosis failure"),
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
%% @private
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
