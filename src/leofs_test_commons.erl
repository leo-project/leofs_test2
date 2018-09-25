%%======================================================================
%%
%% LeoFS
%%
%% Copyright (c) 2012-2018 Rakuten, Inc.
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
-define(DUMP_RING_NODE,'storage_1@127.0.0.1').
-define(UPDATE_LOG_LEVEL_NODE,'storage_1@127.0.0.1').
-define(RECOVER_NODE,  'storage_2@127.0.0.1').
-define(TAKEOVER_NODE, 'storage_4@127.0.0.1').

-define(MNESIA_BACKUP_PATH, "mnesia.bak").

%% MQ related
-define(MQ_QUEUE_SUSPENDED, "leo_per_object_queue").
-define(MQ_STATE_SUSPENDING_FORCE, "suspending(force)").
-define(MQ_STATE_IDLING, "idling").
-define(MQ_STATE_RUNNING, "running").

%% @doc Execute tests
run(?F_CREATE_BUCKET, S3Conf) ->
    catch erlcloud_s3:create_bucket(?env_bucket(), S3Conf),
    ok;
run(?F_DELETE_BUCKET, S3Conf) ->
    catch erlcloud_s3:delete_bucket(?env_bucket(), S3Conf),
    timer:sleep(timer:seconds(10)),
    ok;
%% mnesia backup/restore
run(?F_MNESIA, _S3Conf) ->
    {ok, _} = libleofs:backup_mnesia(?S3_HOST, ?LEOFS_ADM_JSON_PORT, ?MNESIA_BACKUP_PATH),

    %% update mnesia records
    UserID = "user",
    Endpoint = "endpoint",
    Bucket = "bucket",
    AccessKey = "05236",
    {ok, _} = libleofs:create_user(?S3_HOST, ?LEOFS_ADM_JSON_PORT, UserID, "foo"),
    {ok, _} = libleofs:add_endpoint(?S3_HOST, ?LEOFS_ADM_JSON_PORT, Endpoint),
    {ok, _} = libleofs:add_bucket(?S3_HOST, ?LEOFS_ADM_JSON_PORT, Bucket, AccessKey),

    {ok, _} = libleofs:restore_mnesia(?S3_HOST, ?LEOFS_ADM_JSON_PORT, ?MNESIA_BACKUP_PATH),

    %% check if records updated before restoring mnesia are gone
    {ok, UserList} = libleofs:get_users(?S3_HOST, ?LEOFS_ADM_JSON_PORT),
    BinUID = list_to_binary(UserID),
    [] = lists:filter(fun(U) ->
                          proplists:get_value(<<"user_id">>, U) =:= BinUID
                      end, UserList),
    {ok, EndpointList} = libleofs:get_endpoints(?S3_HOST, ?LEOFS_ADM_JSON_PORT),
    BinEndpoint = list_to_binary(Endpoint),
    [] = lists:filter(fun(E) ->
                          proplists:get_value(<<"endpoint">>, E) =:= BinEndpoint
                      end, EndpointList),
    {ok, BucketList} = libleofs:get_buckets(?S3_HOST, ?LEOFS_ADM_JSON_PORT),
    BinBucket = list_to_binary(Bucket),
    [] = lists:filter(fun(B) ->
                          proplists:get_value(<<"bucket">>, B) =:= BinBucket
                      end, BucketList),
    ok;
%% user/endpoint/bucket CRUD
run(?F_USER_CRUD, _S3Conf) ->
    UserID = "user",
    {ok, _} = libleofs:create_user(?S3_HOST, ?LEOFS_ADM_JSON_PORT, UserID, "foo"),
    {ok, _} = libleofs:update_user_password(?S3_HOST, ?LEOFS_ADM_JSON_PORT, UserID, "bar"),
    {ok, _} = libleofs:update_user_role(?S3_HOST, ?LEOFS_ADM_JSON_PORT, UserID, "9"),
    {ok, UserList} = libleofs:get_users(?S3_HOST, ?LEOFS_ADM_JSON_PORT),
    BinUID = list_to_binary(UserID),
    Pred = fun(U) ->
               case proplists:get_value(<<"user_id">>, U) of
                   BinUID ->
                       proplists:get_value(<<"role_id">>, U) =:= 9;
                   _ ->
                       false
               end
           end,
    [_H|_] = lists:filter(Pred, UserList),
    {ok, _} = libleofs:delete_user(?S3_HOST, ?LEOFS_ADM_JSON_PORT, UserID),
    {ok, UserList2} = libleofs:get_users(?S3_HOST, ?LEOFS_ADM_JSON_PORT),
    [] = lists:filter(Pred, UserList2),
    ok;
run(?F_ENDPOINT_CRUD, _S3Conf) ->
    Endpoint = "endpoint",
    {ok, _} = libleofs:add_endpoint(?S3_HOST, ?LEOFS_ADM_JSON_PORT, Endpoint),
    {ok, EndpointList} = libleofs:get_endpoints(?S3_HOST, ?LEOFS_ADM_JSON_PORT),
    BinEndpoint = list_to_binary(Endpoint),
    Pred = fun(E) ->
               case proplists:get_value(<<"endpoint">>, E) of
                   BinEndpoint ->
                       true;
                   _ ->
                       false
               end
           end,
    [_H|_] = lists:filter(Pred, EndpointList),
    {ok, _} = libleofs:delete_endpoint(?S3_HOST, ?LEOFS_ADM_JSON_PORT, Endpoint),
    {ok, EndpointList2} = libleofs:get_endpoints(?S3_HOST, ?LEOFS_ADM_JSON_PORT),
    [] = lists:filter(Pred, EndpointList2),
    ok;
run(?F_BUCKET_CRUD, _S3Conf) ->
    Bucket = "bucket",
    AccessKey = "05236",
    {ok, _} = libleofs:add_bucket(?S3_HOST, ?LEOFS_ADM_JSON_PORT, Bucket, AccessKey),
    %% {error, <<"Already yours">>
    {error, _} = libleofs:add_bucket(?S3_HOST, ?LEOFS_ADM_JSON_PORT, Bucket, AccessKey),
    {ok, BucketList} = libleofs:get_buckets(?S3_HOST, ?LEOFS_ADM_JSON_PORT),
    BinBucket = list_to_binary(Bucket),
    Pred = fun(B) ->
               case proplists:get_value(<<"bucket">>, B) of
                   BinBucket ->
                       true;
                   _ ->
                       false
               end
           end,
    [_|_] = lists:filter(Pred, BucketList),
    {ok, BucketList2} = libleofs:get_buckets(?S3_HOST, ?LEOFS_ADM_JSON_PORT, AccessKey),
    [_|_] = lists:filter(Pred, BucketList2),
    {ok, _} = libleofs:delete_bucket(?S3_HOST, ?LEOFS_ADM_JSON_PORT, Bucket, AccessKey),
    {ok, BucketList3} = libleofs:get_buckets(?S3_HOST, ?LEOFS_ADM_JSON_PORT),
    [] = lists:filter(Pred, BucketList3),
    {ok, BucketList4} = libleofs:get_buckets(?S3_HOST, ?LEOFS_ADM_JSON_PORT, AccessKey),
    [] = lists:filter(Pred, BucketList4),
    timer:sleep(timer:seconds(10)), %% Wait until background delete-bucket process finishes
    ok;
run(?F_UPDATE_LOG_LEVEL, _S3Conf) ->
    {ok, _} = libleofs:update_log_level(?S3_HOST, ?LEOFS_ADM_JSON_PORT, atom_to_list(?UPDATE_LOG_LEVEL_NODE), "warn"),
    {ok, [{<<"node_stat">>, NodeStat}]} = libleofs:status(?S3_HOST, ?LEOFS_ADM_JSON_PORT, atom_to_list(?UPDATE_LOG_LEVEL_NODE)),
    case proplists:get_value(<<"log_level">>, NodeStat) of
        <<"warn">> ->
            ok;
        Other ->
            io:format("[ERROR] update_log_level failed. the value not changed. val:~p~n", [Other]),
            halt()
    end;
run(?F_UPDATE_CONSISTENCY_LEVEL, S3Conf) ->
    {ok, _} = libleofs:update_consistency_level(?S3_HOST, ?LEOFS_ADM_JSON_PORT, 2, 2, 2),
    {ok, [{<<"system_info">>, SysInfo}|_]} = libleofs:status(?S3_HOST, ?LEOFS_ADM_JSON_PORT),
    case proplists:get_value(<<"r">>, SysInfo) of
        2 ->
            ok;
        RC ->
            io:format("[ERROR] update_consistency_level failed. the value 'r' not changed. val:~p~n", [RC]),
            halt()
    end,
    case proplists:get_value(<<"w">>, SysInfo) of
        2 ->
            ok;
        WC ->
            io:format("[ERROR] update_consistency_level failed. the value 'w' not changed. val:~p~n", [WC]),
            halt()
    end,
    case proplists:get_value(<<"d">>, SysInfo) of
        2 ->
            ok;
        DC ->
            io:format("[ERROR] update_consistency_level failed. the value 'd' not changed. val:~p~n", [DC]),
            halt()
    end,
    %% Check if consistency level have really changed.
    Bucket = ?env_bucket(),
    Key = gen_key(1),
    Key4LeoFS = list_to_binary(lists:append([Bucket, "/", Key])),
    Val = crypto:strong_rand_bytes(16),
    Nodes = get_storage_nodes(),
    Node = lists:nth(rand:uniform(length(Nodes)), Nodes),
    {ok, _} = rpc:call(Node, leo_storage_handler_object,
                  debug_put, [Key4LeoFS, Val, 1]),
    try
        Ret = erlcloud_s3:get_object(Bucket, Key, S3Conf),
        %% get_object should fail due to lack of replica.
        io:format("[ERROR] get_object which was supposed to fail succeeded. ret val:~p~n", [Ret]),
        halt()
    catch
        _:_ ->
        %% 500 error should happen
        ok
    after
        %% rollback to the original setting
        {ok, _} = libleofs:update_consistency_level(?S3_HOST, ?LEOFS_ADM_JSON_PORT, 1, 1, 1)
    end;
run(?F_DUMP_RING, _S3Conf) ->
    {ok, _} = libleofs:dump_ring(?S3_HOST, ?LEOFS_ADM_JSON_PORT, atom_to_list(?DUMP_RING_NODE)),
    Dir = lists:append([?env_leofs_dir(), "/leo_storage_1/log/ring"]),
    {ok, DumpFiles} = file:list_dir(Dir),
    [{ok, _} = file:consult(lists:append([Dir, "/", F])) || F <- DumpFiles],
    ok;
run(?F_PURGE_CACHE, S3Conf) ->
    %% PUT
    Bucket = ?env_bucket(),
    Key = gen_key_for_purge_cache("temp"),
    Val = crypto:strong_rand_bytes(16),
    [_H|_Rest] = erlcloud_s3:put_object(Bucket, Key, Val, [], S3Conf),
    %% GET. x-amz-meta-leofs-from-cache should be included (Cache Hit)
    case erlcloud_s3:get_object(Bucket, Key, S3Conf) of
        Ret when is_list(Ret) ->
            case proplists:get_value("x-amz-meta-leofs-from-cache", Ret) of
                undefined ->
                    io:format("[ERROR] purge-cache failed due to undefined x-amz-meta-leofs-from-cache in proplists:~p~n", [Ret]),
                    halt();
                _ ->
                    ok
            end;
        Error ->
            io:format("[ERROR] purge-cache failed due to get_object failure:~p~n", [Error]),
            halt()
    end,
    %% purge-cache
    Key4LeoFS = lists:append([Bucket, "/", Key]),
    {ok, _} = libleofs:purge(?S3_HOST, ?LEOFS_ADM_JSON_PORT, Key4LeoFS),
    %% GET. x-amz-meta-leofs-from-cache should NOT be included (Cache MISS)
    case erlcloud_s3:get_object(Bucket, Key, S3Conf) of
        Ret2 when is_list(Ret2) ->
            case proplists:get_value("x-amz-meta-leofs-from-cache", Ret2) of
                undefined ->
                    ok;
                _ ->
                    io:format("[ERROR] purge-cache failed due to x-amz-meta-leofs-from-cache existing in proplists:~p~n", [Ret2]),
                    halt()
            end;
        Error2 ->
            io:format("[ERROR] purge-cache failed due to get_object failure:~p~n", [Error2]),
            halt()
    end;
run(?F_PUT_OBJ, S3Conf) ->
    Keys = ?env_keys(),
    ok = put_object(S3Conf, Keys),
    ok;
run(?F_PUT_ZERO_BYTE_OBJ, S3Conf) ->
    Keys = ?env_keys(),
    ok = put_zero_byte_object(S3Conf, Keys),
    ok;
run(?F_PUT_INCONSISTENT_OBJ, S3Conf) ->
    Keys = ?env_keys(),
    ok = put_inconsistent_object(S3Conf, Keys),
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
    timer:sleep(timer:seconds(10)),
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
    timer:sleep(timer:seconds(5)),
    ok;
run(?F_RESUME_NODE,_S3Conf) ->
    ok = resume_node(?RESUME_NODE),
    ok;
run(?F_START_NODE,_S3Conf) ->
    ok = start_node(?SUSPEND_NODE),
    ok;
run(?F_STOP_NODE,_S3Conf) ->
    ok = stop_node(?SUSPEND_NODE),
    timer:sleep(timer:seconds(10)),
    ok;
run(?F_WATCH_MQ,_S3Conf) ->
    ok = watch_mq(),
    timer:sleep(timer:seconds(5)),
    ok;
run(?F_DIAGNOSIS,_S3Conf) ->
    ok = diagnosis(),
    ok;
run(?F_COMPACTION,_S3Conf) ->
    ok = compaction(),
    ok;
run(?F_DU,_S3Conf) ->
    ok = du(),
    ok;
run(?F_REMOVE_AVS,_S3Conf) ->
    ok = remove_avs(?RECOVER_NODE),
    ok;
run(?F_RECOVER_FILE,_S3Conf) ->
    ok = recover_file(),
    ok;
run(?F_RECOVER_NODE,_S3Conf) ->
    ok = recover_node(?RECOVER_NODE),
    ok;
run(?F_SCRUB_CLUSTER,_S3Conf) ->
    ok = recover_consistency(),
    ok;
%% MQ(Message Queue - leo_mq) related
run(?F_MQ_SUSPEND_QUEUE,_S3Conf) ->
    ok = mq_suspend_queue(atom_to_list(?SUSPEND_NODE), ?MQ_QUEUE_SUSPENDED),
    ok = mq_wait_until(atom_to_list(?SUSPEND_NODE), ?MQ_QUEUE_SUSPENDED, [?MQ_STATE_SUSPENDING_FORCE]),
    timer:sleep(timer:seconds(5)),
    ok;
run(?F_MQ_RESUME_QUEUE,_S3Conf) ->
    ok = mq_resume_queue(atom_to_list(?RESUME_NODE), ?MQ_QUEUE_SUSPENDED),
    ok = mq_wait_until(atom_to_list(?RESUME_NODE), ?MQ_QUEUE_SUSPENDED, [?MQ_STATE_IDLING, ?MQ_STATE_RUNNING]),
    timer:sleep(timer:seconds(5)),
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
        AllKeys = [lists:append([Bucket, "/", Key]),
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
                              fun({_, {error, not_found}}) ->
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

%% @doc Generate a key for purge-cache
%% @private
gen_key_for_purge_cache(Suffix) when is_list(Suffix) ->
    lists:append(["test/pc/", Suffix]).

%% @doc Generate a key for recover-file
%% @private
gen_key_for_recover_file(Suffix) when is_list(Suffix) ->
    lists:append([?env_bucket(), "/test/rf/", Suffix]).


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

put_inconsistent_object(Conf, Keys) when Keys > ?UNIT_OF_PARTION ->
    do_concurrent_exec(Conf, Keys, fun put_inconsistent_object_1/5);
put_inconsistent_object(Conf, Keys) ->
    put_inconsistent_object_1(Conf, undefined, undefined, 1, Keys).

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

put_inconsistent_object_1(_, From, Ref, Start, End) when Start > End ->
    case (From == undefined andalso
          Ref  == undefined) of
        true ->
            ?msg_progress_finished(),
            ok;
        false ->
            erlang:send(From, {Ref, ok})
    end;
put_inconsistent_object_1(Conf, From, Ref, Start, End) ->
    indicator(Start),
    Key = gen_key(Start),
    Key4LeoFS = list_to_binary(lists:append([?env_bucket(), "/", Key])),
    Val = crypto:strong_rand_bytes(16),
    Nodes = get_storage_nodes(),
    Node = lists:nth(rand:uniform(length(Nodes)), Nodes),
    case rpc:call(Node, leo_storage_handler_object,
                  debug_put, [Key4LeoFS, Val, 1]) of
        {ok, _} ->
            put_inconsistent_object_1(Conf, From, Ref, Start + 1, End);
        _ ->
            timer:sleep(timer:seconds(1)),
            put_inconsistent_object_1(Conf, From, Ref, Start, End)
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
            timer:sleep(timer:seconds(15)),
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
    watch_mq(1).
watch_mq(Times) ->
    io:format("~n        #~w", [Times]),
    Nodes = get_storage_nodes(),
    watch_mq_1(Nodes, [], Times).

watch_mq_1([], Acc, Times) ->
    case (length(Acc) > 0) of
        true ->
            timer:sleep(timer:seconds(5)),
            watch_mq(Times + 1);
        false ->
            ok
    end;
watch_mq_1([Node|Rest], Acc, Times) ->
    io:format("~n            * ~p:", [Node]),
    case rpc:call(?env_manager(),
                  leo_manager_api, mq_stats, [Node]) of
        {ok, RetL} ->
            Acc_1 = case watch_mq_2(RetL, []) of
                        ok ->
                            Acc;
                        _ ->
                            [Node|Acc]
                    end,
            watch_mq_1(Rest, Acc_1, Times);
        _ ->
            ?msg_error("Could not retrieve mq-state of the node"),
            halt()
    end.

watch_mq_2([], []) ->
    io:format("[N/A]", []),
    ok;
watch_mq_2([], Acc) ->
    io:format("[", []),
    lists:foldl(
      fun({Id, Num}, I)  ->
              case (I < length(Acc)) of
                  true ->
                      io:format("{~p,~p}, ", [Id, Num]);
                  false ->
                      io:format("{~p,~p}", [Id, Num])
              end,
              I + 1
      end, 1, Acc),
    io:format("]", []),
    still_running;
watch_mq_2([#mq_state{id = Id,
                      state = Stats}|Rest], Acc) ->
    NumOfMsgs = leo_misc:get_value('consumer_num_of_msgs', Stats, 0),
    Acc_1 = case (NumOfMsgs < 1) of
                true ->
                    Acc;
                false ->
                    [{Id, NumOfMsgs}|Acc]
            end,
    watch_mq_2(Rest, Acc_1).


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
            case Rest of
                [] ->
                    %% do suspend/resume test
                    {ok, _} = libleofs:compact_suspend(?S3_HOST, ?LEOFS_ADM_JSON_PORT, atom_to_list(Node)),
                    ok = compact_wait_until(Node, ?ST_SUSPENDING),
                    timer:sleep(timer:seconds(5)),
                    {ok, _} = libleofs:compact_resume(?S3_HOST, ?LEOFS_ADM_JSON_PORT, atom_to_list(Node)),
                    ok = compact_wait_until(Node, ?ST_RUNNING);
                _Other ->
                    nop
            end,
            ok = compaction_2(Node),
            compaction_1(Rest);
        Error ->
            ?msg_error(["Could not execute data-compaction. error:", Error]),
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

%% @doc Execute du
%% @private
du() ->
    Nodes = get_storage_nodes(),
    du_1(Nodes).

%% @private
du_1([]) ->
    ?msg_progress_finished(),
    ok;
du_1([Node|Rest]) ->
    ?msg_progress_ongoing(),
    {ok, DU} = libleofs:du(?S3_HOST, ?LEOFS_ADM_JSON_PORT, atom_to_list(Node)),
    case proplists:get_value(<<"active_num_of_objects">>, DU) of
        Val when is_integer(Val) ->
            du_1(Rest);
        Other ->
            io:format("[ERROR] du failure. node:~p value:~p~n", [Node, Other]),
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
    recover(Node, "node").

recover_consistency() ->
    Nodes = get_storage_nodes(),
    recover_consistency(Nodes).

recover_consistency([]) ->
    timer:sleep(timer:seconds(10)),
    ok;
recover_consistency([H|Rest]) ->
    ok = recover(H, "consistency"),
    watch_mq(),
    recover_consistency(Rest).

recover(Node, Type) ->
    Ret = case recover_1(Node, 0) of
              ok ->
                  case rpc:call(?env_manager(), leo_manager_api,
                                recover, [Type, Node, true]) of
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
            timer:sleep(timer:seconds(10)),
            ok;
        _ ->
            io:format("[ERROR] recover-~s failure. node:~p~n", [Type, Node]),
            halt()
    end.

%% @private
recover_1(_Node, ?THRESHOLD_ERROR_TIMES) ->
    {error, over_threshold_error_times};
recover_1(Node, Times) ->
    case check_state_of_node(Node, ?STATE_RUNNING) of
        ok ->
            ok;
        {error, not_yet} ->
            timer:sleep(timer:seconds(10)),
            recover_1(Node, Times + 1);
        Other ->
            Other
    end.

%% @doc Recover files with various replicas conditions
%% @private
recover_file() ->
    Replicas = application:get_env(?APP, ?PROP_REPLICAS, ?NUM_OF_REPLICAS),
    %% the combinations of the inconsistency
    TestSuites = case Replicas of
                     2 ->
                         [
                          {"lack_primary", [false, true]},
                          {"lack_secondary", [true, false]}
                         ];
                     3 ->
                         [
                          {"repl_100", [true, false, false]},
                          {"repl_110", [true, true, false]},
                          {"repl_101", [true, false, true]},
                          {"repl_010", [false, true, false]},
                          {"repl_011", [false, true, true]},
                          {"repl_001", [false, false, true]}
                         ];
                     _ ->
                        io:format("[ERROR] recover-file failure due to the unsupportred num_of_replicas replicas:~p~n", [Replicas]),
                        halt()
                 end,
    %% PUT
    [put_inconsistent_object_with_fine_grained_ctrl(Suffix, ReplicaState) || {Suffix, ReplicaState} <- TestSuites],
    [{ok, _} = libleofs:recover_file(?S3_HOST, ?LEOFS_ADM_JSON_PORT, gen_key_for_recover_file(Suffix)) || {Suffix, _} <- TestSuites],
    timer:sleep(timer:seconds(25)), %% for safe
    watch_mq(),
    [check_redundancies_2(gen_key_for_recover_file(Suffix)) || {Suffix, _} <- TestSuites],
    %% DELETE
    [delete_inconsistent_object_with_fine_grained_ctrl(Suffix, ReplicaState) || {Suffix, ReplicaState} <- TestSuites],
    [{ok, _} = libleofs:recover_file(?S3_HOST, ?LEOFS_ADM_JSON_PORT, gen_key_for_recover_file(Suffix)) || {Suffix, _} <- TestSuites],
    timer:sleep(timer:seconds(25)), %% for safe
    watch_mq(),
    [check_redundancies_2(gen_key_for_recover_file(Suffix)) || {Suffix, _} <- TestSuites],
    ok.

put_inconsistent_object_with_fine_grained_ctrl(Suffix, ReplicaState) ->
    Key = list_to_binary(gen_key_for_recover_file(Suffix)),
    Val = crypto:strong_rand_bytes(16),
    Nodes = get_storage_nodes(),
    Node = lists:nth(rand:uniform(length(Nodes)), Nodes),
    case rpc:call(Node, leo_storage_handler_object,
                  debug_put, [Key, Val, ReplicaState]) of
        {ok, _} ->
            ok;
        Error ->
            io:format("[ERROR] recover-file RPC failure. node:~p key:~p err:~p~n", [Node, Key, Error]),
            halt()
    end.

delete_inconsistent_object_with_fine_grained_ctrl(Suffix, ReplicaState) ->
    Key = list_to_binary(gen_key_for_recover_file(Suffix)),
    Nodes = get_storage_nodes(),
    Node = lists:nth(rand:uniform(length(Nodes)), Nodes),
    case rpc:call(Node, leo_storage_handler_object,
                  debug_delete, [Key, ReplicaState]) of
        {ok, _} ->
            ok;
        Error ->
            io:format("[ERROR] recover-file RPC failure. node:~p key:~p err:~p~n", [Node, Key, Error]),
            halt()
    end.


%% @doc MQ related
%% @private
mq_suspend_queue(Node, Queue) ->
    {ok, _} = libleofs:mq_suspend(?S3_HOST, ?LEOFS_ADM_JSON_PORT, Node, Queue),
    ok.
mq_resume_queue(Node, Queue) ->
    {ok, _} = libleofs:mq_resume(?S3_HOST, ?LEOFS_ADM_JSON_PORT, Node, Queue),
    ok.
mq_wait_until(Node, Queue, StateToBe) ->
    StateToBe2 = [list_to_binary(S) || S <- StateToBe],
    mq_wait_until(Node, list_to_binary(Queue), StateToBe2, 0).

mq_wait_until(Node, Queue, StateToBe, ?THRESHOLD_ERROR_TIMES) ->
    io:format("Timeout to wait for node:~p queue:~p transiting to ~p~n", [Node, Queue, StateToBe]),
    halt();
mq_wait_until(Node, Queue, StateToBe, Retry) ->
    {ok, [{<<"mq_stats">>, QueueList}]} = libleofs:mq_stats(?S3_HOST, ?LEOFS_ADM_JSON_PORT, Node),
    Pred = fun(T) ->
               Id = proplists:get_value(<<"id">>, T),
               case Id of
                   Queue ->
                       true;
                   _ ->
                       false
               end
           end,
    [H|_] = lists:filter(Pred, QueueList),
    case lists:member(proplists:get_value(<<"state">>, H), StateToBe) of
        true ->
            ok;
        false ->
            timer:sleep(timer:seconds(5)),
            mq_wait_until(Node, Queue, StateToBe, Retry + 1)
    end.

compact_wait_until(Node, StateToBe) ->
    compact_wait_until(atom_to_list(Node), atom_to_binary(StateToBe, latin1), 0).

compact_wait_until(Node, StateToBe, ?THRESHOLD_ERROR_TIMES) ->
    io:format("Timeout to wait for compacting node:~p transiting to ~p~n", [Node, StateToBe]),
    halt();
compact_wait_until(Node, StateToBe, Retry) ->
    {ok, [{<<"compaction_status">>, CompactStatus}]} = libleofs:compact_status(?S3_HOST, ?LEOFS_ADM_JSON_PORT, Node),
    case proplists:get_value(<<"status">>, CompactStatus) of
        StateToBe ->
            ok;
        false ->
            timer:sleep(timer:seconds(5)),
            compact_wait_until(Node, StateToBe, Retry + 1)
    end.
