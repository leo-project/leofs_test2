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
-define(APP, 'leofs_test').
-define(APP_STRING, atom_to_list(?APP)).

-define(S3_ACCESS_KEY, "05236").
-define(S3_SECRET_KEY, "802562235").
-define(S3_HOST, "localhost").
-define(S3_PORT, 8080).

-define(PROP_MANAGER,   'manager').
-define(PROP_COOKIE,    'cookie').
-define(PROP_BUCKET,    'bucket').
-define(PROP_KEYS,      'keys').
-define(PROP_LEOFS_DIR, 'leofs_dir').

-define(BUCKET,  "backup").
-define(NODE, 'integrator@127.0.0.1').
-define(MANAGER_NODE, "manager_0@127.0.0.1").
-define(COOKIE, "401321b4").
-define(NUM_OF_REPLICAS, 2).
-define(NUM_OF_KEYS,    10000).
-define(UNIT_OF_PARTION, 1000).
-define(THRESHOLD_ERROR_TIMES, 3).
-define(DEF_TIMEOUT, timer:seconds(300)).

-define(msg_start_scenario(),         io:format("~n~s~n", ["::: START :::"])).
-define(msg_start_test(_Test, _Desc), io:format("~n::: TEST: ~w (~s)::::~n", [_Test, _Desc])).
-define(msg_finished(_Sec),           io:format("~n::: Finished (~wsec) :::~n", [_Sec])).
-define(msg_error(Args),              io:format("[ERROR] ~p~n", [Args])).
-define(msg_progress_ongoing(),       io:format("~s", ["-"])).
-define(msg_progress_finished(),      io:format("~s", ["|"])).

-define(env_manager(),
        case application:get_env(?APP, ?PROP_MANAGER) of
            undefined ->
                list_to_atom(?MANAGER_NODE);
            {ok, _EnvManager} ->
                _EnvManager
        end).

-define(env_keys(),
        case application:get_env(?APP, ?PROP_KEYS) of
            undefined ->
                ?NUM_OF_KEYS;
            {ok, _EnvKeys} ->
                _EnvKeys
        end).

-define(env_bucket(),
        case application:get_env(?APP, ?PROP_BUCKET) of
            undefined ->
                ?BUCKET;
            {ok, _EnvBucket} ->
                _EnvBucket
        end).

-define(env_leofs_dir(),
        case application:get_env(?APP, ?PROP_LEOFS_DIR) of
            undefined ->
                [];
            {ok, _EnvLeoFSDir} ->
                _EnvLeoFSDir
        end).


%% TEST Scenatios:
-define(F_PUT_OBJ,        put_objects).
-define(F_DEL_OBJ,        del_objects).
-define(F_CREATE_BUCKET,  create_bucket).
-define(F_CHECK_REPLICAS, check_redundancies).
-define(F_ATTACH_NODE,    attach_node).
-define(F_DETACH_NODE,    detach_node).
-define(F_SUSPEND_NODE,   suspend_node).
-define(F_RESUME_NODE,    resume_node).
-define(F_START_NODE,     start_node).
-define(F_STOP_NODE,      stop_node).
-define(F_WATCH_MQ,       watch_mq).
-define(F_COMPACTION,     compaction).
-define(F_REMOVE_AVS,     remove_avs).
-define(F_RECOVER_NODE,   recover_node).

-define(SC_ITEM_PUT_OBJ,        {?F_PUT_OBJ,        "put objects"}).
-define(SC_ITEM_DEL_OBJ,        {?F_DEL_OBJ,        "remove objects"}).
-define(SC_ITEM_CREATE_BUCKET,  {?F_CREATE_BUCKET,  "create a bucket"}).
-define(SC_ITEM_CHECK_REPLICAS, {?F_CHECK_REPLICAS, "check redundancies of replicas"}).
-define(SC_ITEM_ATTACH_NODE,    {?F_ATTACH_NODE,    "attach a node"}).
-define(SC_ITEM_DETACH_NODE,    {?F_DETACH_NODE,    "detach a node"}).
-define(SC_ITEM_SUSPEND_NODE,   {?F_SUSPEND_NODE,   "suspend a node"}).
-define(SC_ITEM_RESUME_NODE,    {?F_RESUME_NODE,    "resume a node"}).
-define(SC_ITEM_START_NODE,     {?F_START_NODE,     "start a node"}).
-define(SC_ITEM_STOP_NODE,      {?F_STOP_NODE,      "stop a node"}).
-define(SC_ITEM_WATCH_MQ,       {?F_WATCH_MQ,       "watch state of mq"}).
-define(SC_ITEM_COMPACTION,     {?F_COMPACTION,     "execute data-compaction"}).
-define(SC_ITEM_REMOVE_AVS,     {?F_REMOVE_AVS,     "remove avs of a node"}).
-define(SC_ITEM_RECOVER_NODE,   {?F_RECOVER_NODE,   "recover data of a node"}).
-define(SC_ITEMS, [?SC_ITEM_PUT_OBJ,
                   ?SC_ITEM_DEL_OBJ,
                   ?SC_ITEM_CREATE_BUCKET,
                   ?SC_ITEM_CHECK_REPLICAS,
                   ?SC_ITEM_ATTACH_NODE,
                   ?SC_ITEM_DETACH_NODE,
                   ?SC_ITEM_SUSPEND_NODE,
                   ?SC_ITEM_RESUME_NODE,
                   ?SC_ITEM_START_NODE,
                   ?SC_ITEM_STOP_NODE,
                   ?SC_ITEM_WATCH_MQ,
                   ?SC_ITEM_COMPACTION,
                   ?SC_ITEM_REMOVE_AVS,
                   ?SC_ITEM_RECOVER_NODE
                  ]).
-define(SCENARIO_1, {"SCENARIO-1", [?SC_ITEM_CREATE_BUCKET,
                                    ?SC_ITEM_PUT_OBJ,
                                    ?SC_ITEM_CHECK_REPLICAS,
                                    ?SC_ITEM_DEL_OBJ,
                                    ?SC_ITEM_CHECK_REPLICAS
                                   ]}).

-define(SCENARIO_2, {"SCENARIO-2", [?SC_ITEM_PUT_OBJ,
                                    ?SC_ITEM_DETACH_NODE,
                                    ?SC_ITEM_WATCH_MQ,
                                    ?SC_ITEM_CHECK_REPLICAS
                                   ]}).

-define(SCENARIO_3, {"SCENARIO-3", [?SC_ITEM_ATTACH_NODE,
                                    ?SC_ITEM_WATCH_MQ,
                                    ?SC_ITEM_CHECK_REPLICAS,
                                    ?SC_ITEM_COMPACTION,
                                    ?SC_ITEM_CHECK_REPLICAS
                                   ]}).

-define(SCENARIO_4, {"SCENARIO-4", [?SC_ITEM_SUSPEND_NODE,
                                    ?SC_ITEM_STOP_NODE,
                                    ?SC_ITEM_START_NODE,
                                    ?SC_ITEM_RESUME_NODE,
                                    ?SC_ITEM_PUT_OBJ,
                                    ?SC_ITEM_CHECK_REPLICAS
                                   ]}).

-define(SCENARIO_5, {"SCENARIO-5", [?SC_ITEM_REMOVE_AVS,
                                    ?SC_ITEM_RECOVER_NODE,
                                    ?SC_ITEM_WATCH_MQ,
                                    ?SC_ITEM_CHECK_REPLICAS
                                   ]}).

%% @doc Nodes
-define(storage_nodes, ['storage_0@127.0.0.1',
                        'storage_1@127.0.0.1',
                        'storage_2@127.0.0.1',
                        'storage_3@127.0.0.1']).
-define(gateway_nodes, ['gateway_0@127.0.0.1']).
-define(manager_nodes, ['manager_0@127.0.0.1',
                        'manager_1@127.0.0.1']).
-define(nodes, ?storage_nodes ++ ?gateway_nodes ++ ?manager_node).

%% @doc Convert node-name to the path
-define(node_to_path(Node),
        begin
            Name = lists:nth(1, string:tokens(atom_to_list(Node), "@")),
            Type = lists:nth(1, string:tokens(Name, "_")),
            case Type of
                "manager" -> lists:append(["leo_", Name, "/bin/leo_manager"]);
                "storage" -> lists:append(["leo_", Name, "/bin/leo_storage"]);
                "gateway" -> lists:append(["leo_", Name, "/bin/leo_gateway"])
            end
        end).

-define(node_to_avs_dir(Node),
        begin
            Name = lists:nth(1, string:tokens(atom_to_list(Node), "@")),
            filename:join(["leo_"++Name, "avs"])
        end).

%% @doc mq-state record
-record(mq_state, {
          id :: atom(),
          desc = [] :: string(),
          state     :: [{atom(), any()}]
         }).


%% @doc compaction-related
-define(ST_IDLING,     'idling').
-define(ST_RUNNING,    'running').
-define(ST_SUSPENDING, 'suspending').
-type(compaction_state() :: ?ST_IDLING     |
                            ?ST_RUNNING    |
                            ?ST_SUSPENDING).

-record(compaction_stats, {
          status = ?ST_IDLING :: compaction_state(),
          total_num_of_targets    = 0  :: non_neg_integer(),
          num_of_reserved_targets = 0  :: non_neg_integer(),
          num_of_pending_targets  = 0  :: non_neg_integer(),
          num_of_ongoing_targets  = 0  :: non_neg_integer(),
          reserved_targets = []        :: [atom()],
          pending_targets  = []        :: [atom()],
          ongoing_targets  = []        :: [atom()],
          locked_targets   = []        :: [atom()],
          latest_exec_datetime = 0     :: non_neg_integer(),
          acc_reports = []             :: [tuple()]
         }).
