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
-define(NUM_OF_KEYS, 10000).


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
-define(SC_ITEM_PUT_OBJ, {put_objects,   "put objects"}).
-define(SC_ITEM_DEL_OBJ, {del_objects,   "del objects"}).
-define(SC_ITEM_CREATE_BUCKET,  {create_bucket, "create a bucket"}).
-define(SC_ITEM_CHECK_REPLICAS, {check_redundancies, "check redundancies of replicas"}).

-define(SC_ITEM_ATTACH_NODE,     {attach_node,  "attach a node"}).
-define(SC_ITEM_DETACH_NODE,     {detach_node,  "detach a node"}).
-define(SC_ITEM_SUSPEND_NODE,     {suspend_node, "suspend a node"}).
-define(SC_ITEM_RESUME_NODE,      {resume_node,  "resume a node"}).
-define(SC_ITEM_STOP_AND_RESTART, {resume_node,  "stop and restart a node"}).
-define(SC_ITEM_WATCH_MQ,         {watch_mq,     "watch state of mq"}).

-define(SCENARIO_1, {"SCENARIO_1", [?SC_ITEM_CREATE_BUCKET,
                                    ?SC_ITEM_PUT_OBJ,
                                    ?SC_ITEM_CHECK_REPLICAS,
                                    ?SC_ITEM_DEL_OBJ
                                   ]}).

-define(SCENARIO_2, {"SCENARIO_2", [?SC_ITEM_PUT_OBJ,
                                    ?SC_ITEM_DETACH_NODE,
                                    ?SC_ITEM_WATCH_MQ,
                                    ?SC_ITEM_CHECK_REPLICAS
                                   ]}).

-define(SCENARIO_3, {"SCENARIO_3", [?SC_ITEM_PUT_OBJ,
                                    ?SC_ITEM_ATACH_NODE,
                                    ?SC_ITEM_WATCH_MQ,
                                    ?SC_ITEM_CHECK_REPLICAS
                                   ]}).

-define(SCENARIO_4, {"SCENARIO_4", [?SC_ITEM_SUSPEND_NODE,
                                    ?SC_ITEM_STOP_AND_RESTART,
                                    ?SC_ITEM_RESUME_NODE,
                                    ?SC_ITEM_PUT_OBJ,
                                    ?SC_ITEM_CHECK_REPLICAS
                                   ]}).


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


-record(mq_state, {
          id :: atom(),
          desc = [] :: string(),
          state     :: [{atom(), any()}]
         }).
