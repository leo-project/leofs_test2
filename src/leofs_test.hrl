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


-define(PROP_MANAGER, 'manager').
-define(PROP_COOKIE,  'cookie').
-define(PROP_BUCKET,  'bucket').
-define(PROP_KEYS,    'keys').

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

