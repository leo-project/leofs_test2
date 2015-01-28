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
-module(leofs_test_scenario).
-include("leofs_test.hrl").
-export([run/2]).

%% @doc scenario-1:
%%      - put 10000 object
%%      - check redundancies of each object
%%      - retrieve 10000 object
%%      - remove 1000 object
%%      - check redundancies of each object
%%
run({ScenarioName, Items}, S3Conf) ->
    io:format("~n [~s]", [ScenarioName]),
    run_1(Items, S3Conf).

%% @private
run_1([],_S3Conf) ->
    ok;
run_1([{F, Description}|Rest], S3Conf) ->
    io:format("~n   * ~w (~s)", [F, Description]),
    timer:sleep(timer:seconds(5)),
    ok = leofs_test_commons:run(F, S3Conf),
    run_1(Rest, S3Conf).
