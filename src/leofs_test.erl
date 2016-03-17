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
-module(leofs_test).

-include("leofs_test.hrl").
-include_lib("deps/erlcloud/include/erlcloud_aws.hrl").

-export([main/1]).


%% @doc Execute LeoFS integration test
%%
-spec(main(Args) ->
             ok when Args::[string()]).
main(["-h"]) ->
    help();
main(["--help"]) ->
    help();
main(["-v"]) ->
    version();
main(["--version"]) ->
    version();
main(Args) ->
    %% Prepare
    {Opts, _NonOptArgs}= parse_args(Args),
    io:format(" ~p~n", [Opts]),

    [] = os:cmd("epmd -daemon"),
    net_kernel:start([?NODE, longnames]),


    Bucket = leo_misc:get_value(?PROP_BUCKET, Opts, ?BUCKET),
    ok = application:set_env(?APP, ?PROP_BUCKET, Bucket),

    Cookie = leo_misc:get_value(?PROP_COOKIE, Opts, ?COOKIE),
    erlang:set_cookie(node(), list_to_atom(Cookie)),

    Keys = leo_misc:get_value(?PROP_KEYS, Opts, ?NUM_OF_KEYS),
    ok = application:set_env(?APP, ?PROP_KEYS, Keys),

    LeoFSDir = leo_misc:get_value(?PROP_LEOFS_DIR, Opts, []),
    ok = application:set_env(?APP, ?PROP_LEOFS_DIR, LeoFSDir),

    Manager = leo_misc:get_value(?PROP_MANAGER, Opts, ?MANAGER_NODE),
    ok = application:set_env(?APP, ?PROP_MANAGER, list_to_atom(Manager)),

    %% Load/Start apps
    ok = code:add_paths(["ebin",
                         "deps/erlcloud/ebin",
                         "deps/jsx/ebin",
                         "deps/getopt/ebin",
                         "deps/leo_commons/ebin"
                        ]),
    ok = application:start(crypto),
    ok = application:start(asn1),
    ok = application:start(public_key),
    ok = application:start(ssl),
    ok = application:start(xmerl),

    %% Launch erlcloud
    ok = erlcloud:start(),
    S3Conf = erlcloud_s3:new(?S3_ACCESS_KEY,
                             ?S3_SECRET_KEY,
                             ?S3_HOST,
                             ?S3_PORT),

    %% Launch LeoFS
    case ?env_leofs_dir() of
        [] ->
            void;
        LeoFSDir ->
            ok = leofs_test_launcher:run(LeoFSDir)
    end,

    S3Conf_1 = S3Conf#aws_config{s3_scheme = "http://"},
    case leo_misc:get_value('test', Opts, not_found) of
        not_found ->
            %% Execute scenarios:
            ?msg_start_scenario(),
            StartDateTime = leo_date:now(),
            ok = leofs_test_scenario:run(?SCENARIO_1, S3Conf_1),
            ok = leofs_test_scenario:run(?SCENARIO_2, S3Conf_1),
            ok = leofs_test_scenario:run(?SCENARIO_3, S3Conf_1),
            ok = leofs_test_scenario:run(?SCENARIO_4, S3Conf_1),
            ok = leofs_test_scenario:run(?SCENARIO_5, S3Conf_1),
            ok = leofs_test_scenario:run(?SCENARIO_6, S3Conf_1),
            EndDateTime = leo_date:now(),
            ?msg_finished(EndDateTime - StartDateTime);
        Test ->
            case leo_misc:get_value(Test, ?SC_ITEMS, not_found) of
                not_found ->
                    ?msg_error("Not found the test");
                Test_1 ->
                    ?msg_start_test(Test, Test_1),
                    StartDateTime = leo_date:now(),
                    ok = leofs_test_scenario:run({"TEST", [{Test, Test_1}]}, S3Conf_1),
                    EndDateTime = leo_date:now(),
                    ?msg_finished(EndDateTime - StartDateTime)
            end
    end,
    ok.


%% @doc
%% @private
option_spec_list() ->
    [
     %% {Name, ShortOpt, LongOpt, ArgSpec, HelpMsg}
     {bucket,   $b, "bucket",   string,    "Target a bucket"},
     {cookie,   $c, "cookie",   string,    "Distributed-cookie for communication with LeoFS"},
     {leofs_dir,$d, "dir",      string,    "LeoFS directory"},
     {keys,     $k, "keys",     integer,   "Total number of keys"},
     {manager,  $m, "manager",  string,    "LeoFS Manager"},
     {test,     $t, "test",     atom,      "Execute a test"},
     %% misc
     {help,     $h, "help",     undefined, "Show the program options"},
     {version,  $v, "version",  undefined, "Show version information"}
    ].

help() ->
    OptSpecList = option_spec_list(),
    getopt:usage(OptSpecList, ?APP_STRING).

version() ->
    ok = application:load(?APP),
    {ok, Vsn} = application:get_key(?APP, vsn),
    io:format(?APP_STRING ++ " ~s~n", [Vsn]).

%% @doc Parse getopt options
parse_args(RawArgs) ->
    OptSpecList = option_spec_list(),
        case getopt:parse(OptSpecList, RawArgs) of
        {ok, Args} ->
                Args;
            {error, {_Reason, _Data}} ->
                help(),
                halt(1)
        end.
