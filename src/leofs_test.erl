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

main(["help"]) ->
    help();
main(["version"]) ->
    version();
main(Args) ->
    %% Prepare
    {Opts, _NonOptArgs}= parse_args(Args),
    io:format(" ~p~n", [Opts]),

    [] = os:cmd("epmd -daemon"),
    net_kernel:start([?NODE, longnames]),

    Cookie = leo_misc:get_value(?PROP_COOKIE, Opts, ?COOKIE),
    erlang:set_cookie(node(), list_to_atom(Cookie)),

    Manager = leo_misc:get_value(?PROP_MANAGER, Opts, ?MANAGER_NODE),
    ok = application:set_env(?APP, ?PROP_MANAGER, list_to_atom(Manager)),

    Keys = leo_misc:get_value(?PROP_KEYS, Opts, ?NUM_OF_KEYS),
    ok = application:set_env(?APP, ?PROP_KEYS, Keys),

    Bucket = leo_misc:get_value(?PROP_BUCKET, Opts, ?BUCKET),
    ok = application:set_env(?APP, ?PROP_BUCKET, Bucket),

    %% Load/Start apps
    ok = code:add_paths(["ebin",
                         "deps/erlcloud/ebin",
                         "deps/lhttpc/ebin",
                         "deps/jsx/ebin",
                         "deps/getopt/ebin",
                         "deps/leo_commons/ebin"
                        ]),
    ok = application:start(crypto),
    ok = application:start(asn1),
    ok = application:start(public_key),
    ok = application:start(ssl),
    ok = application:start(lhttpc),
    ok = application:start(xmerl),

    %% Launch erlcloud
    ok = erlcloud:start(),
    S3Conf = erlcloud_s3:new(?S3_ACCESS_KEY,
                             ?S3_SECRET_KEY,
                             ?S3_HOST,
                             ?S3_PORT),

    %% Execute scenarios:
    ok = leofs_test_s1:run(S3Conf#aws_config{s3_scheme = "http://"}),
    ok.


%% @doc
%% @private
option_spec_list() ->
    [
     %% {Name, ShortOpt, LongOpt, ArgSpec, HelpMsg}
     {help,     $h, "help",     undefined, "Show the program options"},
     {manager,  $m, "manager",  string,    "Specify a manager node to connect"},
     {cookie,   $c, "cookie",   string,    "Specify a cookie to connect"},
     {bucket,   $b, "bucket",   string,    "Specify a bucket to check data"},
     {keys,     $k, "keys",     integer,   "Specify a total number of keys"},
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
