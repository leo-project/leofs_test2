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
-module(leofs_test_launcher).

-include("leofs_test.hrl").
-include("leo_redundant_manager.hrl").

-export([run/1]).


%% @doc Execute launch of LeoFS
-spec(run(Dir) ->
             ok when Dir::string()).
run(Dir) ->
    %% Check the directory
    PackageDirs = [filename:join([Dir, "leo_manager_0"]),
                   filename:join([Dir, "leo_manager_1"]),
                   filename:join([Dir, "leo_storage_0"]),
                   filename:join([Dir, "leo_storage_1"]),
                   filename:join([Dir, "leo_storage_2"]),
                   filename:join([Dir, "leo_storage_3"]),
                   filename:join([Dir, "leo_gateway_0"])],
    ok = check_packages(PackageDirs),

    %% Launch each server
    ok = launch(PackageDirs),

    %% Check state of each server
    {ok, Nodes} = check_status(attached, 4),
    io:format("nodes:~p~n", [Nodes]),

    %%

    ok.


%% @doc Check directory of the packages
%% @private
check_packages([]) ->
    ok;
check_packages([Dir|Rest]) ->
    case filelib:is_dir(Dir) of
        true ->
            check_packages(Rest);
        false ->
            io:format("ERROR: ~s~n", ["Not exist package of LeoFS applications"]),
            halt()
    end.


%% @doc Launch the applications
%% @private
launch([]) ->
    ok;
launch([Dir|Rest]) ->
    case get_type_from_dir(Dir) of
        leo_manager ->
            os:cmd(Dir ++ "/bin/leo_manager start");
        leo_storage ->
            os:cmd(Dir ++ "/bin/leo_storage start");
        leo_gateway ->
            os:cmd(Dir ++ "/bin/leo_gateway start");
        _ ->
            void
    end,
    launch(Rest).


%% @private
get_type_from_dir(Dir) ->
    case string:str(Dir, "leo_manager") > 0 of
        true ->
            'leo_manager';
        false ->
            case string:str(Dir, "leo_storage") > 0 of
                true ->
                    'leo_storage';
                false ->
                    case string:str(Dir, "leo_gateway") > 0 of
                        true ->
                            'leo_gateway';
                        false ->
                            'invlid name'
                    end
            end
    end.


%% @private
check_status(attached, NumOfStorages) ->
    Manager = ?env_manager(),
    Ret = case rpc:call(Manager,
                        leo_manager_mnesia, get_storage_nodes_all, []) of
              {ok, RetL} when length(RetL) == NumOfStorages ->
                  Nodes = [N || #node_state{node = N,
                                            state = ?STATE_ATTACHED} <- RetL],
                  case length(Nodes) == NumOfStorages of
                      true ->
                          {ok, Nodes};
                      false ->
                          not_prepared
                  end;
              _ ->
                  not_prepared
          end,

    case Ret of
        not_prepared ->
            timer:sleep(timer:seconds(1)),
            check_status(attached, NumOfStorages);
        {ok, StorageNodes} ->
            case rpc:call(Manager, leo_manager_api, start, [null]) of
                ok ->
                    check_status(gateway, 1, StorageNodes);
                _ ->
                    io:format("ERROR: ~s~n", ["Could not launch LeoFS"]),
                    halt()
            end
    end.

check_status(gateway, NumOfGateways, StorageNodes) ->
    Manager = ?env_manager(),
    Ret = case rpc:call(Manager,
                        leo_manager_mnesia, get_gateway_nodes_all, []) of
              {ok, RetL} when length(RetL) == NumOfGateways ->
                  Nodes = [N || #node_state{node = N,
                                            state = ?STATE_RUNNING} <- RetL],
                  case length(Nodes) == NumOfGateways of
                      true ->
                          {ok, Nodes};
                      false ->
                          not_prepared
                  end;
              _ ->
                  not_prepared
          end,

    case Ret of
        not_prepared ->
            timer:sleep(timer:seconds(1)),
            check_status(gateway, NumOfGateways, StorageNodes);
        {ok, GatewayNodes} ->
            {ok, StorageNodes ++ GatewayNodes}
    end.
