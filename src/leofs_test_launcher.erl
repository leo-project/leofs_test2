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
    io:format("~p~n", [Dir]),
    case check_status(running) of
        not_prepared ->
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
            {ok,_Nodes} = check_status(attached),
            ok;
        _ ->
            ok
    end.


%% @doc Check directory of the packages
%% @private
check_packages([]) ->
    ok;
check_packages([Dir|Rest]) ->
    case filelib:is_dir(Dir) of
        true ->
            check_packages(Rest);
        false ->
            ?msg_error("Not exist package of LeoFS applications"),
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


%% @doc Retrieve type of a node from the directory
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


%% @doc Check state of the cluster
%% @private
check_status(running) ->
    Manager = ?env_manager(),
    case rpc:call(Manager,
                  leo_manager_mnesia, get_storage_nodes_all, []) of
        {ok, RetL} when length(RetL) == length(?storage_nodes) ->
            Nodes = [N || #node_state{node = N,
                                      state = ?STATE_RUNNING} <- RetL],
            case length(Nodes) == length(?storage_nodes) of
                true ->
                    check_status(gateway, length(?gateway_nodes), []);
                false ->
                    not_prepared
            end;
        _Other ->
            not_prepared
    end;
check_status(attached) ->
    Manager = ?env_manager(),
    NumOfStorages = length(?storage_nodes),

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
            check_status(attached);
        {ok, StorageNodes} ->
            case rpc:call(Manager, leo_manager_api, start, [null]) of
                ok ->
                    check_status(gateway, 1, StorageNodes);
                _ ->
                    ?msg_error("Could not launch LeoFS"),
                    halt()
            end
    end.

%% @doc Check state of the gateway
%% @private
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
