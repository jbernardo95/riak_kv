%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 Basho Technologies, Inc.  All Rights Reserved.
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
%% -------------------------------------------------------------------

-module(riak_kv_group_keys_response).

-export([new_response/3,
         get_metadatas/1,
         get_common_prefixes/1,
         is_truncated/1
        ]).

-export_type([response/0]).

-record(response, {
          metadatas :: list(),
          common_prefixes :: list(binary()),
          is_truncated :: boolean()
         }).

-opaque response() :: #response{}.

-spec new_response(Metadatas::list(), CommonPrefixes::list(binary()), IsTruncated::boolean) ->
    response().
new_response(Metadatas, CommonPrefixes, IsTruncated) ->
    #response {
       metadatas = Metadatas,
       common_prefixes = CommonPrefixes,
       is_truncated = IsTruncated
      }.

-spec get_metadatas(response()) -> list().
get_metadatas(#response{metadatas = Metadatas}) -> Metadatas.

-spec get_common_prefixes(response()) -> list().
get_common_prefixes(#response{common_prefixes = CommonPrefixes}) -> CommonPrefixes.

-spec is_truncated(response()) -> boolean().
is_truncated(#response{is_truncated = IsTruncated}) -> IsTruncated.