%% -------------------------------------------------------------------
%%
%% riak_kv_aaefold: Type definitions for aae_folds
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

-module(riak_kv_aaefold).

%% Building blocks for supported aae fold query definitions
-type segment_filter() :: list(non_neg_integer()).
-type tree_size() :: xxsmall|xsmall|small|medium|large|xlarge.
-type branch_filter() :: list(non_neg_integer()).
-type key_range() :: {riak_object:key(), riak_object:key()}|all.
-type bucket() :: riak_object:bucket().
-type n_val() :: pos_integer().
-type modified_range() :: {date, non_neg_integer(), non_neg_integer()}.
    %% dates in modified_range are 32bit integer timestamp of seconds
    %% since unix epoch
-type hash_method() :: pre_hash|{rehash, non_neg_integer()}.
    %% clocks are pre-hashed before storage to reduce CPU load for hash
    %% comparisons.  However, there may be be hash collisions, and in this case
    %% it may be periodically required to use an alternate hash.  For this
    %% {rehash, non_neg_integer()} is used whereby the integer concatenated
    %% with the hash
-type change_method() :: {job, pos_integer()}|local|count.
    %% When reaping tombstones (or erasing keys) the reap/erase can either
    %% be actioned only by a job-specific riak_kv_reaper/eraser process started
    %% by this FSM.  Or each fold can send reap/delete requests direct to the
    %% local node's riak_kv_reaper/riak_kv_eraser to distribute the load across
    %% the cluster and increase parallelisation of the process.
    %% The count change_method() will perform no reaps/deletes - but will
    %% simply count the matching keys - this is cheaper than running
    %% find_tombs/find_keys to accumulate/sort a large list for counting. 

-type nval_queries() ::
    % N-val AAE (using cached trees)
    {merge_root_nval, n_val()}|
        % Merge the roots of cached Tictac trees for the given n-val to give
        % a single root for the cluster.  This should be a fast, low-overhead
        % operation
    {merge_branch_nval, n_val(), branch_filter()}|
        % Merge a selection of branches of cached Tictac trees for the given
        % n-val to give a combined view of those branches across the cluster.
        % This should be a fast, low-overhead operation
    {fetch_clocks_nval, n_val(), segment_filter()}|
    {fetch_clocks_nval, n_val(), segment_filter(), modified_range()}.
        % Scan over all the keys for a given n_val in the tictac AAE key store
        % (which for native stores will be the actual key store), skipping 
        % those blocks of the store not containing keys in the segment filter,
        % returning a list of keys and clocks for that n_val within the
        % cluster.  This is a background operation, but will have lower 
        % overheads than traditional store folds, subject to the size of the
        % segment filter being small - ideally o(10) or smaller
        % Variant supported with a modified range, which will be converted into
        % a fetch_clocks_range

-type range_queries() ::
    % Range-based AAE (requiring folds over native/parallel AAE key stores)
    {merge_tree_range, 
        bucket(),
        key_range(), 
        tree_size(),
        {segments, segment_filter(), tree_size()}|all,
        modified_range()|all,
        hash_method()}|
        % Provide the values for a subset of AAE tree branches for the given
        % key range.  This will be a background operation, and the cost of
        % the operation will be in-proportion to the number of keys in the
        % range, depending on the filter applied
        %
        % Different size trees can be requested.  Smaller tree sizes are more
        % likely to lead to false negative results, but are more efficient
        % to calculate and have a reduced load on the network
        % 
        % A segment_filter() may be passed.  For example, if a tree comparison
        % has been done between two clusters, it might be preferable to confirm
        % the differences before fetching clocks. This can be done by
        % requesting a second tree but placing the mismatched segments into a
        % segment filter so that the subsequent comparison will be made just on
        % those segments.  This will reduce the cost of producing the tree by
        % an order of magnitude.
        %
        % A modified_range() may be passed.  This will calculate the tree based
        % only on the keys which were last modified within the range.  If the
        % subset of keys above the low date in the range is small relative to
        % the overall key space in the range - then this will reduce the cost
        % of producing the tree by an order of magnitude.
        %
        % There exists the possibility of a hash collision with the 32-bit
        % hashes use - i.e. the same key in two stores has different values
        % that both hash to the same hash.  This is a 1 in 4 billion  chance,
        % for an occurrence of a replication failure - so the risk of this
        % being a relevant issue depends on the number of replication failures
        % expected over the lifetime of a cluster pair.
        %
        % Hash collisions are probably not a significant risk in the general
        % context of eventual consistency, however, there is protection
        % provided through the ability to set the hash algorithm to be used
        % when hashing the vector clocks to produce the tree.
        % See `hash_function/1` for implementation details of the options,
        % which are either:
        % - pre_hash (use the default pre-calculated hash)
        % - {rehash, IV} rehash the vector clock concatenated with an integer
    {fetch_clocks_range, 
        bucket(),
        key_range(), 
        {segments, segment_filter(), tree_size()} | all,
        modified_range() | all}|
        % Return the keys and clocks in the given bucket and key range.
        % There are two filters that may be applied to the results:
        % - A segment filter to be used after a tree comparison has shown that
        % a manageable subset of segments is mismatched.  There is a limit on
        % the number of segments which may be passed (to ensure the query is
        % relatively efficient.
        % - A modified date filter as in merge_tree_range
        %
        % Care should be taken when using this feature if TictacAAE is running
        % in parallel mode with the leveled_so backend (not the leveled_ko)
        % backend.  If no segment_filter of modified_range is provided, the
        % whole store will be scanned. The leveled_ko backend should be used
        % for parallel TictacAAE key stores if range-type folds are to be run.
        %
        % Large result sets (e.g. o(100K) keys may cause issues with the size
        % of the result set.  It is currently an application responsibility to
        % control the size of the result set by use of the filter options
        % available.
        %
        % TODO - loose_limit()
        %
        % The leveled backend supports a max_key_count which could be used to
        % provide a loose_limit on the results returned.  However, there are
        % issues with this and segment_ordered backends, as well as extra 
        % complexity curtailing the results (and signalling the results are
        % curtailed).  The main downside of large result sets is network over
        % use.  Perhaps compressing the payload may be a better answer?
    {repl_keys_range, 
        bucket(),
        key_range(), 
        modified_range() | all,
        riak_kv_replrtq_src:queue_name()}|
        % Replicate all the objects in a given key and modified range.  By
        % sending references to each object to the given queue_name which
        % should have been pre-configured within the riak_kv_replrtq_src on
        % each node.
        % If the queue name is not configured, the work will complete without
        % any positive outcome.
        % This is expected to be used when transitioning buckets between
        % clusters, and also when repairing a cluster from a known outage in
        % real-time repl (utilising a modified range)
    {repair_keys_range,
        bucket(),
        key_range(),
        modified_range() | all,
        all}.
        % Read repair all keys in the range.  Keys will be read in batches
        % and then queued for repair
        % Will default to repairing all keys (i.e. all of those fetched and a
        % delta is discovered).  Scope to support not_in_coverage later - i.e.
        % only attempt to read those keys where a primary vnode is not
        % participating in coverage

-type operations_queries() ::
    % Operational support functions
    {find_keys, 
        bucket(),
        key_range(),
        modified_range() | all,
        {sibling_count, pos_integer()}|{object_size, pos_integer()}}|
        % Find all the objects in the key range that have more than
        % the given count of siblings (where {sibling_count, 1} means
        % find all objects with more than a single, unconflicted
        % value), or are bigger than the given object size.  This uses
        % the AAE keystore, and will only discover siblings that have
        % been generated and stored within a vnode (which should
        % eventually be all siblings given AAE is enabled and if
        % allow_mult is true). If finding keys by size, then the size
        % is the pre-calculated size stored in the aae key store as
        % metadata.
        %
        % The query returns a list of [{Key, SiblingCount}] tuples or 
        % [{Key, ObjectSize}] tuples depending on the filter requested.  The 
        % cost of this operation will increase with the size of the range
        % 
        % It would be beneficial to use the results of object_stats (or 
        % knowledge of the application) to ensure that the result size of
        % this query is reasonably bounded (e.g. don't set too low an object
        % size).  If only interested in the outcome of recent modifications,
        % use a modified_range().
    {object_stats, bucket(), key_range(), modified_range() | all}|
        % Returns:
        % - the total count of objects in the key range
        % - the accumulated total size of all objects in the range
        % - a list [{Magnitude, ObjectCount}] tuples where Magnitude represents
        % the order of magnitude of the size of the object (e.g. 1KB is objects 
        % from 100 bytes to 1KB, 10KB is objects from 1KB to 10KB etc)
        % - a list of [{SiblingCount, ObjectCount}] tuples where Sibling Count
        % is the number of siblings the object has.
        % - sample portion - (n_val * sample_size) / ring_size
        % e.g.
        % [{total_count, 1000}, 
        %   {total_size, 1000000}, 
        %   {sizes, [{1, 800}, {2, 180}, {3, 20}]}, 
        %   {siblings, [{1, 1000}]}]
        %
        % If only interested in the outcome of recent modifications,
        % use a modified_range().
    {find_tombs,
        bucket(),
        key_range(), 
        {segments, segment_filter(), tree_size()} | all,
        modified_range() | all}|
        % Find all tombstones in the range that match the criteria, and
        % return a list of keys and delete_hashes
    {reap_tombs,
        bucket(),
        key_range(),
        {segments, segment_filter(), tree_size()} | all,
        modified_range() | all,
        change_method()}|
        % Reap all the tombstones in the range using either a job-specific
        % reaper process, or using the process on each node (local to each
        % vnode fold).  Should return a count of all the tombstones for
        % which a reap request was made
    {erase_keys,
        bucket(),
        key_range(),
        {segments, segment_filter(), tree_size()} | all,
        modified_range() | all,
        change_method()}|
        % Erase keys using a riak_kv_eraser.  This is of specific use when
        % expiring keys beyond a certain modified date
    {list_buckets, n_val()}.
        % List all buckets in the aae store - assuming a given n_val

-type query_definition() ::
    % Use of these folds depends on the Tictac AAE being enabled in either
    % native mode, or in parallel mode with key_order being used.  
    nval_queries() | range_queries() | operations_queries().

-export_type([query_definition/0, hash_method/0]).