-define(LOG, riak_kv_log).
-define(LOG_CACHE, riak_kv_log_cache).

-record(log_record, {
    type,
    payload
}). 
