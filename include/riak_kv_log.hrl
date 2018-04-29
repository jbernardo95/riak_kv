-define(LOG, riak_kv_log).

-record(log_record, {
    type,
    payload
}). 
