# riak_kv

This is a fork of [riak_kv](https://github.com/basho/riak_kv) altered to have the functionality of EunomiaKV Plus. 

## Dependencies

- Erlang R16B02
- GNU-style build system to compile and run =riak_kv=

## Compilation 

```
# Install dependencies and compile project
$ make compile 

# Clean build files 
$ make clean
```

## Configuration

`riak_kv` buckets must be configured in the following way:

```
buckets.default.n_val = 1
buckets.default.r = 1
buckets.default.w = 1
```
