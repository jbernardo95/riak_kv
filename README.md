# riak_kv

This is a fork of [riak_kv](https://github.com/basho/riak_kv) altered to have the functionality of EunomiaKV Plus. 

## Dependencies

- Erlang R16B02
- GNU-style build system

## Compilation 

```
# Install dependencies and compile project
$ make compile 

# Clean build files 
$ make clean
```

## Read Log Content

In order to read the content of the log use the `scripts/read_log` script:

```
$ scripts/read_log data_path max_n_bytes max_n_files
```
