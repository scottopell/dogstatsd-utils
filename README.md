# Utilities for working with dogstatsd


## Build
`cargo build --release`

## `dsd-cat`
> Install via `cargo install --git https://github.com/scottopell/dogstatsd-utils --bin dsd-cat`

This tool takes in data either as a dogstatsd v3 replay file or as raw utf-8 encoded text.
It will print out the contents of the dogstatsd messages either to a file or stdout.

Note for dogstatsd replay files, it ignores the other metadata such as timestamps and OOB data.

```
$ dsd-cat --help
Take data from the specified input file and write it either to stdout or to a specified file. Data can be raw utf-8 text or a dogstatsd-replay file, optionally zstd encoded

Usage: dsd-cat [OPTIONS] [INPUT]

Arguments:
  [INPUT]  File containing dogstatsd data

Options:
  -o, --output <OUTPUT>  Where output dogstatsd messages should go
  -h, --help             Print help
  -V, --version          Print version
```

## `dsd-analyze`
> Install via `cargo install --git https://github.com/scottopell/dogstatsd-utils --bin dsd-analyze`

This tool takes in a stream of text dogstatsd messages either from a file or
from stdin. These can be zstd encoded, replay files, or utf-8 encoded text.
Prints out some basic histograms about the messages (metric name length, # of tags, etc)

```
$ dsd-analyze --help
Analyze DogStatsD traffic messages

Usage: dsd-analyze [INPUT]

Arguments:
  [INPUT]  File containing dogstatsd data

Options:
  -h, --help     Print help
  -V, --version  Print version
```

## `dsd-generate`
> Install via `cargo install --git https://github.com/scottopell/dogstatsd-utils --bin dsd-generate`

This tool utilizes the Lading dogstatsd payload generator
[(src)](https://github.com/DataDog/lading/blob/main/lading_payload/src/dogstatsd.rs)
to generate a configurable number of dogstatsd messages.

This can be useful to feed into a UDP or UDS socket via socat, eg:
`dsd-generate --rate 10hz --metric-types=sketch,count | socat STDIN UNIX-SENDTO:/tmp/dsd.sock`

```
$ dsd-generate --help
Generate random dogstatsd messages and emit them to stdout line-by-line. If no options are specified, then it will emit a single message and exit

Usage: dsd-generate [OPTIONS]

Options:
  -n, --num-msgs <NUM_MSGS>          Emit this finite amount of msgs
      --num-contexts <NUM_CONTEXTS>  Emit this number of unique contexts
      --metric-types <METRIC_TYPES>  metric_types is optional and if specified will emit only metrics of the given types
  -r, --rate <RATE>                  Rate can be specified as throughput (ie, bytes per second) or time (ie 1hz) eg '1kb' or '10 hz'
  -o, --output <OUTPUT>              Where output dogstatsd messages should go
  -h, --help                         Print help
  -V, --version                      Print version
```
