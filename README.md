# Utilities for working with dogstatsd


## Build
`cargo build --release`

## `dsd-cat`
This tool takes in data either as a dogstatsd v3 replay file or as raw utf-8 encoded text.
It will print out the contents of the dogstatsd messages either to a file or stdout.

Note for dogstatsd replay files, it ignores the other metadata such as timestamps and OOB data.

```
./target/release/dsd-cat --help
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
This tool takes in a stream of text dogstatsd messages either from a file or
from stdin. These can be zstd encoded, replay files, or utf-8 encoded text.
Prints out some basic histograms about the messages (metric name length, # of tags, etc)

```
./target/release/dsd-analyze --help
Analyze DogStatsD traffic messages

Usage: dsd-analyze [INPUT]

Arguments:
  [INPUT]  File containing dogstatsd data

Options:
  -h, --help     Print help
  -V, --version  Print version
```

