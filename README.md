# Utilities for working with dogstatsd


## Build
`cargo build --release`

## `replaydump`
This tool takes in dogstatsd replay files and dumps out the raw dogstatsd
messages contained. It ignores other metadata such as the timestamps and any OOB
UDS data.

```
./target/debug/replaydump
Usage: ./target/debug/replaydump <file_path>
```

## `msgstats`
This tool takes in a stream of text dogstatsd messages either from a file or
from stdin. It will process them and report out a summary at the end of how many
messages there were, how many tags they had, how many multi-values they had etc.

```
cat my-data-file | ./target/debug/stats
```

