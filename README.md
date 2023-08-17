# Utilities for working with dogstatsd

- [replay-parser](./replay-parser)
    - This tool will take in a dogstatsd replay file and parse it into raw
      dogstatsd text messages.
- [stats-parser](./stats-parser)
    - This tool takes in dogstatsd text messages and emits some basic statistics
      about aspects of the dogstatsd messages (how many values, etc)

