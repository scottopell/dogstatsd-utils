#!/bin/bash

echo "Paste the byte array and press Ctrl-D when done:"

# Read multi-line input from stdin
INPUT=$(cat)

# Remove commas, spaces, newlines and convert hex to binary
echo "$INPUT" | tr -d '[:space:],' | xxd -r -p > output.bin

echo "Binary file created as output.bin"
