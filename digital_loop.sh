#!/bin/bash

while IFS= read -r line; do
  ./digital_load_parquet.sh $line
done < "$1"
