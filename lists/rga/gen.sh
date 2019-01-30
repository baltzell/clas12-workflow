#!/bin/bash

inp=/mss/clas12/rg-a/data
out=/cache/mss/clas12/rg-a/production/decoded/6b.0.0
wrk=/volatile/clas12/rga/production/decoding/6b.0.0

for flow in `seq 9`
do
  echo \
  ./scripts/gen-decoding.py --runGroup rga \
  --workflow rga-decode$flow --runFile ./lists/rga/groups-50-golden/runfile_g$flow.txt \
  --phaseSize 1300 --mergeSize 10 \
  --inputs $inp --outDir $out --workDir $wrk
done

