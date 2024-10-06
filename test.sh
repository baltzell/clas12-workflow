#!/bin/bash
clas12-workflow \
--model decrecqtlana \
--runGroup rgk \
--tag test \
--forties \
--debug \
--runs 19216,19215 \
--inputs /cache/clas12/rg-k/data/clas_019215 \
--inputs /cache/clas12/rg-k/data/clas_019216 \
--outDir /volatile/clas12/users/baltzell/wok-test \
--reconYaml ./yamls/test.yaml \
--trainYaml calib \
--coatjava 11.0.1 \
--denoise \
$@

