#!/bin/bash
clas12-workflow \
--model decrecqtlana \
--runGroup rgk \
--tag test \
--forties \
--debug \
--runs 19891-19893 \
--inputs /mss/clas12/rg-k/data/clas_019891 \
--inputs /mss/clas12/rg-k/data/clas_019892 \
--inputs /mss/clas12/rg-k/data/clas_019893 \
--outDir /volatile/clas12/users/baltzell/wok-test \
--reconYaml ./yamls/test.yaml \
--trainYaml calib \
--coatjava 11.0.1 \
--denoise \
$@

