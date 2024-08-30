#!/bin/bash
clas12-workflow \
--model recqtlana \
--runGroup rgc \
--tag test \
--forties \
--debug \
--runs 016471,016472 \
--inputs /cache/clas12/rg-c/production/decoded/10.0.9/016471 \
--inputs /cache/clas12/rg-c/production/decoded/10.0.9/016472 \
--outDir /volatile/clas12/users/baltzell/wok-test \
--reconYaml ./yamls/test.yaml \
--trainYaml calib \
--coatjava 11.0.1 \
--denoise \
$@

