clas12-workflow \
--model decrecqtlana \
--runGroup rgk \
--tag test \
--debug \
--forties \
--runs 19891-19893 \
--inputs /mss/clas12/rg-k/data/clas_019891 \
--inputs /mss/clas12/rg-k/data/clas_019892 \
--inputs /mss/clas12/rg-k/data/clas_019893 \
--outDir /cache/hallb/scratch/baltzell/roc-test \
--workDir /volatile/clas12/users/baltzell/roc-test-work \
--reconYaml ~/clas12-default.yaml \
--trainYaml calib \
--coatjava 10.1.0 \
--denoise \
$@
