clas12-workflow \
--model decrecana \
--runGroup rgk \
--tag test \
--forties \
--debug \
--runs 19891-19893 \
--inputs /mss/clas12/rg-k/data/clas_019891 \
--inputs /mss/clas12/rg-k/data/clas_019892 \
--inputs /mss/clas12/rg-k/data/clas_019893 \
--outDir /volatile/clas12/users/baltzell/wok-test \
--reconYaml ~/clas12-default.yaml \
--trainYaml calib \
--coatjava 10.1.0 \
--denoise

