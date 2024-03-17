clas12-workflow \
--model decrecana \
--runGroup rgk \
--tag test \
--forties \
--runs 19891-19893 \
--inputs /mss/clas12/rg-k/data/clas_019891 \
--inputs /mss/clas12/rg-k/data/clas_019892 \
--inputs /mss/clas12/rg-k/data/clas_019893 \
--outDir /volatile/clas12/users/baltzell/wok-test \
--reconYaml /home/baltzell/clas12-config/coatjava/10.0.5/rgk_data-cv-calib.yaml \
--trainYaml calib \
--coatjava 10.0.9 \
--denoise \
--el9

