clas12-workflow.py \
--model recana \
--runGroup rga \
--tag test \
--runs 5196 \
--inputs /mss/clas12/rg-a/production/decoded/6b.2.0/005196/ \
--outDir /volatile/clas12/users/baltzell/wok-test \
--reconYaml ./examples/test.yaml \
--trainYaml calib \
--coatjava 8.4.0 \
--forties \
--nevents 5000

