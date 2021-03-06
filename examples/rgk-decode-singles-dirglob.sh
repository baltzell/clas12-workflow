./scripts/clas12-workflow.py \
    --runGroup rgk \
    --tag v0 \
    --model dec \
    --runs 5933-5944 \
    --inputs '/mss/clas12/rg-k/data/clas_00593*' \
    --fileRegex '.*clas[_A-Za-z]*_(\d+)\.evio\.(0*[01]\d\d$)' \
    --outDir /volatile/clas12/rgk/test

