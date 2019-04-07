./scripts/gen-decoding.py \
    --config examples/rga-decode-singles.json \
    --tag v1 \
    --run 6688,6689 \
    --inputs /mss/clas12/rg-a/data/clas_00668[89] \
    --outDir /volatile/clas12/baltzell/test2 \
    --fileRegex .*clas[_A-Za-z]*_(\d+)\.evio\.(0*[01]\d\d$)

