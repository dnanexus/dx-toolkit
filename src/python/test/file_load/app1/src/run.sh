main() {
    dx-download-all-inputs

    echo "hello world, $seq1, $seq2" > report.txt
    mkdir -p out/result
    cp -f report.txt out/result/

    mkdir -p out/genes
    echo 'ls in/$ref' > out/genes/refs_ls.txt
    echo 'ls in/$reads' > out/genes/reads_ls.txt

    dx-upload-all-outputs
}
