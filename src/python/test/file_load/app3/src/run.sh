main() {
    dx-download-input "$seq1"
    dx-download-input "$seq2"

    #dx-download-all-inputs --except seq1
    #dx-download-all-inputs --except seq2

    echo "hello world, $seq1, $seq2" > report.txt
    mkdir -p out/blast_result
    cp -f report.txt out/blast_result/

    echo "hello world, $seq1, $seq2" > report.txt
    mkdir -p out/result
    cp -f report.txt out/result/

    dx-upload-all-outputs
}
