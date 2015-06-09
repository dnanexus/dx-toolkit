main() {
    dx-download-all-inputs

    mkdir -p out/seq2

    if [[ "$create_seq3" == "true" ]];
    then
        mkdir -p out/seq3
        echo "abcd-$create_seq3" > out/seq3/X.txt
    fi

    dx-upload-all-outputs
}
