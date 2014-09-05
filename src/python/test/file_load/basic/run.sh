main() {
    dx-download-all-inputs

    # Check file content
    dx download "$seq1" -o seq1
    diff seq1 in/seq1/* # seq1_filepath should be set to in/seq1/A.txt (if the file is called A.txt)
    dx download "$seq2" -o seq2
    diff seq2 in/seq2/*

    # check file content for arrays
    var=0
    echo "Checking file arrays"
    mkdir -p ref
    echo "ref=$ref"

    for i in "${ref[@]}"
    do
        mkdir -p ref/$var
        dx download "$i" -o ref/$var/
        var=$((var+1))
    done
    diff -r ref in/ref

    mkdir -p out/result
    echo "hello world, $seq1, $seq2" > out/result/report.txt

    mkdir -p out/genes
    echo 'ls in/$ref' > out/genes/refs_ls.txt
    echo 'ls in/$reads' > out/genes/reads_ls.txt

    mkdir -p out/foo
    echo "ABC" > out/foo/X_1.txt
    for i in 2 3 4 5 6 7 8;
    do
        cp out/foo/X_1.txt out/foo/X_$i.txt
    done

    dx-upload-all-outputs
}
