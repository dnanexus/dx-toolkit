main() {
    # This should download into $home/in directory two subdirectories
    dx-download-all-inputs

    mkdir -p out/genes
    echo 'ls in/$ref' > out/genes/refs_ls.txt
    echo 'ls in/$reads' > out/genes/reads_ls.txt

    dx-upload-all-outputs
}
