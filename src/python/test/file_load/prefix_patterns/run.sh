main() {
    dx-download-all-inputs

    check_var "seq1_prefix" "$seq1_prefix" "A"
    check_var "seq2_prefix" "$seq2_prefix" "A"
    check_var "gene_prefix" "$gene_prefix" "A"
    check_var "map_prefix"  "$map_prefix"  "A.foo"
    check_var "map2_prefix" "$map2_prefix" "fooxxx"
    check_var "map3_prefix" "$map3_prefix" "A"
    check_var "map4_prefix" "$map4_prefix" "A"
    check_var "multi_prefix" "$multi_prefix" "x13year23"

    dx-upload-all-outputs
}

# Check if an environment variable is defined, and if it has the correct value
check_var() {
    if [[ $# -ne 3 ]];
    then
        echo "Error: check_var expects three inputs, but got $#"
        dx-jobutil-report-error "check_var expects three inputs, but got $#" "AppError"
        exit 1
    fi

    if [[ ! "$2" == "$3" ]];
    then
        echo "Error: expecting $2 to equal $3 (var=$1)"
        dx-jobutil-report-error "expecting $2 to equal $3 (var=$1)" "AppError"
        exit 1
    fi
}


