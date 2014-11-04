main() {
    dx-download-all-inputs

    # compare the bash variables to the old version of the code
    dx-print-bash-vars --dbg-compare-old > BASH_VAR_ERRS

    ## Checking file type
    check_var_defined "$seq1"

    check_var_defined "$seq2"

    ## Checking array:file type
    check_array_var_defined "$genes"

    # TODO: uncomment this section and remove this line after the next dx-toolkit deployment"
    #check_var_defined "$seq1_name" "A.txt"
    #check_var_defined "$seq1_path" "\$HOME/in/seq1/A.txt"
    #check_var_defined "$seq1_prefix" "A"
    #check_var_defined "$seq2_name" "A.txt"
    #check_var_defined "$seq2_path" "\$HOME/in/seq2/A.txt"
    #check_var_defined "$seq2_prefix" "A"
    #check_array_var_defined "$genes_name" "( A.txt A.txt )"
    #check_array_var_defined "$genes_path" "( \$HOME/in/genes/A.txt \$HOME/in/genes/A.txt )"
    #check_array_var_defined "$genes_prefix" "( A A )"

    dx-upload-all-outputs
}

# Check if an environment variable is defined, and if it has the correct value
check_var_defined() {
    if [[ -z $1 ]];
    then
        echo "Error: expecting environment variable $1 to be defined"
        dx-jobutil-report-error "Error: expecting environment variable $1 to be defined" "AppError"
        exit 1
    fi

    if [ $# -ne 3 ];
    then
        return
    fi

    if [[ ! "$1" == "$2" ]];
    then
        echo "Error: expecting environment variable $1 to equal $2"
        dx-jobutil-report-error "Error: expecting environment variable $1 to equal $2" "AppError"
        exit 1
    fi
}


# The same, for a variable that is supposed to take on an array value
check_array_var_defined() {
    if [[ -z $1 ]];
    then
        echo "Error: expecting environment variable $1 to be defined"
        dx-jobutil-report-error "Error: expecting environment variable $1 to be defined" "AppError"
        exit 1
    fi

    if [ $# -ne 3 ];
    then
        return
    fi

    if [ ! cmp_string_arrays $1 $2 ];
    then
        echo "Error: expecting environment variable $1 to equal $2"
        dx-jobutil-report-error "Error: expecting environment variable $1 to equal $2" "AppError"
        exit 1
    fi
}

# Compare two arrays, return 1 if they are equal, 0 otherwise
cmp_string_arrays() {
    if [ $# -ne 2 ];
    then
        echo "cmp_string_arrays requires two input arguments"
        exit 1
    fi

    local a=$1
    local len_a=${#a[@]}
    local b=$2
    local len_b=${#b[@]}

    if [ $len_a -ne $len_b ];
    then
        return 0
    fi

    for (( i=0; i<${len_a}; i++ ));
    do
        if [ ${a[$i]} != ${b[$i]} ];
        then
            return 0
        fi
    done

    return 1
}
