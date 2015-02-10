#!/bin/bash
#
# App expects at least two files in *files* (the input file array)
#
# Assertions to make about the job's output after it is done running:
# - *first_file* is a file named first_file.txt containing the string:
#     "contents of first_file"
# - *final_file* is a file named final_file.txt containing the
#   *concatenation of the two input files in *files*

main() {
    # This entry point has an input and output spec that apply
    dx-download-all-inputs

    echo "Value of files: '${files[@]}'"

    for i in ${!files[@]}
    do
        process_jobs[$i]=$(dx-jobutil-new-job process -iprocess_input:file="${files[$i]}")
    done

    postprocess=$(dx-jobutil-new-job postprocess -ipp_input0:file="${process_jobs[0]}":p_output -ipp_input1:file="${process_jobs[1]}":p_output)

    # make some file here to upload as an output
    mkdir -p out/first_file
    echo "contents of first_file" > out/first_file/first_file.txt

    # This should communicate where the final_file is going to come from
    dx-jobutil-add-output final_file "$postprocess":final_file --class=jobref

    # This should upload first_file which is ready
    dx-upload-all-outputs
}

process() {
    # This entry point has neither an input nor an output spec
    dx-download-all-inputs

    # Check file content
    dx download "$process_input" -o process_input
    diff process_input in/process_input/*

    mkdir -p out/p_output
    cp in/process_input/* out/p_output/

    dx-upload-all-outputs --wait-on-close
}

postprocess() {
    # This entry point has neither an input nor an output spec
    dx-download-all-inputs

    # Check file content
    dx download "$pp_input0" -o pp_input0
    diff pp_input0 in/pp_input0/*
    dx download "$pp_input1" -o pp_input1
    diff pp_input1 in/pp_input1/*

    mkdir -p out/final_file
    cat in/pp_input0/* >> out/final_file/final_file.txt
    cat in/pp_input1/* >> out/final_file/final_file.txt

    dx-upload-all-outputs --wait-on-close
}
