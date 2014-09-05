main() {
    cmd_line_args=""
    if [ -n "$parallel" ]
    then
        cmd_line_args="--parallel"
    fi
    echo "cmd_line_args= $cmd_line_args"

    # download
    start_time=$(date +%s)
    dx-download-all-inputs $cmd_line_args
    end_time=$(date +%s)
    echo "Download took $(($end_time - $start_time)) seconds"

    files=$(find "in" -type f)

    # creating dummy outputs
    mkdir -p out/results
    counter=0
    for i in $files
    do
        cp $i out/results/$counter
        counter=$[$counter +1]
    done

    # parallel upload
    start_time=$(date +%s)
    dx-upload-all-outputs $cmd_line_args
    end_time=$(date +%s)
    echo "Upload took $(($end_time - $start_time)) seconds"
}

