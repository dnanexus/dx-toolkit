main() {
    # sequential download
    dx-download-all-inputs --sequential
    input_dir="in"
    mkdir tmp
    mv $input_dir tmp

    # parallel download
    dx-download-all-inputs --parallel

    # compare and check that the two download methods are the same
    DIFF=$(diff -r $input_dir tmp/$input_dir)
    if [ "$DIFF" != "" ]
    then
        echo "Download sequential and parallel differ"
        exit 1
    fi
    /bin/rm -rf tmp

    # creating some output results
    echo "ABCD" > dummy_data.txt
    mkdir -p out
    dirs="result1 result2 result3 result4"

    for d in $dirs
    do
        mkdir -p out/$d
        cp dummy_data.txt out/$d/$d
    done

    # sequential upload
    dx-upload-all-outputs --sequential
    compare_upload_to_outdir
    remove_uploaded_files

    # parallel upload
    dx-upload-all-outputs --clearJSON=true --parallel
    compare_upload_to_outdir
}

# download files uploaded from the "out" directory, and compare
# against the originals
function compare_upload_to_outdir() 
{
    files=$(find "out" -type f)

    mkdir -p tmp
    for f in $files
    do
        basename=${f##*/}

        # ensure the file is closed before downloading it
        dx wait "$basename"
        dx download "$basename" -o tmp/"$basename"
        DIFF=$(diff out/$basename/$basename tmp/$basename)
        if [ "$DIFF" != "" ]
        then
            echo "Error in upload, file $basename is incorrect"
            exit 1
        fi
    done

    rm -rf tmp
}


function remove_uploaded_files() 
{
    files=$(find "out" -type f)

    for f in $files
    do
        echo "f=$f"
        basename=${f##*/}

        dx rm $basename
    done
}
