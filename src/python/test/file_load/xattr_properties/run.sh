main() {
    # parallel download
    dx-download-all-inputs --parallel
    # Install xattr
    python3 -m pip install -U xattr

    # creating some output results
    echo "ABCD" > dummy_data.txt
    mkdir -p out
    dirs="result1 result2 result3 result4"
    keys="key1 "

    for d in $dirs
    do
        mkdir -p out/$d
        cp dummy_data.txt out/$d/$d
        attr -s "key0" -V "val0" out/$d/$d
        attr -s "key1" -V "val1" out/$d/$d
        attr -s "runFolder" -V "runValue" out/$d/$d
    done

    # sequential upload with metadata as properties
    dx-upload-all-outputs --sequential --wait-on-close --xattr-properties
    compare_xattr_to_properties
    remove_uploaded_files

    # parallel upload with metadata as properties
    dx-upload-all-outputs --clearJSON=true --parallel --wait-on-close --xattr-properties
    compare_xattr_to_properties
}

function compare_xattr_to_properties()
{
    set -x
    files=$(find "out" -type f)
    for f in $files
    do
        echo "f=$f"
        basename=${f##*/}
        properties=$(dx describe --json $basename | jq -r .properties)
        [[ "val0" == $(echo $properties | jq -r .key0) ]]
        [[ "val1" == $(echo $properties | jq -r .key1) ]]
        [[ "runValue" == $(echo $properties | jq -r .runFolder) ]]
    done
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
