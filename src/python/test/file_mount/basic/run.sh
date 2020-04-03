main() {
    # Simple app test to invoke dx-mount-all-inputs and
    # verify that the data can be accessed from the
    # mounted file.

    dx-mount-all-inputs

    find /home/dnanexus/in -name \*

    FILE=$(cat /home/dnanexus/in/seq1/A.txt)
    if [ "$FILE" != "1234" ]
    then
        echo "Failed to read correct data from mounted file."
        exit 1
    fi

    FILE=$(cat /home/dnanexus/in/seq2/A.txt)
    if [ "$FILE" != "ABCD" ]
    then
        echo "Failed to read correct data from mounted file."
        exit 1
    fi

    FILE=$(cat /home/dnanexus/in/ref/0/A.txt)
    if [ "$FILE" != "1234" ]
    then
        echo "Failed to read correct data from mounted file."
        exit 1
    fi

    FILE=$(cat /home/dnanexus/in/ref/1/B.txt)
    if [ "$FILE" != "ABCD" ]
    then
        echo "Failed to read correct data from mounted file."
        exit 1
    fi
}
