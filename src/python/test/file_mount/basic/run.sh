main() {
    echo "(wjk) starting..."

    dx-mount-all-inputs

    find /home/dnanexus/in -name \*

    cat /home/dnanexus/in/seq1/A.txt

    FILE1=$(cat /home/dnanexus/in/seq1/A.txt)
    echo wjktest FILE1 $FILE1

    if [ "$FILE1" != "1234\n" ]
    then
        echo "Failed to read correct data from mounted file."
        exit 1
    fi

    echo "(wjk) done!"
}
