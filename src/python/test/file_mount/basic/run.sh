main() {
    echo "(wjk) starting..."

    dx-mount-all-inputs

    #DIFF=$(diff -r --brief tmp out)
    #if [ "$DIFF" != "" ]
    #then
    #    echo "Upload of subdirectories does not work properly"
    #    exit 1
    #fi

    find /home/dnanexus/in -name \*

    exit 1

    echo "(wjk) done!"
}
