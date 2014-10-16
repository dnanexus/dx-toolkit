main() {
#    dx-download-all-inputs

    gene_d="out/genes"
    mkdir -p $gene_d
    echo 'ABCD' > $gene_d/A.txt
    echo '1234' > $gene_d/B.txt

    # create a few subdirectories with data
    mkdir -p $gene_d/clue
    for i in 1 2 3; do
        echo "ABC" > $gene_d/clue/X_$i.txt
    done
    mkdir -p $gene_d/hint
    for i in 1 2 3; do
        echo "hint$i" > $gene_d/hint/V_$i.txt
    done

    # another subdirectory
    phen_d="out/phenotypes"
    mkdir -p $phen_d
    echo 'hello world' > $phen_d/C.txt

    mkdir -p $phen_d/clue2
    for i in 1 2 3; do
        echo "ACGT" > $phen_d/clue2/Y_$i.txt
    done
    mkdir -p $phen_d/hint2
    for i in 1 2 3; do
        echo "ACGT_$i" > $phen_d/hint2/Z_$i.txt
    done

    # nested hierarchy
    mkdir -p out/report/foo/bar
    echo "Luke, behind you!" > out/report/foo/bar/luke.txt

    # single file
    mkdir -p out/helix
    echo "12345678" > out/helix/num_chrom.txt

    # hidden files, directories, and symbolic links. These should be skipped
    mkdir $gene_d/.hide
    echo "XYZ" > $gene_d/.hide/V.txt
    echo "1234" > $gene_d/.hidden_file
    ln -s $gene_d/clue/X_1.txt $phen_d/clue_symlink
    ln -s $gene_d/hint/V_1.txt $phen_d/hint_symlink

    dx-upload-all-outputs

    #  Check that directory structure was copied
    #  correctly to the project space
    mkdir space
    pushd space
    dx download --overwrite --recursive "/"
    popd
    mkdir tmp
    mkdir tmp/genes
    mv space/{A.txt,B.txt,hint,clue} tmp/genes/
    mkdir tmp/phenotypes
    mv space/{C.txt,hint2,clue2} tmp/phenotypes
    mkdir -p tmp/report
    mv space/foo tmp/report/
    mkdir -p tmp/helix
    mv space/num_chrom.txt tmp/helix/

    # check that hidden files and symbolic links were skipped
    # the space directory should now be empty
    extra_files=$(find space)
    if [ $extra_files != "space" ]
    then
        echo "extra files have been uploaded: <$extra_files>"
        exit 1
    fi

    # remove the hidden files and symbolic links, so that a recursive diff
    # will work
    find out -name ".hid*" | xargs rm -rf
    find out -name "*symlink" | xargs rm -rf

    DIFF=$(diff -r --brief tmp out)
    if [ "$DIFF" != "" ]
    then
        echo "Upload of subdirectories does not work properly"
        exit 1
    fi
}
