## import dxpy
#
#
#@dxpy.entry_point('main')
#def main(**kargs):
#    print "Hello, DNAnexus!"
#    return {}

#import dxpy, subprocess
#
#@dxpy.entry_point('main')
#def main(seq1, seq2):
#    dxpy.download_dxfile(seq1, "seq1.fasta")
#    dxpy.download_dxfile(seq2, "seq2.fasta")
#    
#    subprocess.call("blast2 -p blastn -i seq1.fasta -j seq2.fasta > report.txt", shell=True)
#    
#    report = dxpy.upload_local_file("report.txt")
#    return {"blast_result": report}

main() {

    ## Under development
    #dx-download-all-inputs

    #dx-download-input "$seq1"
    #dx-download-input "$seq2"

    ## Under development
    #dx-download-all-inputs --except seq1
    #dx-download-all-inputs --except seq2

    # The following line(s) use the dx command-line tool to download your file
    # inputs to the local file system using variable names for the filenames. To
    # recover the original filenames, you can use the output of "dx describe
    # "$variable" --name".
    #dx download "$seq1" -o seq1.fasta
    #dx download "$seq2" -o seq2.fasta

    #blast2 -p blastn -i seq1.fasta -j seq2.fasta > report.txt
    echo "hello world, $seq1, $seq2" > report.txt
    mkdir -p out/blast_result
    cp -f report.txt out/blast_result/

    # The following line(s) use the dx command-line tool to upload your file
    # outputs after you have created them on the local file system.  It assumes
    # that you have used the output field name for the filename for each output,
    # but you can change that behavior to suit your needs.  Run "dx upload -h"
    # to see more options to set metadata.

    #output_file=$(dx upload report.txt --brief)

    # The following line(s) use the utility dx-jobutil-add-output to format and
    # add output variables to your job's output as appropriate for the output
    # class.  Run "dx-jobutil-add-output -h" for more information on what it
    # does.

    #dx-jobutil-add-output "blast_result" "$output_file" --class=file

    #dx-upload-all-outputs

    dx-download-all-inputs

    echo "hello world, $seq1, $seq2" > report.txt
    mkdir -p out/result
    cp -f report.txt out/result/

    dx-upload-all-outputs
}
