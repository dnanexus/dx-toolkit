A test for deep directories in the output directories. Such directories
should be uploaded as subdirectories in the project workspace. For example,
if "genes" is an output key, then:
    out/genes/{X1, X2}.txt
will be copied to:
    /{X1, X2}.txt

If the user creates a subdirectory like:
    out/genes/clue/{B1, B2, B3}.txt
It will be copied to the project space at:
    /clue/{B1, B2, B3}.txt
