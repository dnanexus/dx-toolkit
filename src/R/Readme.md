dxR: DNAnexus R API
===================

[API Documentation](http://autodoc.dnanexus.com/bindings/R/current/)

Build dependencies
------------------

* R (http://www.r-project.org/)
* R Packages:
    - RCurl
    - RJSONIO

Building and installing
-----------------------

You should install the `RCurl` and `RJSONIO` packages if you have not
already.  You can do so by running the following commands from within
R.

    > install.packages("RCurl")
    > install.packages("RJSONIO")

Next, run the following command from the command-line (outside of R)
to install the package from source.

    R CMD INSTALL dx-toolkit/src/R/dxR

You can use the `-l` flag to specify a local directory in which to
install the package if you do not have system permissions to install
it for all users.  If taking such an approach, you should set your
`R_LIBS` environment variable appropriately if you have not already.

    export R_LIBS='/home/username/path/to/dest/library'
    R CMD INSTALL dx-toolkit/src/R/dxR -l $R_LIBS

Using the package
-----------------

Once you have installed the library, you are ready to use it.  Inside
R, just run:

    library(dxR)
