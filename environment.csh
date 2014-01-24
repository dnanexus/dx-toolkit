setenv DNANEXUS_HOME "`dirname $0`"

setenv PATH "$DNANEXUS_HOME/bin:$PATH"

if $?PYTHONPATH then
    setenv PYTHONPATH "$DNANEXUS_HOME/share/dnanexus/lib/python2.7/site-packages:$DNANEXUS_HOME/lib/python:$PYTHONPATH"
else
    setenv PYTHONPATH "$DNANEXUS_HOME/share/dnanexus/lib/python2.7/site-packages:$DNANEXUS_HOME/lib/python"
endif

if $?PERL5LIB then
    setenv PERL5LIB "$DNANEXUS_HOME/lib/perl5:$PERL5LIB"
else
    setenv PERL5LIB "$DNANEXUS_HOME/lib/perl5"
endif

if $?CLASSPATH then
    setenv CLASSPATH "$DNANEXUS_HOME/lib/java/*:$CLASSPATH"
else
    setenv CLASSPATH "$DNANEXUS_HOME/lib/java/*"
endif

if $?GEM_PATH then
    setenv GEM_PATH "$DNANEXUS_HOME/lib/rubygems:$GEM_PATH"
else
    setenv GEM_PATH "$DNANEXUS_HOME/lib/rubygems"
endif

setenv PYTHONIOENCODING UTF-8
