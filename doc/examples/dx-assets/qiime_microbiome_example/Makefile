SHELL=/bin/bash -e

UBUNTU_VERSION := $(shell test -e /etc/issue && (grep -o "Ubuntu [0-9]\+\.[0-9]\+" /etc/issue | sed -e "s/Ubuntu //"))

ifeq ($(UBUNTU_VERSION), 12.04)
        CRAN_SITE = "deb http://cran.rstudio.com/bin/linux/ubuntu precise/"
else ifeq ($(UBUNTU_VERSION), 14.04)
        CRAN_SITE="deb http://cran.rstudio.com/bin/linux/ubuntu trusty/"
endif

SRC="/etc/apt/sources.list"
EA_UTILS="https://storage.googleapis.com/google-code-archive-source/v2/code.google.com/ea-utils/source-archive.zip"
EA_FILE="ea_utils.zip"

IS_FOUND = $(shell sudo grep $(CRAN_SITE) $(SRC))

all:

ifeq ($(IS_FOUND), )
	$(shell sudo echo $(CRAN_SITE) >> $(SRC))
endif
	# Trust the signing key for this repo.Reference: http://cran.rstudio.com/bin/linux/ubuntu/README.html
	sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E084DAB9
	sudo rm -f /etc/apt/apt.conf.d/99dnanexus
        #Update apt-get
	sudo apt-get --yes update
	sudo apt-get --yes upgrade
	sudo apt-get --yes install python-dev libncurses5-dev libssl-dev libzmq-dev libgsl0-dev openjdk-6-jdk libxml2 libxslt1.1 libxslt1-dev ant git subversion build-essential zlib1g-dev libpng12-dev libfreetype6-dev mpich2 libreadline-dev gfortran unzip libmysqlclient18 libmysqlclient-dev ghc sqlite3 libsqlite3-dev libc6-i386 libbz2-dev tcl-dev tk-dev r-base r-base-dev libatlas-dev libatlas-base-dev liblapack-dev swig libhdf5-serial-dev
	sudo apt-get --yes install python-simplegeneric
	sudo apt-get --yes install build-essential python-dev python-pip
	sudo apt-get --yes install python-numpy python-scipy python-matplotlib ipython ipython-notebook python-pandas python-sympy python-nose
	# install bioconductor and it's packages
	R -e "install.packages(c('ape', 'biom', 'optparse', 'RColorBrewer', 'randomForest', 'vegan'), repos='http://cran.rstudio.com/'); source('http://bioconductor.org/biocLite.R'); biocLite(); biocLite(c('DESeq2', 'metagenomeSeq'))"
	sudo pip install future click
	git clone https://github.com/pycogent/pycogent.git
	cd pycogent; python setup.py build; sudo python setup.py install; cd ..; sudo rm -rf pycogent
	sudo pip install qiime
	echo "Upgrading numexpr......."
	sudo pip install -U numexpr
	sudo apt-get --yes install libgsl0-dev
	echo "Downloading and installing ea-utils........."
	sudo curl -L $(EA_UTILS) > $(EA_FILE)
	sudo unzip $(EA_FILE)
	echo "Building and making ea-utils.........."
	sudo $(MAKE) -C ea-utils/trunk/clipper
	sudo $(MAKE) -C ea-utils/trunk/clipper install
