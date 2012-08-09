DNAnexus Python API
===================

# Convention for python scripts that are also modules

Some scripts, such as format converters, are useful both as standalone executables and as importable modules.

We have the following convention for these scripts:
* Install the script into ```src/python/dxpy/scripts``` with a name like ```dx_useful_script.py```. This will allow
  importing with ```import dxpy.scripts.dx_useful_script```.
* Include in the script a top-level function called ```main()```, which should be the entry point processor, and
  conclude the script with the following stanza:

        if __name__ == '__main__':
             main()

* The dxpy installation process (invoked through ```setup.py``` or with ```make -C src python``` at the top level)
  will find the script and install a launcher for it into the executable path automatically. This is done using the
  ```entry_points``` facility of setuptools/distribute.

* Note: the install script will replace underscores in the name of your module with dashes in the name of the launcher
  script.

___

TODO: intro, link to docserver; this file should also show up on pypi
