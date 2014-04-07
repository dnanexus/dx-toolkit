DNAnexus Python API
===================

The Python library is called ```dxpy```.

Documentation is available at http://autodoc.dnanexus.com/bindings/python/current/.

## Python coding style and version compatibility
* Conform to [PEP-8](http://legacy.python.org/dev/peps/pep-0008/).
    * Relax the line length requirement to 120 characters per line, where you judge readability not to be compromised.
    * Relax other PEP-8 requirements at your discretion if it simplifies code or is needed to follow conventions
      established elsewhere at DNAnexus.
* Document your code in a format usable by [Sphinx Autodoc](http://sphinx-doc.org/ext/autodoc.html).

Other useful resources:
* [The Hitchhiker’s Guide to Python](http://docs.python-guide.org/en/latest/index.html)

## Convention for Python scripts that are also modules

Some scripts, such as format converters, are useful both as standalone executables and as importable modules.

We have the following convention for these scripts:
* Install the script into ```src/python/dxpy/scripts``` with a name like ```dx_useful_script.py```. This will allow
  importing with ```import dxpy.scripts.dx_useful_script```.
* Include in the script a top-level function called ```main()```, which should be the entry point processor, and
  conclude the script with the following stanza:

  ```python
  if __name__ == '__main__':
      main()
  ```

* The dxpy installation process (invoked through ```setup.py``` or with ```make -C src python``` at the top level)
  will find the script and install a launcher for it into the executable path automatically. This is done using the
  ```entry_points``` facility of setuptools/distribute.

    * Note: the install script will replace underscores in the name of your module with dashes in the name of the launcher
      script.

* Typically, when called on the command line, *main()* will first parse the command line arguments (sys.argv). However,
  when imported as a module, the arguments need to instead be passed as inputs to a function. The following is a
  suggestion for how to accommodate both styles simultaneously with just one entry point (```main```):

  ```python
  def main(**kwargs):
      if len(kwargs) == 0:
          kwargs = vars(arg_parser.parse_args(sys.argv[1:]))
      ...

  if __name__ == '__main__':
      main()
  ```
