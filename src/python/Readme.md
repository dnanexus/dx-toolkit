dxpy: DNAnexus Python API
=========================

[API Documentation](http://autodoc.dnanexus.com/bindings/python/current/)

Building
--------

From the dx-toolkit root directory:

```
python3 -m pip install -e src/python
```

Debugging
---------

Set the `_DX_DEBUG` environment variable to a positive integer before
running a dxpy-based program (such as `dx`) to display the input and
output of each API call. Supported values are 1, 2, and 3 with
increasing numbers producing successively more verbose output.

Example:

```
$ _DX_DEBUG=1 dx ls
```

Python coding style
-------------------

* Conform to [PEP-8](http://legacy.python.org/dev/peps/pep-0008/).
    * Relax the line length requirement to 120 characters per line, where you judge readability not to be compromised.
    * Relax other PEP-8 requirements at your discretion if it simplifies code or is needed to follow conventions
      established elsewhere at DNAnexus.
* Document your code in a format usable by [Sphinx Autodoc](http://sphinx-doc.org/ext/autodoc.html).
* Run `pylint -E` on your code before checking it in.
* Do not introduce module import-time side effects.
    * Do not add module-level attributes into the API unless you are absolutely certain they will remain constants. For
      example, do not declare an attribute `dxpy.foo` (`dxpy._foo` is OK), or any other non-private variable in the
      global scope of any module. This is because unless the value is a constant, it may need to be updated by an
      initialization method, which may need to run lazily to avoid side effects at module load time. Instead, use
      accessor methods that can perform the updates at call time:

      ```python
      _foo = None

      def get_foo():
          initialize()
          return _foo
      ```

Other useful resources:

* [Google Python style guide](http://google.github.io/styleguide/pyguide.html)

Python version compatibility
----------------------------
dxpy is supported on Python 3 (3.8+)

Convention for Python scripts that are also modules
---------------------------------------------------

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
