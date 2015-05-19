DNAnexus Python API
===================

The Python library is called ```dxpy```.

Documentation is available at http://autodoc.dnanexus.com/bindings/python/current/.

## Python coding style
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

## Python version compatibility
Code going into the Python codebase should be written in Python 3.3 style, and should be compatible with Python 3.3, 3.4,
and 2.7. To facilitate Python 2 compatibility, we have the compat module in https://github.com/dnanexus/dx-toolkit/blob/master/src/python/dxpy/compat.py. Also, the following boilerplate should be
inserted into all Python source files:

```
from __future__ import absolute_import, division, print_function, unicode_literals
```

- `dxpy.compat` has some simple shims that mirror Python 3.3 builtins and redirect them to Python 2.7 equivalents when on 2.7. Most critically, `from dxpy.compat import str` will import the `unicode` builtin on 2.7 and the `str` builtin on 3.3. Use `str` wherever you would have used `unicode`. To convert unicode strings to bytes, use `.encode('utf-8')`.
- Use `from __future__ import print_function` and use print as a function. Instead of `print >>sys.stderr`, write `print(..., file=sys.stderr)`.
- The next most troublesome gotcha after the bytes/unicode conversions is that many iterables operators return generators in Python 3. For example, `map()` returns a generator. This breaks places that expect a list, and requires either explicit casting with `list()`, or the use of list comprehensions (usually preferred).
- Instead of `raw_input`, use `from dxpy.compat import input`.
- Instead of `.iteritems()`, use `.items()`. If this is a performance concern on 2.7, introduce a shim in compat.py.
- Instead of `StringIO.StringIO`, use `from dxpy.compat import BytesIO` (which is StringIO on 2.7).
- Instead of `<iterator>.next()`, use `next(<iterator>)`.
- Instead of `x.has_key(y)`, use `y in x`.
- Instead of `sort(x, cmp=lambda x, y: ...)`, use `x=sorted(x, key=lambda x: ...)`.

Other useful resources:
* [The Hitchhiker’s Guide to Python](http://docs.python-guide.org/en/latest/index.html)
*  http://lucumr.pocoo.org/2013/5/21/porting-to-python-3-redux/

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
