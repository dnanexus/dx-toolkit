#!/usr/bin/env python2.7

# This file is copied from the Python package z3c.coverage and adapted by DNAnexus for the Python package dxpy by
# adding the --cover and --exclude options.

##############################################################################
#
# Copyright (c) 2007 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""Coverage Report

Convert trace.py coverage reports to HTML.

Usage: coveragereport.py [input-dir-or-file directory [output-directory]]

Loads coverage data from plain-text coverage reports (files named
``dotted.package.name.cover``) in the report directory *or* a .coverage
file produced by http://pypi.python.org/pypi/coverage and produces HTML
reports in the output directory.

The format of plain-text coverage reports is as follows: the file name is
a dotted Python module name with a ``.cover`` suffix (e.g.
``zope.app.__init__.cover``).  Each line corresponds to the source file
line with a 7 character wide prefix.  The prefix is one of

  '       ' if a line is not an executable code line
  '  NNN: ' where NNN is the number of times this line was executed
  '>>>>>> ' if this line was never executed

You can produce such files with the Zope test runner by specifying
``--coverage`` on the command line, or, more generally, by using the
``trace`` module in the standard library.  Although you should consider
using the above-mentioned coverage package instead, as it's much faster
than trace.py.

$Id: coveragereport.py 129516 2013-02-20 05:29:42Z srichter $
"""
from __future__ import print_function
__docformat__ = "reStructuredText"

import sys
import os
import datetime
import cgi
import subprocess
import optparse
import tempfile


HIGHLIGHT_COMMAND = ['enscript', '-q', '--footer', '--header', '-h',
                     '--language=html', '--highlight=python', '--color',
                     '-o', '-']


class Lazy(object):
    """Descriptor for lazy evaluation"""

    def __init__(self, func):
        self.func = func
        self.name = func.__name__

    def __get__(self, obj, cls):
        if obj is None:
            return self
        value = self.func(obj)
        obj.__dict__[self.name] = value
        return value


class CoverageNode(dict):
    """Tree node.

    Leaf nodes have no children (items() == []) and correspond to Python
    modules.  Branches correspond to Python packages.  Child nodes are
    accessible via the Python mapping protocol, as you would normally use
    a dict.  Item keys are non-qualified module names.
    """

    @Lazy
    def covered(self):
        return sum(child.covered for child in self.values())

    @Lazy
    def total(self):
        return sum(child.total for child in self.values())

    @Lazy
    def uncovered(self):
        return self.total - self.covered

    @Lazy
    def percent(self):
        if self.total != 0:
            return 100 * self.covered // self.total
        else:
            return 100

    @Lazy
    def html_source(self):
        return ''

    def __str__(self):
        return '%s%% covered (%s of %s lines uncovered)' % \
               (self.percent, self.uncovered, self.total)

    def get_at(self, path):
        """Return a tree node for a given path.

        The path is a sequence of child node names.
        """
        node = self
        for name in path:
            node = node[name]
        return node

    def set_at(self, path, node):
        """Create a tree node at a given path.

        The path is a sequence of child node names.

        Creates intermediate nodes if necessary.
        """
        parent = self
        for name in path[:-1]:
            parent = parent.setdefault(name, CoverageNode())
        parent[path[-1]] = node


class TraceCoverageNode(CoverageNode):
    """Coverage node loaded from an annotated source file."""

    def __init__(self, cover_filename):
        self.cover_filename = cover_filename
        self.covered, self.total = self._parse(cover_filename)

    def _parse(self, filename):
        """Parse a plain-text coverage report and return (covered, total)."""
        covered = 0
        total = 0
        with open(filename) as file:
            for line in file:
                if line.startswith(' '*7) or len(line) < 7:
                    continue
                total += 1
                if not line.startswith('>>>>>>'):
                    covered += 1
        return (covered, total)

    @Lazy
    def html_source(self):
        text = syntax_highlight(self.cover_filename)
        text = highlight_uncovered_lines(text)
        return '<pre>%s</pre>' % text


class CoverageCoverageNode(CoverageNode):
    """Coverage node loaded from a coverage.py data file."""

    def __init__(self, cov, source_filename):
        self.cov = cov
        self.source_filename = source_filename
        (filename_again, statements, excluded, missing,
         missing_str) = cov.analysis2(source_filename)
        self.covered = len(statements) - len(excluded) - len(missing)
        self.total = len(statements) - len(excluded)
        self._missing = set(missing)
        self._statements = set(statements)
        self._excluded = set(excluded)

    @Lazy
    def annotated_source(self):
        MISSING   = '>>>>>> '
        STATEMENT = '    1: '
        EXCLUDED  = '     # '
        OTHER     = '       '
        lines = []
        f = open(self.source_filename)
        for n, line in enumerate(f):
            n += 1 # workaround lack of enumerate(f, start=1) support in 2.4/5
            if n in self._missing:      prefix = MISSING
            elif n in self._excluded:   prefix = EXCLUDED
            elif n in self._statements: prefix = STATEMENT
            else:                       prefix = OTHER
            lines.append(prefix + line)
        return ''.join(lines)

    @Lazy
    def html_source(self):
        tmpdir = tempfile.mkdtemp(prefix='z3c.coverage')
        tmpfilename = os.path.join(tmpdir,
                            os.path.basename(self.source_filename) + '.cover')
        tmpf = open(tmpfilename, 'w')
        tmpf.write(self.annotated_source)
        tmpf.close()
        text = syntax_highlight(tmpf.name)
        os.unlink(tmpfilename)
        os.rmdir(tmpdir)
        text = highlight_uncovered_lines(text)
        return '<pre>%s</pre>' % text


def get_file_list(path, filter_fn=None):
    """Return a list of files in a directory.

    If you can specify a predicate (a callable), only file names matching it
    will be returned.
    """
    return filter(filter_fn, os.listdir(path))


def filename_to_list(filename):
    """Return a list of package/module names from a filename.

    One example is worth a thousand descriptions:

        >>> filename_to_list('z3c.coverage.__init__.cover')
        ['z3c', 'coverage', '__init__']

    """
    return filename.split('.')[:-1]


def create_tree_from_files(filelist, path):
    """Create a tree with coverage statistics.

    Takes the directory for coverage reports and a list of filenames relative
    to that directory.  Parses all the files and constructs a module tree with
    coverage statistics.

    Returns the root node of the tree.
    """
    root = CoverageNode()
    for filename in filelist:
        tree_index = filename_to_list(filename)
        filepath = os.path.join(path, filename)
        root.set_at(tree_index, TraceCoverageNode(filepath))
    return root


def create_tree_from_coverage(cov, strip_prefix=None, path_aliases=None, cover=[], exclude=[]):
    """Create a tree with coverage statistics.

    Takes a coverage.coverage() instance.

    Returns the root node of the tree.
    """
    root = CoverageNode()
    if path_aliases:
        apply_path_aliases(cov, dict([alias.partition('=')[::2]
                                      for alias in path_aliases]))
    for filename in cov.data.measured_files():
        if not any(pattern in filename.replace('/', '.') for pattern in cover):
            continue
        if any(pattern in filename.replace('/', '.') for pattern in exclude):
            continue

        if strip_prefix and filename.startswith(strip_prefix):
            short_name = filename[len(strip_prefix):]
            short_name = short_name.replace('/', os.path.sep)
            short_name = short_name.lstrip(os.path.sep)
        else:
            short_name = cov.file_locator.relative_filename(filename)
        tree_index = filename_to_list(short_name.replace(os.path.sep, '.'))
        if 'tests' in tree_index or 'ftests' in tree_index:
            continue
        root.set_at(tree_index, CoverageCoverageNode(cov, filename))
    return root


def apply_path_aliases(cov, aliases):
    """Adjust filenames in coverage data."""
    # XXX: fragile: we're touching the internal data structures directly
    # longest key first
    aliases = sorted(
        aliases.items(), key=lambda i: len(i[0]), reverse=True)
    def fixup_filename(filename):
        for alias, local in aliases:
            if filename.startswith(alias):
                return local + filename[len(alias):]
        return filename
    cov.data.lines = map_and_merge_dict_keys(fixup_filename, cov.data.lines)
    cov.data.arcs = map_dict_keys(fixup_filename, cov.data.arcs)

def map_and_merge_dict_keys(fn, d):
    def merge_fn(*args):
        accumulated = set()
        for l in args:
            accumulated.update(l)
        return sorted(accumulated)
    output = {}
    for k, v in d.iteritems():
        if fn(k) not in output:
            output[fn(k)] = []
        output[fn(k)].append(v)
    return dict((f_k, merge_fn(*v)) for f_k, v in output.iteritems())

def map_dict_keys(fn, d):
    """Transform {x: y} to {fn(x): y}."""
    return dict((fn(k), v) for k, v in d.items())


def traverse_tree(tree, index, function):
    """Preorder traversal of a tree.

    ``index`` is the path of the root node (usually []).

    ``function`` gets one argument: the path of a node.
    """
    function(tree, index)
    for key, node in tree.items():
        traverse_tree(node, index + [key], function)


def traverse_tree_in_order(tree, index, function, order_by):
    """Preorder traversal of a tree.

    ``index`` is the path of the root node (usually []).

    ``function`` gets one argument: the path of a node.

    ``order_by`` gets one argument a tuple of (key, node).
    """
    function(tree, index)
    for key, node in sorted(tree.items(), key=order_by):
        traverse_tree_in_order(node, index + [key], function, order_by)


def index_to_url(index):
    """Construct a relative hyperlink to a tree node given its path."""
    if index:
        return '%s.html' % '.'.join(index)
    return 'index.html'


def index_to_nice_name(index):
    """Construct an indented name for the node given its path."""
    if index:
        return '&nbsp;' * 4 * (len(index) - 1) + index[-1]
    else:
        return 'Everything'


def index_to_name(index):
    """Construct the full name for the node given its path."""
    if index:
        return '.'.join(index)
    return 'everything'


def percent_to_colour(percent):
    if percent == 100:
        return 'green'
    elif percent >= 95:
        return '#74F300'
    elif percent >= 90:
        return 'yellow'
    elif percent >= 80:
        return 'orange'
    else:
        return 'red'


def print_table_row(html, node, file_index):
    """Generate a row for an HTML table."""
    nice_name = index_to_nice_name(file_index)
    if not node.keys():
        nice_name += '.py'
    else:
        nice_name += '/'
    print('<tr><td><a href="%s">%s</a></td>' % \
              (index_to_url(file_index), nice_name), file=html)
    print('<td style="background: %s">&nbsp;&nbsp;&nbsp;&nbsp;</td>' % \
              (percent_to_colour(node.percent)), file=html)
    print('<td>covered %s%% (%s of %s uncovered)</td></tr>' % \
              (node.percent, node.uncovered, node.total), file=html)


HEADER = """
    <html>
      <head><title>Test coverage for %(name)s</title>
      <style type="text/css">
        a {text-decoration: none; display: block; padding-right: 1em;}
        a:hover {background: #EFA;}
        hr {height: 1px; border: none; border-top: 1px solid gray;}
        .notcovered {background: #FCC;}
        .footer {margin: 2em; font-size: small; color: gray;}
      </style>
      </head>
      <body><h1>Test coverage for %(name)s</h1>
      <table>
    """


FOOTER = """
      <div class="footer">
      %s
      </div>
    </body>
    </html>"""


def generate_html(output_filename, tree, my_index, info, path, footer=""):
    """Generate HTML for a tree node.

    ``output_filename`` is the output file name.

    ``tree`` is the root node of the tree.

    ``my_index`` is the path of the node for which you are generating this HTML
    file.

    ``info`` is a list of paths of child nodes.

    ``path`` is the directory name for the plain-text report files.
    """
    html = open(output_filename, 'w')
    print(HEADER % {'name': index_to_name(my_index)}, file=html)
    info = [(tree.get_at(node_path), node_path) for node_path in info]
    def key(node_info):
        (node, node_path) = node_info
        return (len(node_path), -node.uncovered, node_path and node_path[-1])
    info.sort(key=key)
    for node, file_index in info:
        if not file_index:
            continue # skip root node
        print_table_row(html, node, file_index)
    print('</table><hr/>', file=html)
    print(tree.get_at(my_index).html_source, file=html)
    print(FOOTER % footer, file=html)
    html.close()


def syntax_highlight(filename):
    """Return HTML with syntax-highlighted Python code from a file."""
    # TODO: use pygments instead
    try:
        pipe = subprocess.Popen(HIGHLIGHT_COMMAND + [filename],
                                stdout=subprocess.PIPE)
        text, stderr = pipe.communicate()
        if pipe.returncode != 0:
            raise OSError
    except OSError:
        # Failed to run enscript; maybe it is not installed?  Disable
        # syntax highlighting then.
        with open(filename, 'r') as file:
            text = cgi.escape(file.read())
    else:
        #print(text)
        #text = text.decode('utf-8')
        text = text[text.find('<PRE>')+len('<PRE>'):]
        text = text[:text.find('</PRE>')]
    return text


def highlight_uncovered_lines(text):
    """Highlight lines beginning with '>>>>>>'."""
    def color_uncov(line):
        # The line must start with the missing line indicator or some HTML
        # was put in front of it.
        if line.startswith('&gt;'*6) or '>'+'&gt;'*6 in line:
            return ('<div class="notcovered">%s</div>'
                    % line.rstrip('\n'))
        return line
    text = ''.join(map(color_uncov, text.splitlines(True)))
    return text


def generate_htmls_from_tree(tree, path, report_path, footer=""):
    """Generate HTML files for all nodes in the tree.

    ``tree`` is the root node of the tree.

    ``path`` is the directory name for the plain-text report files.

    ``report_path`` is the directory name for the output files.
    """
    def make_html(node, my_index):
        info = []
        def list_parents_and_children(node, index):
            position = len(index)
            my_position = len(my_index)
            if position <= my_position and index == my_index[:position]:
                info.append(index)
            elif (position == my_position + 1 and
                  index[:my_position] == my_index):
                info.append(index)
            return
        traverse_tree(tree, [], list_parents_and_children)
        output_filename = os.path.join(report_path, index_to_url(my_index))
        if not my_index:
            return # skip root node
        generate_html(output_filename, tree, my_index, info, path, footer)
    traverse_tree(tree, [], make_html)


def generate_overall_html_from_tree(tree, output_filename, footer=""):
    """Generate an overall HTML file for all nodes in the tree."""
    html = open(output_filename, 'w')
    print(HEADER % {'name': ', '.join(sorted(tree.keys()))}, file=html)
    def print_node(node, file_index):
        if file_index: # skip root node
            print_table_row(html, node, file_index)
    def sort_by(node_info):
        (key, node) = node_info
        return (-node.uncovered, key)
    traverse_tree_in_order(tree, [], print_node, sort_by)
    print('</table><hr/>', file=html)
    print(FOOTER % footer, file=html)
    html.close()


def create_report_path(report_path):
    if not os.path.exists(report_path):
        os.makedirs(report_path)


def filter_fn(filename):
    """Filter interesting coverage files.

        >>> filter_fn('z3c.coverage.__init__.cover')
        True
        >>> filter_fn('z3c.coverage.tests.cover')
        False
        >>> filter_fn('z3c.coverage.tests.test_foo.cover')
        False
        >>> filter_fn('z3c.coverage.ftests.test_bar.cover')
        False
        >>> filter_fn('z3c.coverage.testing.cover')
        True
        >>> filter_fn('z3c.coverage.testname.cover')
        True
        >>> filter_fn('something-unrelated.txt')
        False
        >>> filter_fn('<doctest something-useless.cover')
        False

    """
    parts = filename.split('.')
    return (filename.endswith('.cover') and
            not filename.startswith('<') and
            'tests' not in parts and
            'ftests' not in parts)


def load_coverage(path, opts):
    """Load coverage information from ``path``.

    ``path`` can point to a directory full of files named *.cover, or it can
    point to a single pickle file containing coverage information.
    """
    if os.path.isdir(path):
        filelist = get_file_list(path, filter_fn)
        tree = create_tree_from_files(filelist, path)
        return tree
    else:
        import coverage
        cov = coverage.coverage(data_file=path, config_file=False)
        cov.load()
        tree = create_tree_from_coverage(cov, strip_prefix=opts.strip_prefix,
                                         path_aliases=opts.path_alias,
                                         cover=opts.cover,
                                         exclude=opts.exclude)
        return tree


def make_coverage_reports(path, report_path, opts):
    """Convert reports from ``path`` into HTML files in ``report_path``."""
    if opts.verbose:
        print("Loading coverage reports from %s" % path)
    tree = load_coverage(path, opts=opts)
    if opts.verbose:
        print(tree)
    rev = get_git_revision(os.path.join(path, os.path.pardir))
    timestamp = str(datetime.datetime.utcnow())+"Z"
    footer = "Generated for revision %s on %s" % (rev, timestamp)
    create_report_path(report_path)
    generate_htmls_from_tree(tree, path, report_path, footer)
    generate_overall_html_from_tree(
        tree, os.path.join(report_path, 'all.html'), footer)
    if opts.verbose:
        print("Generated HTML files in %s" % report_path)


def get_svn_revision(path):
    """Return the Subversion revision number for a working directory."""
    try:
        pipe = subprocess.Popen(['svnversion', path], stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        stdout, stderr = pipe.communicate()
        rev = stdout.strip()
    except OSError:
        rev = ""
    if not rev:
        rev = "UNKNOWN"
    return rev

def get_git_revision(path):
    """Return the Git describe output for a working directory."""
    try:
        pipe = subprocess.Popen(['git', 'describe', '--always'], stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        stdout, stderr = pipe.communicate()
        rev = stdout.strip()
    except OSError:
        rev = ""
    if not rev:
        rev = "UNKNOWN"
    return rev

def main(args=None):
    """Process command line arguments and produce HTML coverage reports."""

    parser = optparse.OptionParser(
        "usage: %prog [options] [inputpath [outputdir]]",
        description=
            'Converts coverage reports to HTML.  If the input path is'
            ' omitted, it defaults to coverage or .coverage, whichever'
            ' exists.  If the output directory is omitted, it defaults to'
            ' inputpath + /report or ./coverage-reports, depending on whether'
            ' the input path points to a directory or a file.')

    parser.add_option('-q', '--quiet', help='be quiet',
                      action='store_const', const=0, dest='verbose')
    parser.add_option('-v', '--verbose', help='be verbose (default)',
                      action='store_const', const=1, dest='verbose', default=1)
    parser.add_option('--strip-prefix', metavar='PREFIX',
                      help='strip base directory from filenames loaded from .coverage')
    parser.add_option('--path-alias', metavar='PATH=LOCALPATH',
                      help='define path mappings for filenames loaded from .coverage',
                      action='append')
    parser.add_option('--cover', default=[], action='append')
    parser.add_option('--exclude', default=[], action='append')

    if args is None:
        args = sys.argv[1:]
    opts, args = parser.parse_args(list(args))

    if len(args) > 0:
        path = args[0]
    else:
        if os.path.exists('coverage'):
            # backward compat: default input path used to be 'coverage'
            path = 'coverage'
        else:
            path = '.coverage'

    if len(args) > 1:
        report_path = args[1]
    else:
        if os.path.isdir(path):
            # backward compat: default input path is 'coverage', default output
            # path is 'coverage/reports'
            report_path = os.path.join(path, 'reports')
        else:
            report_path = 'coverage-reports'

    if len(args) > 2:
        parser.error("too many arguments")

    make_coverage_reports(path, report_path, opts=opts)


if __name__ == '__main__':
    main()
