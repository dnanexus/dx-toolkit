DNAnexus Perl API
=================

Building
--------

    make all

Using the package
-----------------

Before the Perl bindings can be initialized, you must set environment
variables that supply DNAnexus configuration (including a token that
authenticates you). You can do this with:

    dx login
    source <(dx env --bash)

Tests / Sample Code
-------------------

    perl t/10-basics.t
