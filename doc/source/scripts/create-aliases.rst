create-aliases
==============

Synopsis
--------

    create-aliases [-fvh] [-c path] [--generate-configfile] [-m module]

Description
-----------

create-aliases creates or updates entity aliases.

Options
-------

.. cmdoption:: -c <path>, --configfile <path>

   Specify which config file to use. By default minerva-node will use ``/etc/minerva/node.conf``.

.. cmdoption:: --generate-configfile

   Generate a template config file and send it to stdout.

.. cmdoption:: -f, --flush

   Flush/delete all existing aliases of the same type before generating new
   ones.

.. cmdoption:: -m module, --module module

   Specifies the module to use for generating aliases.


Generic Options
---------------

.. cmdoption:: -h, --help

   Print a short description of all command line options and exit.

.. cmdoption:: -v, --version

   Print the version of create-aliases and exit.

