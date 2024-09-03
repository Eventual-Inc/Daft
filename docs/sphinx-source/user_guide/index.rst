Daft User Guide
===============

.. toctree::
    :hidden:
    :maxdepth: 1

    basic_concepts
    daft_in_depth
    poweruser
    integrations
    tutorials
    fotw/index

Welcome to **Daft**!

Daft is a Python dataframe library that enables Pythonic data processing at large scale.

* **Fast** - Daft kernels are written and accelerated using Rust on Apache Arrow arrays.

* **Flexible** - you can work with any Python object in a Daft Dataframe.

* **Interactive** - Daft provides a first-class notebook experience.

* **Scalable** - Daft uses out-of-core algorithms to work with datasets that cannot fit in memory.

* **Distributed** - Daft scales to a cluster of machines using Ray to crunch terabytes of data.

* **Intelligent** - Daft performs query optimizations to speed up your work.

The core interface provided by Daft is the *DataFrame*, which is a table of data consisting of rows and columns. This user guide
aims to help Daft users master the usage of the Daft *DataFrame* for all your data processing needs!

.. NOTE::

    Looking to get started with Daft ASAP?

    The Daft User Guide is a useful resource to take deeper dives into specific Daft concepts, but if you are ready to jump into
    code you may wish to take a look at these resources:

    1. :doc:`../10-min`: Itching to run some Daft code? Hit the ground running with our 10 minute quickstart notebook.
    2. (Coming soon!) Cheatsheet: Quick reference to commonly-used Daft APIs and usage patterns - useful to keep next to your laptop as you code!
    3. :doc:`../api_docs/index`: Searchable documentation and reference material to Daft's public Python API.

Table of Contents
-----------------

The Daft User Guide is laid out as follows:

:doc:`Basic Concepts <basic_concepts>`
**************************************

High-level overview of Daft interfaces and usage to give you a better understanding of how Daft will fit into your day-to-day workflow.

:doc:`Daft in Depth <daft_in_depth>`
************************************

Core Daft concepts all Daft users will find useful to understand deeply.

:doc:`The Daft Poweruser <poweruser>`
*************************************

Become a true Daft Poweruser! This section explores advanced topics to help you configure Daft for specific application environments, improve reliability and optimize for performance.

:doc:`Integrations <integrations>`
**********************************

Learn how to use Daft's integrations with other technologies such as Ray Datasets or Apache Iceberg.

:doc:`Tutorials <tutorials>`
****************************

Applications built using Daft to serve as inspiration for your own projects
