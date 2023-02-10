Installation
============

To install Daft, run this from your terminal:

``pip install getdaft``

Extra Dependencies
------------------

Some Daft functionality may also require other dependencies, which are specified as "extras":

1. Installing ``getdaft[aws]`` will install the extra dependencies required to use Daft with AWS services such as AWS S3
2. Installing ``getdaft[ray]`` will install the extra dependencies required to use Daft with Ray

To install Daft with all extras, run:

``pip install getdaft[all]``

Advanced Installation
---------------------

Installing Nightlies
^^^^^^^^^^^^^^^^^^^^

If you wish to use Daft at the bleeding edge of development, you may also install the nightly build of Daft which is built every night against the ``main`` branch:

``pip install getdaft --pre --extra-index-url https://pypi.anaconda.org/daft-nightly/simple``

Installing Daft from source
^^^^^^^^^^^^^^^^^^^^^^^^^^^

``pip install https://github.com/Eventual-Inc/Daft/archive/refs/heads/main.zip``

Please note that Daft requires the Rust toolchain in order to build from source.
