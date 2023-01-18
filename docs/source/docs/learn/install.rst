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

Installing Daft from source
---------------------------

If you wish to use Daft at the bleeding edge of development, you may also install Daft from source on the ``main`` branch with:

``pip install https://github.com/Eventual-Inc/Daft/archive/refs/heads/main.zip``

Installing this way will grant you access to all the new features of Daft as they are merged in, but these changes will not have been tested as rigorously as those from an official release.
