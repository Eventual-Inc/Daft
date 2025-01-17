Installation
============

To install Daft, run this from your terminal:

.. code-block:: shell

    pip install -U getdaft

Extra Dependencies
------------------

Some Daft functionality may also require other dependencies, which are specified as "extras":

To install Daft with the extra dependencies required for interacting with AWS services, such as AWS S3, run:

.. code-block:: shell

    pip install -U getdaft[aws]

To install Daft with the extra dependencies required for running distributed Daft on top of a `Ray cluster <https://docs.ray.io/en/latest/index.html>`__, run:

.. code-block:: shell

    pip install -U getdaft[ray]

To install Daft with all extras, run:

.. code-block:: shell

    pip install -U getdaft[all]

Advanced Installation
---------------------

Installing Nightlies
^^^^^^^^^^^^^^^^^^^^

If you wish to use Daft at the bleeding edge of development, you may also install the nightly build of Daft which is built every night against the ``main`` branch:

.. code-block:: shell

    pip install -U getdaft --pre --extra-index-url https://pypi.anaconda.org/daft-nightly/simple

Installing Daft from source
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: shell

    pip install -U https://github.com/Eventual-Inc/Daft/archive/refs/heads/main.zip

Please note that Daft requires the Rust toolchain in order to build from source.
