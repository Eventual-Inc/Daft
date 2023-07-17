DataTypes
=========

.. currentmodule:: daft

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/datatype_methods

    daft.DataType

DataType Constructors
#####################

Construct Daft DataTypes using these constructor APIs.

This is useful in many situations, such as casting, schema declaration and more.

.. code:: python

    import daft

    dtype = daft.DataType.int64()
    df = df.with_column("int64_column", df["int8_col"].cast(dtype))

.. _api-datatypes-numeric:

Numeric
-------

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/datatype_methods

    daft.DataType.int8
    daft.DataType.int16
    daft.DataType.int32
    daft.DataType.int64
    daft.DataType.uint8
    daft.DataType.uint16
    daft.DataType.uint32
    daft.DataType.uint64
    daft.DataType.float32
    daft.DataType.float64


.. _api-datatypes-logical:

Logical
-------

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/datatype_methods

    daft.DataType.bool


.. _api-datatypes-string:

Strings
-------

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/datatype_methods

    daft.DataType.binary
    daft.DataType.string


.. _api-datatypes-temporal:

Temporal
--------

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/datatype_methods

    daft.DataType.date


.. _api-datatypes-nested:

Nested
------

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/datatype_methods

    daft.DataType.list
    daft.DataType.fixed_size_list
    daft.DataType.struct


Python
------

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/datatype_methods

    daft.DataType.python


.. _api-datatypes-complex:

Complex Types
-------------

Machine Learning
^^^^^^^^^^^^^^^^

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/datatype_methods

    daft.DataType.tensor
    daft.DataType.embedding

Computer Vision
^^^^^^^^^^^^^^^

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/datatype_methods

    daft.DataType.image

.. autosummary::
    :nosignatures:

    ImageMode

.. autosummary::
    :nosignatures:

    ImageFormat


Miscellaneous
^^^^^^^^^^^^^
.. autosummary::
    :nosignatures:
    :toctree: doc_gen/datatype_methods

    daft.DataType.null

.. toctree::
    :hidden:

    datetype_image_format/daft.ImageFormat
    datatype_image_mode/daft.ImageMode
