User Defined Functions (UDFs)
=============================

User-Defined Functions (UDFs) are a mechanism to run Python code on the data that lives in a DataFrame.

A UDF can be used just like :doc:`Expressions <../user_guide/basic_concepts/expressions>`, allowing users to express computation that
should be executed by Daft lazily.

To write a UDF, you should use the :func:`@udf <daft.udfs.udf>` decorator, which can decorate either a Python
function or a Python class, producing either a :class:`StatelessUDF <daft.udfs.StatelessUDF>` or
:class:`StatefulUDF <daft.udfs.StatefulUDF>` respectively.

For more details, please consult the :doc:`UDF User Guide <../user_guide/daft_in_depth/udf>`

.. currentmodule:: daft

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/udfs

    udf

.. toctree::

    doc_manual/stateful_udf
    doc_manual/stateless_udf
