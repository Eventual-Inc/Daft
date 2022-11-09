Daft: The Distributed Python Dataframe
======================================

Daft is a **fast and scalable Python dataframe** for Complex Data and Machine Learning workloads.

.. image:: /_static/daft_illustration.png
   :alt: Daft python dataframes make it easy to load any data such as PDF documents, images, protobufs, csv, parquet and audio files into a table dataframe structure for easy querying
   :width: 500
   :align: center

----

Get Started
-----------

Installing Daft is simple with ``pip``:

.. raw:: html

   <div style="background: #333333; padding: 8px; margin: 12px; color: #f1f3f6; font-family: 'Roboto Mono', sans-serif; text-align: center; border-radius: 8px;">pip install getdaft</div>

Video Demo
^^^^^^^^^^

.. raw:: html

    <div style="position: relative; padding-bottom: 62.5%; height: 0;"><iframe src="https://www.loom.com/embed/12b02103a23b47558a7655d410efa46b" frameborder="0" webkitallowfullscreen mozallowfullscreen allowfullscreen style="position: absolute; top: 0; left: 0; width: 100%; height: 100%;"></iframe></div>

----

More Resources
^^^^^^^^^^^^^^

.. raw:: html

   <div class="features-row">
      <div class="features-row-box features-row-box-col-3">
         <h4>10-minutes to Daft</h4>
         <p>10-minute walkthrough of all of Daft's major functionality.</p>
         <a href="/docs/learn/10-min.html">View Walkthrough</a>
      </div>
      <div class="features-row-box features-row-box-col-3">
         <h4>Tutorials</h4>
         <p>Hosted examples using Daft in various common use-cases.</p>
         <a href="/docs/learn/tutorials.html">View Tutorials</a>
      </div>
      <div class="features-row-box features-row-box-col-3">
         <h4>Docs</h4>
         <p>Developer documentation for referencing Daft APIs.</p>
         <a href="/docs/index.html">View Docs</a>
      </div>
   </div>

----

Community
---------

.. raw:: html

   <div class="features-row">
      <div class="features-row-box features-row-box-col-2">
         <a href="https://github.com/Eventual-Inc/Daft/discussions" style="display: block; height: 100%;">
            <div class="centered clickable-box">
                  <img alt="Github Discussions" height="36" width="auto" src="/_static/github-logo.png" style="margin-top: 16px"/>
                  <h4>Github Discussions Forums</h4>
                  <p>Post questions, suggest features and more</p>
            </div>
         </a>
      </div>
      <div class="features-row-box features-row-box-col-2">
         <a href="https://discord.gg/eByWAQwTaP" style="display: block; height: 100%;">
            <div class="centered clickable-box">
               <img alt="Discord" height="36" width="auto" src="/_static/discord-logo.svg"  style="margin-top: 16px"/>
               <h4>Discord Server</h4>
               <p>Chat with Daft devs and show off your projects!</p>
            </div>
         </a>
      </div>
   </div>

----

Integrations
------------

Daft is open-sourced and you can use any Python library when processing data in a dataframe. It integrates with many other open-sourced technologies as well, plugging directly into your current infrastructure and systems.

.. raw:: html

   <div class="features-row">
      <div class="features-row-box features-row-box-col-2">
         <h4>Data Science and Machine Learning</h4>
         <div class="image-grid">
            <img alt="numpy the Python numerical library" height="36" width="auto" src="/_static/numpy-logo.png"/>
            <img alt="Pandas a python dataframe library" height="36" width="auto" src="/_static/pandas-logo.png"/>
            <img alt="Polars a python dataframe library" height="36" width="auto" src="/_static/polars-logo.svg"/>
            <img alt="Ray the Python distributed systems framework" height="36" width="auto" src="/_static/ray-logo.png"/>
            <img alt="Jupyter notebooks for interactive computing" height="36" width="auto" src="/_static/jupyter-logo.png"/>
         </div>
      </div>
      <div class="features-row-box features-row-box-col-2">
         <h4>Storage</h4>
         <div class="image-grid">
            <img alt="Apache Parquet file formats" height="36" width="auto" src="/_static/parquet-logo.png"/>
            <img alt="Apache Arrow for efficient data serialization" height="36" width="auto" src="/_static/arrow-logo.png"/>
            <img alt="AWS S3 for cloud storage" height="36" width="auto" src="/_static/amazon-s3-logo.png"/>
            <img alt="Google Cloud Storage for cloud storage" height="36" width="auto" src="/_static/google-cloud-storage.png"/>
            <img alt="Azure Blob Store for cloud storage" height="36" width="auto" src="/_static/azure-blob-store.png"/>
         </div>
      </div>
   </div>

----

Use-Cases
---------

.. raw:: html

   <div class="features-row">
      <div class="features-row-box features-row-box-col-2">
         <h4>Data Science Experimentation</h4>
         <p>Daft enables data scientists/engineers to work from their preferred Python notebook environment for interactive experimentation on complex data</p>
      </div>
      <div class="features-row-box features-row-box-col-2">
         <h4>Complex Data Warehousing</h4>
         <p>The Daft Python dataframe efficiently pipelines complex data from raw data lakes to clean, queryable datasets for analysis and reporting.</p>
      </div>
      <div class="features-row-box features-row-box-col-2">
         <h4>Machine Learning Training Dataset Curation</h4>
         <p>Modern Machine Learning is data-driven and relies on clean data. The Daft Python dataframe integrates with dataloading frameworks such as <a href="https://www.ray.io">Ray</a> and <a href="https://www.pytorch.org">PyTorch</a> to feed data to distributed model training.</p>
      </div>
      <div class="features-row-box features-row-box-col-2">
         <h4>Machine Learning Model Evaluation</h4>
         <p>Evaluating the performance of machine learning systems is challenging, but Daft Python dataframes make it easy to run models and SQL-style analyses at scale.</p>
      </div>
   </div>

----

Key Features
------------

.. raw:: html

   <div class="features-row">
      <div class="features-row-box features-row-box-col-3">
         <h4>Python UDF</h4>
         <p>Daft supports running User-Defined Functions (UDF) on columns of Python objects - if Python supports it Daft can handle it!</p>
      </div>
      <div class="features-row-box features-row-box-col-3">
         <h4>Interactive Computing</h4>
         <p>Daft embraces Python's dynamic and interactive nature, enabling fast, iterative experimentation on data in your notebook and on your laptop.</p>
      </div>
      <div class="features-row-box features-row-box-col-3">
         <h4>Distributed Computing</h4>
         <p>Daft integrates with frameworks such as <a href="https://www.ray.io">Ray</a> to run large petabyte-scale dataframes on a cluster of machines in the cloud.</p>
      </div>
   </div>


.. toctree::
   :hidden:

   docs/index
   dataframe_comparison

.. .. Indices and tables
.. .. ==================

.. .. * :ref:`genindex`
.. .. * :ref:`modindex`
.. .. * :ref:`search`
