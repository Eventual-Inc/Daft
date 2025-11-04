# Benchmarks

## AI Benchmarks

!!! info "Powered by Flotilla"

    These benchmarks showcase [Flotilla](https://www.daft.ai/blog/introducing-flotilla-simplifying-multimodal-data-processing-at-scale), Daft's distributed engine optimized for multimodal data processing.

    [Daft vs Spark vs Ray Data: Benchmarks for Multimodal AI Workloads](https://www.daft.ai/blog/benchmarks-for-modalities-workloads) - Full methodology and technical deep-dive.

Modern AI workloads demand simple, fast, and scalable data engines. Daft is purpose-built for these workloads, excelling where traditional big data frameworks fall short.

Our benchmarks show how Daft uniquely delivers on the core needs of AI data processing:

1. **Process any modality:** Audio, video, images, and documents as first class citizens.
2. **Scale from 1 to 1000:** Easily scale across machines without rewriting code or configs.
3. **Run models over data:** Seamlessly use models to embed, classify, and generate data.

### Setup

The AI benchmarks cover four different workload types, each designed to test different use cases across modalities, and at large scale. Each benchmark runs on AWS g6.xlarge instances with 8 worker nodes.

The complete benchmark code is available in our repository:

- **[Audio Transcription](https://github.com/Eventual-Inc/Daft/tree/main/benchmarking/ai/audio_transcription)** - Transcribing 113,800 audio files using OpenAI Whisper-tiny model
- **[Document Embedding](https://github.com/Eventual-Inc/Daft/tree/main/benchmarking/ai/document_embedding)** - Generating embeddings for 10,000 PDF documents using sentence-transformers
- **[Image Classification](https://github.com/Eventual-Inc/Daft/tree/main/benchmarking/ai/image_classification)** - Classifying 803,580 images using ResNet18 model
- **[Video Object Detection](https://github.com/Eventual-Inc/Daft/tree/main/benchmarking/ai/video_object_detection)** - Detecting objects in 1,000 videos using YOLO11n model

Each benchmark includes implementations for Daft, Ray Data, and EMR Spark, along with cluster configurations and dependencies.

**Benchmark Date**: September 22, 2025

**Engine Versions**: Daft 0.6.2, Ray Data 2.49.2, AWS EMR Spark 7.10.0

### Results

<div>
    <script type="text/javascript">window.PlotlyConfig = {MathJaxConfig: 'local'};</script>
    <script charset="utf-8" src="https://cdn.plot.ly/plotly-2.20.0.min.js"></script>
    <div id="ai-benchmarks-chart" class="plotly-graph-div" style="height:100%; width:100%;"></div>
    <script type="text/javascript">
        window.PLOTLYENV = window.PLOTLYENV || {};
        if (document.getElementById("ai-benchmarks-chart")) {
            Plotly.newPlot(
                "ai-benchmarks-chart",
                [
                    {
                        "marker": {"color": "rgba(255, 0, 255, 1)"},
                        "name": "Daft",
                        "x": ["Audio Transcription", "Document Embedding", "Image Classification", "Video Object Detection"],
                        "y": [6.37, 1.9, 4.38, 11.77],
                        "type": "bar",
                        "textposition": "inside"
                    },
                    {
                        "hovertext": ["4.6x Slower", "7.6x Slower", "5.4x Slower", "2.2x Slower"],
                        "marker": {"color": "rgba(0, 102, 255, 0.75)"},
                        "name": "Ray Data",
                        "x": ["Audio Transcription", "Document Embedding", "Image Classification", "Video Object Detection"],
                        "y": [29.33, 14.53, 23.5, 25.9],
                        "type": "bar",
                        "textposition": "inside"
                    },
                    {
                        "hovertext": ["4.0x Slower", "4.2x Slower", "10.3x Slower", "18.4x Slower"],
                        "marker": {"color": "rgba(226,90,28, 0.75)"},
                        "name": "Spark",
                        "x": ["Audio Transcription", "Document Embedding", "Image Classification", "Video Object Detection"],
                        "y": [25.77, 8.07, 45.12, 216.0],
                        "type": "bar",
                        "textposition": "inside"
                    }
                ],
                {
                    "title": {"text": "AI Benchmarks - Performance Comparison (lower is better)"},
                    "yaxis": {
                        "title": {"text": "Time (minutes, log scale)"},
                        "type": "log",
                        "tickmode": "array",
                        "tickvals": [1, 2, 5, 10, 20, 40, 60, 220],
                        "ticktext": ["1", "2", "5", "10", "20", "40", "60", "220"]
                    },
                    "xaxis": {"title": {"text": "Workload"}},
                    "uniformtext": {"minsize": 8, "mode": "hide"}
                },
                {"displayModeBar": false, "responsive": true}
            );
        }
    </script>
</div>

|          | Daft | Ray Data | EMR Spark |
| -------- | :--: | :------: | :---: |
| Audio Transcription | 6m 22s | 29m 20s (4.6x slower) | 25m 46s (4.0x slower) |
| Document Embedding | 1m 54s | 14m 32s (7.6x slower) | 8m 4s (4.2x slower) |
| Image Classification | 4m 23s | 23m 30s (5.4x slower) | 45m 7s (10.3x slower) |
| Video Object Detection | 11m 46s | 25m 54s (2.2x slower) | 3h 36m (18.4x slower) |

### Logs

Complete execution logs for all AI benchmark runs are available for transparency and reproducibility:

| Workload | Daft | Ray Data | Spark |
| -------- | ---- | -------- | ----- |
| Audio Transcription | s3://daft-public-data/benchmarking/logs/ai_benchmarking_logs/audio_transcription/daft.txt | s3://daft-public-data/benchmarking/logs/ai_benchmarking_logs/audio_transcription/ray_data.txt | s3://daft-public-data/benchmarking/logs/ai_benchmarking_logs/audio_transcription/spark.txt |
| Document Embedding | s3://daft-public-data/benchmarking/logs/ai_benchmarking_logs/document_embedding/daft.txt | s3://daft-public-data/benchmarking/logs/ai_benchmarking_logs/document_embedding/ray_data.txt | s3://daft-public-data/benchmarking/logs/ai_benchmarking_logs/document_embedding/spark.txt |
| Image Classification | s3://daft-public-data/benchmarking/logs/ai_benchmarking_logs/image_classification/daft.txt | s3://daft-public-data/benchmarking/logs/ai_benchmarking_logs/image_classification/ray_data.txt | s3://daft-public-data/benchmarking/logs/ai_benchmarking_logs/image_classification/spark.txt |
| Video Object Detection | s3://daft-public-data/benchmarking/logs/ai_benchmarking_logs/video_object_detection/daft.txt | s3://daft-public-data/benchmarking/logs/ai_benchmarking_logs/video_object_detection/ray_data.txt | s3://daft-public-data/benchmarking/logs/ai_benchmarking_logs/video_object_detection/spark.txt |

---

## TPC-H Benchmarks

Here we compare Daft against some popular Distributed Dataframes such as Spark, Modin, and Dask on the TPC-H benchmark. Our goal for this benchmark is to demonstrate that Daft is able to meet the following development goals:

1. **Solid out of the box performance:** great performance without having to tune esoteric flags or configurations specific to this workload
2. **Reliable out-of-core execution:** highly performant and reliable processing on larger-than-memory datasets, without developer intervention and Out-Of-Memory (OOM) errors
3. **Ease of use:** getting up and running should be easy on cloud infrastructure for an individual developer or in an enterprise cloud setting

A great stress test for Daft is the [TPC-H benchmark](https://www.tpc.org/tpch/), which is a standard benchmark for analytical query engines. This benchmark helps ensure that while Daft makes it very easy to work with multimodal data, it can also do a great job at larger scales (terabytes) of more traditional tabular analytical workloads.

### Setup

The basic setup for our benchmarks are as follows:

1. We run questions 1 to 10 of the TPC-H benchmarks using Daft and other commonly used Python Distributed Dataframes.
2. The data for the queries are stored and retrieved from AWS S3 as partitioned Apache Parquet files, which is typical of enterprise workloads. No on disk/in-memory caching was performed.
3. We run each framework on a cluster of AWS i3.2xlarge instances that each have:
    - 8 vCPUs
    - 61G of memory
    - 1900G of NVMe SSD space

The frameworks that we benchmark against are Spark, Modin, and Dask. We chose these comparable Dataframes as they are the most commonly referenced frameworks for running large scale distributed analytical queries in Python.

For benchmarking against Spark, we use AWS EMR which is a hosted Spark service. For other benchmarks, we host our own Ray and Dask clusters on Kubernetes. Please refer to the section on our [Detailed Benchmarking Setup](#detailed-benchmarking-setup) for additional information.

### Results

!!! success "Highlights"

    1. Out of all the benchmarked frameworks, **only Daft and EMR Spark are able to run terabyte scale queries reliably** on out-of-the-box configurations.
    2. **Daft is consistently much faster** (3.3x faster than EMR Spark, 7.7x faster than Dask Dataframes, and 44.4x faster than Modin).

!!! note "Note"

    We were unable to obtain full results for Modin due to cluster OOMs, errors and timeouts (one hour limit per question attempt). Similarly, Dask was unable to provide comparable results for the terabyte scale benchmark. It is possible that these frameworks may perform and function better with additional tuning and configuration. Logs for all the runs are provided in a public AWS S3 bucket.

#### 100 Scale Factor

First we run TPC-H 100 Scale Factor (around 100GB) benchmark  on 4 i3.2xlarge worker instances. In total, these instances add up to 244GB of cluster memory which will require the Dataframe library to perform disk spilling and out-of-core processing for certain questions that have a large join or sort.

<!-- todo(doc): Find better way to embed html file content, rather than pasting the whole file, how to use snippet? -->

<div>                        <script type="text/javascript">window.PlotlyConfig = {MathJaxConfig: 'local'};</script>
    <script charset="utf-8" src="https://cdn.plot.ly/plotly-2.20.0.min.js"></script>
    <div id="78330a19-a541-460b-bd9f-217b9d4cd137" class="plotly-graph-div" style="height:100%; width:100%;"></div>
    <script type="text/javascript">
        window.PLOTLYENV = window.PLOTLYENV || {};
        if (document.getElementById("78330a19-a541-460b-bd9f-217b9d4cd137")) {
            Plotly.newPlot(
                "78330a19-a541-460b-bd9f-217b9d4cd137",
                [
                    {
                        "marker": {"color": "rgba(255, 0, 255, 1)"},
                        "name": "Daft",
                        "x": ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8", "Q9", "Q10"],
                        "y": [1.0666666666666667, 0.7666666666666667, 0.9833333333333333, 1.05, 1.9666666666666666, 0.6333333333333333, 1.1666666666666667, 2.25, 2.183333333333333, 1.0166666666666666],
                        "type": "bar",
                        "textposition": "inside"
                    },
                    {
                        "hovertext": ["5.6x Slower", "1.1x Slower", "5.1x Slower", "2.8x Slower", "2.0x Slower", "9.7x Slower", "4.3x Slower", "2.0x Slower", "2.3x Slower", "4.8x Slower"],
                        "marker": {"color": "rgba(226,90,28, 0.75)"},
                        "name": "Spark",
                        "x": ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8", "Q9", "Q10"],
                        "y": [5.991666666666666, 0.8716666666666666, 4.996666666666667, 2.955, 3.8583333333333334, 6.135000000000001, 4.985, 4.428333333333333, 5.051666666666667, 4.863333333333333],
                        "type": "bar",
                        "textposition": "inside"
                    },
                    {
                        "hovertext": ["4.2x Slower", "1.4x Slower", "6.9x Slower", "13.0x Slower", "8.2x Slower", "6.1x Slower", "6.8x Slower", "3.6x Slower", "11.8x Slower", "12.1x Slower"],
                        "marker": {"color": "rgba(255,193,30, 0.75)"},
                        "name": "Dask",
                        "x": ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8", "Q9", "Q10"],
                        "y": [4.456666666666666, 1.0983333333333334, 6.748333333333333, 13.615, 16.215, 3.8366666666666664, 7.96, 8.148333333333333, 25.790000000000003, 12.306666666666667],
                        "type": "bar",
                        "textposition": "inside"
                    },
                    {
                        "hovertext": ["29.1x Slower", "12.5x Slower", "nanx Slower", "48.6x Slower", "nanx Slower", "87.7x Slower", "nanx Slower", "nanx Slower", "nanx Slower", "52.7x Slower"],
                        "marker": {"color": "rgba(0,173,233, 0.75)"},
                        "name": "Modin",
                        "x": ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8", "Q9", "Q10"],
                        "y": [31.066666666666666, 9.616666666666667, null, 51.05, null, 55.53333333333333, null, null, null, 53.6],
                        "type": "bar",
                        "textposition": "inside"
                    }
                ],
                {
                    "title": {"text": "TPCH 100 Scale Factor - 4 Nodes (lower is better)"},
                    "yaxis": {"title": {"text": "Time (minutes)"}},
                    "xaxis": {"title": {"text": "TPCH Question"}},
                    "uniformtext": {"minsize": 8, "mode": "hide"}
                },
                {"displayModeBar": false, "responsive": true}
            );
        }
    </script>
</div>

| Dataframe | Questions Completed | Total Time (seconds) | Relative to Daft |
| --------- | :-----------------: | :------------------: | :--------------: |
| Daft      | 10/10               | 785                  | 1.0x             |
| Spark     | 10/10               | 2648                 | 3.3x             |
| Dask      | 10/10               | 6010                 | 7.7x             |
| Modin     | 5/10                | Did not finish       | 44.4x*           |

*\* Only for queries that completed.*

From the results we see that Daft, Spark, and Dask are able to complete all the questions and Modin completes less than half. We also see that Daft is **3.3x** faster than Spark and **7.7x** faster than Dask including S3 IO. We expect these speed-ups to be much larger if the data is loaded in memory instead of cloud storage, which we will show in future benchmarks.

#### 1000 Scale Factor

Next we scale up the data size by 10x while keeping the cluster size the same. Since we only have 244GB of memory and 1TB+ of tabular data, the DataFrame library will be required to perform disk spilling and out-of-core processing for all questions at nearly all stages of the query.

<!-- Find better way to embed html file content, rather than pasting the whole file -->
<div>                        <script type="text/javascript">window.PlotlyConfig = {MathJaxConfig: 'local'};</script>
    <script charset="utf-8" src="https://cdn.plot.ly/plotly-2.20.0.min.js"></script>
    <div id="2e3c4bff-c808-4722-8664-d4c63ee41e55" class="plotly-graph-div" style="height:100%; width:100%;"></div>
    <script type="text/javascript">
        window.PLOTLYENV = window.PLOTLYENV || {};
        if (document.getElementById("2e3c4bff-c808-4722-8664-d4c63ee41e55")) {
            Plotly.newPlot(
                "2e3c4bff-c808-4722-8664-d4c63ee41e55",
                [
                    {
                        "marker": {"color": "rgba(255, 0, 255, 1)"},
                        "name": "Daft",
                        "x": ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8", "Q9", "Q10"],
                        "y": [4.85, 9.766666666666667, 12.933333333333334, 11.233333333333333, 17.616666666666667, 2.7, 15.15, 18.5, 22.833333333333332, 13.983333333333333],
                        "type": "bar",
                        "textposition": "inside"
                    },
                    {
                        "hovertext": ["12.1x Slower", "0.9x Slower", "3.8x Slower", "2.9x Slower", "2.1x Slower", "22.3x Slower", "3.5x Slower", "2.7x Slower", "2.6x Slower", "3.4x Slower"],
                        "marker": {"color": "rgba(226,90,28, 0.75)"},
                        "name": "Spark",
                        "x": ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8", "Q9", "Q10"],
                        "y": [58.625, 8.591666666666667, 48.559999999999995, 32.88666666666667, 36.98166666666667, 60.11333333333334, 52.34, 49.475, 58.26166666666666, 46.85333333333333],
                        "type": "bar",
                        "textposition": "inside"
                    },
                    {
                        "hovertext": ["8.7x Slower", "2.1x Slower", "nanx Slower", "nanx Slower", "nanx Slower", "13.7x Slower", "nanx Slower", "nanx Slower", "nanx Slower", "nanx Slower"],
                        "marker": {"color": "rgba(255,193,30, 0.75)"},
                        "name": "Dask",
                        "x": ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8", "Q9", "Q10"],
                        "y": [42.37166666666667, 20.926666666666666, null, null, null, 36.968333333333334, null, null, null, null],
                        "type": "bar",
                        "textposition": "inside"
                    }
                ],
                {
                    "title": {"text": "TPCH 1000 Scale Factor - 4 Nodes (lower is better)"},
                    "yaxis": {"title": {"text": "Time (minutes)"}},
                    "xaxis": {"title": {"text": "TPCH Question"}},
                    "uniformtext": {"minsize": 8, "mode": "hide"}
                },
                {"displayModeBar": false, "responsive": true}
            );
        }
    </script>
</div>


| Dataframe | Questions Completed | Total Time (seconds) | Relative to Daft |
| --------- | :-----------------: | :------------------: | :--------------: |
| Daft      | 10/10               | 7774                 | 1.0x             |
| Spark     | 10/10               | 27161                | 3.5x             |
| Dask      | 3/10                | Did not finish       | 5.8x*            |
| Modin     | 0/10                | Did not finish       | No data          |


*\* Only for queries that completed.*

From the results we see that only Daft and Spark are able to complete all the questions. Dask completes less than a third and Modin is unable to complete any due to OOMs and cluster crashes. Since we can only compare to Spark here, we see that Daft is **3.5x** faster including S3 IO. This shows that Daft and Spark are the only Dataframes in this comparison capable of processing data larger than memory, with Daft standing out as the significantly faster option.

#### 1000 Scale Factor - Node Count Ablation

Finally, we compare how Daft performs on varying size clusters on the terabyte scale dataset. We run the same Daft TPC-H questions on the same dataset as the [previous section](#1000-scale-factor) but sweep the worker node count.

<!-- Find better way to embed html file content, rather than pasting the whole file -->
<div>                        <script type="text/javascript">window.PlotlyConfig = {MathJaxConfig: 'local'};</script>
    <script charset="utf-8" src="https://cdn.plot.ly/plotly-2.20.0.min.js"></script>
    <div id="8da53ffa-b330-43c6-b32b-a84051abed03" class="plotly-graph-div" style="height:100%; width:100%;"></div>
    <script type="text/javascript">
        window.PLOTLYENV = window.PLOTLYENV || {};
        if (document.getElementById("8da53ffa-b330-43c6-b32b-a84051abed03")) {
            Plotly.newPlot(
                "8da53ffa-b330-43c6-b32b-a84051abed03",
                [
                    {
                        "name": "1 Node",
                        "x": ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8", "Q9", "Q10"],
                        "y": [18.466666666666665, 34.7, 49.516666666666666, 37.583333333333336, 67.01666666666667, 12.133333333333333, 56.18333333333333, 68.68333333333334, 92.1, 57.63333333333333],
                        "type": "bar",
                        "textposition": "inside"
                    },
                    {
                        "name": "4 Node",
                        "x": ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8", "Q9", "Q10"],
                        "y": [4.85, 9.766666666666667, 12.933333333333334, 11.233333333333333, 17.616666666666667, 2.7, 15.15, 18.5, 22.833333333333332, 13.983333333333333],
                        "type": "bar",
                        "textposition": "inside"
                    },
                    {
                        "name": "8 Node",
                        "x": ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8", "Q9", "Q10"],
                        "y": [2.6, 5.933333333333334, 6.583333333333333, 5.083333333333333, 10.2, 1.5, 7.95, 9.733333333333333, 16.666666666666668, 7.183333333333334],
                        "type": "bar",
                        "textposition": "inside"
                    }
                ],
                {
                    "title": {"text": "TPCH 1000 Scale Factor - Node Count vs Daft Query Time"},
                    "yaxis": {"title": {"text": "Time (minutes)"}},
                    "xaxis": {"title": {"text": "TPCH Question"}},
                    "uniformtext": {"minsize": 8, "mode": "hide"}
                },
                {"displayModeBar": false, "responsive": true}
            );
        }
    </script>
</div>

We note two interesting results here:

1. Daft can process 1TB+ of analytical data on a single 61GB instance without being distributed (16x more data than memory).
2. Daft query times scale linearly with the number of nodes (e.g. 4 nodes being 4 times faster than a single node). This allows for faster queries while maintaining the same compute cost!

### Detailed Benchmarking Setup

#### Benchmarking Code

Our benchmarking scripts and code can be found in the [distributed-query-benchmarks](https://github.com/Eventual-Inc/distributed-query-benchmarking) GitHub repository.

- TPC-H queries for Daft were written by us.
- TPC-H queries for SparkSQL was adapted from [this repository](https://github.com/bodo-ai/Bodo/blob/main/benchmarks/tpch/pyspark_notebook.ipynb).
- TPC-H queries for Dask and Modin were adapted from these repositories for questions [Q1-7](https://github.com/pola-rs/tpch) and [Q8-10](https://github.com/xprobe-inc/benchmarks/tree/main/tpch).

### Infrastructure
Our infrastructure runs on an EKS Kubernetes cluster.

<!-- Markdown doesn't support table without header row -->
- **Driver Instance**: i3.2xlarge
- **Worker Instance**: i3.2xlarge
- **Number of Workers**: 1/4/8
- **Networking**: All instances colocated in the same Availability Zone in the AWS us-west-2 region

#### Data
Data for the benchmark was stored in AWS S3.
No node-level caching was performed, and data is read directly from AWS S3 on every attempt to simulate realistic workloads.

- **Storage**: AWS S3 Bucket
- **Format**: Parquet
- **Region**: us-west-2
- **File Layout**: Each table is split into 32 (for the 100SF benchmark) or 512 (for the 1000SF benchmark) separate Parquet files. Parquet files for a given table have their paths prefixed with that table’s name, and are laid out in a flat folder structure under that prefix. Frameworks are instructed to read Parquet files from that prefix.
- **Data Generation**: TPC-H data was generated using the utilities found in the open-sourced [Daft repository](https://github.com/Eventual-Inc/Daft/blob/main/benchmarking/tpch/pipelined_data_generation.py). This data is also available on request if you wish to reproduce any results!

#### Cluster Setup

##### Dask and Ray

To help us run the Distributed Dataframe libraries, we used Kubernetes for deploying Dask and Ray clusters.
The configuration files for these setups can be found in our [open source benchmarking repository](https://github.com/Eventual-Inc/distributed-query-benchmarking/tree/main/cluster_setup).

Our benchmarks for Daft and Modin were run on a [KubeRay](https://github.com/ray-project/kuberay) cluster, and our benchmarks for Dask was run on a [Dask-on-Kubernetes](https://github.com/dask/dask-kubernetes) cluster. Both projects are owned and maintained officially by the creators of these libraries as one of the main methods of deploying.

##### Spark

For benchmarking Spark we used AWS EMR, the official managed Spark solution provided by AWS. For more details on our setup and approach, please consult our Spark benchmarks [README](https://github.com/Eventual-Inc/distributed-query-benchmarking/tree/main/distributed_query_benchmarking/spark_queries).

#### Logs

| Dataframe | Scale Factor | Nodes  | Links                     |
| --------- | ------------ | ------ | ------------------------- |
| Daft      | 1000         | 8      | 1. s3://daft-public-data/benchmarking/logs/daft.0_1_3.1tb.8-i32xlarge.log     |
| Daft      | 1000         | 4      | 1. s3://daft-public-data/benchmarking/logs/daft.0_1_3.1tb.4-i32xlarge.log     |
| Daft      | 1000         | 1      | 1. s3://daft-public-data/benchmarking/logs/daft.1tb.1.i3-2xlarge.part1.log <br> 2. s3://daft-public-data/benchmarking/logs/daft.1tb.1.i3-2xlarge.part2.log    |
| Daft      | 100          | 4      | 1. s3://daft-public-data/benchmarking/logs/daft.0_1_3.100gb.4-i32xlarge.log
| Spark     | 1000         | 4      | 1. s3://daft-public-data/benchmarking/logs/emr-spark.6_10_0.1tb.4-i32xlarge.log
| Spark     | 100          | 4      | 1. s3://daft-public-data/benchmarking/logs/emr-spark.6_10_0.100gb.4-i32xlarge.log.gz
|Dask (failed, multiple retries) | 1000 | 16 | 1. s3://daft-public-data/benchmarking/logs/dask.2023_5_0.1tb.16-i32xlarge.0.log <br> 2. s3://daft-public-data/benchmarking/logs/dask.2023_5_0.1tb.16-i32xlarge.1.log <br> 3. s3://daft-public-data/benchmarking/logs/dask.2023_5_0.1tb.16-i32xlarge.2.log <br> 4. s3://daft-public-data/benchmarking/logs/dask.2023_5_0.1tb.16-i32xlarge.3.log |
| Dask (failed, multiple retries)| 1000 | 4  | 1. s3://daft-public-data/benchmarking/logs/dask.2023_5_0.1tb.4-i32xlarge.q126.log |
| Dask (multiple retries) | 100 | 4 | 1. s3://daft-public-data/benchmarking/logs/dask.2023_5_0.100gb.4-i32xlarge.0.log <br> 2. s3://daft-public-data/benchmarking/logs/dask.2023_5_0.100gb.4-i32xlarge.0.log <br> 3. s3://daft-public-data/benchmarking/logs/dask.2023_5_0.100gb.4-i32xlarge.1.log |
| Modin (failed, multiple retries) | 1000 | 16 | 1. s3://daft-public-data/benchmarking/logs/modin.0_20_1.1tb.16-i32xlarge.0.log <br> 2. s3://daft-public-data/benchmarking/logs/modin.0_20_1.1tb.16-i32xlarge.1.log |
| Modin (failed, multiple retries) | 100  | 4  | 1. s3://daft-public-data/benchmarking/logs/modin.0_20_1.100gb.4-i32xlarge.log |
