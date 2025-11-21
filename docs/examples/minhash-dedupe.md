# MinHash Deduplication on Common-Crawl Web Text

<a target="_blank" href="https://colab.research.google.com/github/Eventual-Inc/Daft/blob/main/tutorials/minhash_dedupe/minhash_dedupe_common_crawl.ipynb">
  <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
</a>

In this notebook we will be performing the MinHash Deduplication algorithm over extracted text from html documents in the common crawl dataset. The Common Crawl corpus contains petabytes of data, with its oldest entries dating back to 2008. Each dataset includes raw web page data, metadata extracts, and text extracts. Deduplication is a helpful top-of-funnel strategy for improving dataset quality and is commonly used to improve generalization in LLM training, RAG, and Search.

When implemented as a pipeline, this workload can process 100,000 web pages in under 4 minutes on a MacBook Air (M2) with 8 GB of RAM. That includes preprocessing pages into html blocks, minhash, lsh banding, connected components, and final dedupe!

```text
# of documents loaded:  100000
# of text rows before:  4944669
# of text rows after:   1855814
% of text rows kept:    37.53%
Overall Time:           222.21s
```

## References

- [Connected Components in MapReduce and Beyond](https://dl.acm.org/doi/abs/10.1145/2670979.2670997)
- [On the resemblance and containment of documents](https://ieeexplore.ieee.org/document/666900)
- [Finding Near Duplicates with Jaccard Similarity and MinHash by Nelson Elhage](https://blog.nelhage.com/post/fuzzy-dedup/)


## Table of Contents

- [Quickstart](#quickstart)
- [Loading Common Crawl](#loading-html-documents-from-common-crawl)
- [Preprocessing](#preprocessing)
- [Text Normalization](#text-normalization)
- [MinHash](#minhash)
- [LSH Banding](#lsh-banding)
- [Connected Components](#connected-components)
- [Validation with igraph](#validation-with-igraph)
- [Merge Results](#merge-results)
- [Conclusion](#conclusion)

## Quickstart

```python
# Install dependencies (skip if using uv - dependencies are already in pyproject.toml)
# For Google Colab or standalone environments:
!pip install 'daft[aws,pandas]' selectolax scipy matplotlib igraph
```

### Define Key Parameters

```python
NUM_ROWS = 500
index_col = "block_id"
content_col = "block"

# Minhash Parameters
K = 64 # Number of Permutations
SEED = 42 # Seed for the hash function
NGRAM_SIZE = 5 # Size of the n-grams
LSH_THRESHOLD = 0.7 # Jaccard Similarity Threshold for LSH
```

## Loading HTML Documents from Common Crawl

We will be accessing Common Crawl through [WARC files](https://commoncrawl.org/blog/navigating-the-warc-file-format) since [Daft supports the format natively][daft.read_warc].

### (Optional) AWS Authentication

Crawl data is free to access by anyone from anywhere. The data is hosted by Amazon Web Services’ Open Data Sets Sponsorships program on the bucket s3://commoncrawl/, located in the US-East-1 (Northern Virginia) AWS Region. The most performant means of accessing Common Crawl is through S3, so if you plan to process a lot of data you'll want to authenticate with a `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.

However, Common Crawl data can also be accessed without authentication, anonymously via its HTTP endpoint.

```python
import daft
from daft.io import IOConfig, S3Config
import os
from dotenv import load_dotenv
from IPython.display import clear_output
```

```python
IN_AWS = False
if os.environ.get("AWS_ACCESS_KEY_ID"):
    # Make sure to define your AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in your environment variables or in a .env file
    s3_config = S3Config(
        region_name="us-east-1",
        requester_pays=True,
        key_id=os.environ["AWS_ACCESS_KEY_ID"],
        access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        anonymous=False,
    )
    IN_AWS = True
    IO_CONFIG = IOConfig(s3=s3_config)
    daft.set_planning_config(default_io_config=IO_CONFIG)
```

```python
# Read the WARC files from the Common Crawl S3 bucket or HTTP endpoint
df_warc = daft.datasets.common_crawl(
    "CC-MAIN-2025-33",
    in_aws=IN_AWS
).limit(NUM_ROWS).collect()
df_warc.show(3)
```

```python
# Let's investigate the different types of payloads we have:
df_warc.select("WARC-Identified-Payload-Type").distinct().show()
```

## Preprocessing

Since we are primarily concerned with text, we will focus on `text/html` payloads, extracting text content from html body and normalizing the text itself. Common Crawl also comes with text only .wet files that come preprocessed, but here we choose to handle each html block explicitly to ensure consistent comparisons across pages.

```python
from daft import col

# Define a UDF to remove http headers from the payload
@daft.func()
def remove_http_headers(x: str) -> str:
    """Remove HTTP headers from input string by splitting on double CRLF, returning the body or empty string."""
    if x is None:
        return ""
    if len(x.split("\r\n\r\n")) > 1:
        return x.split("\r\n\r\n")[1]
    return ""

# Filter the dataframe to only include text/html payloads
df_html = df_warc.where(col("WARC-Identified-Payload-Type")== "text/html")

# Separate the HTTP headers from the payloads
df_html = (
    df_html
    .with_column("content_raw", remove_http_headers(col("warc_content").try_decode("utf-8")))
    .where(col("content_raw") != "")
)
```

### Extracting Text from HTML

```python
from selectolax.parser import HTMLParser

# Define a UDF to extract text from HTML content, specifically (article, main, p, h1, h2, h3, li)
@daft.func()
def extract_blocks(html: str) -> list[str]:
    tree = HTMLParser(html)
    for n in tree.css("script,style,noscript"):
        n.decompose()

    blocks = []
    for node in tree.css("""title, article, main, p, h1, h2, h3, h4, h5, h6, li, div, section, img[alt], figcaption, caption, blockquote, table th, table td, pre, code, summary, meta[name="description"], meta[property="og:title"], meta[property="og:description"]"""):
        txt = node.text(separator=" ", strip=True)
        if txt:
            blocks.append(txt)
    return blocks

@daft.func()
def get_block_idx(blocks: list[str]) -> list[int]:
    return list(range(len(blocks)))

df_text = (
    df_html
    .with_column("blocks", extract_blocks(col("content_raw")))
    .with_column("block_idx", get_block_idx(col("blocks")))
    .explode("blocks", "block_idx")
    .where(col("blocks") != "")
    .where(col("blocks").not_null())
    .with_column(index_col, col("WARC-Record-ID")+ "-" + col("block_idx"))
    .with_column(content_col, col("blocks"))
    .select(
        "WARC-Record-ID",
        index_col,
        content_col,
    )
)
df_text = df_text.collect()
df_text.show(3)
```

```python
# Drop unneeded columns
df_ready = (
    df_text
    .select(index_col, content_col)
)
```

### Text Normalization

So far we have extracted the text out of each html document into blocks. Now we move to normalize the text blocks to prepare for the MinHash operation.

*Note: It is recommended to run your preprocessing pipeline separately from your MinHash deduplication workload.*

See docs: [normalize][daft.Expression.normalize]

```python
# Normalize text
df_norm = df_ready.with_column("content_normalized",
    col(content_col).str.normalize(
        remove_punct=True,
        lowercase=True,
        nfd_unicode=True,
        white_space=True
    )
)
df_norm.select(index_col, content_col, "content_normalized").show(3)
```

### MinHash

Normally when you perform a MinHash on text data, you have to define the shingling strategy, hash functions, and permutation parameters manually.

Luckily, Daft has a built-in [MinHash expression][daft.Expression.minhash].

```python
# Calculate the MinHash vectors
df_minhash = (
    df_norm
    .with_column("min_hashes", col("content_normalized").minhash(
        num_hashes = K,
        ngram_size = NGRAM_SIZE,
        seed = SEED,
        hash_function = 'xxhash'
        )
    )
)
df_minhash.select(index_col, content_col, "min_hashes").show(3)
```

## LSH Banding

LSH Banding involves splitting each document's MinHash signature into bands and rows, where documents with identical bands are considered candidate pairs for similarity comparison. This technique dramatically reduces the number of comparisons needed by only comparing documents that share at least one identical band, making near-duplicate detection scalable for large datasets

Next, we will:

1. Use the optimal_param function to determine the best band (b) and row (r) parameters for our LSH bucketing
2. Split each document's MinHash vector into `B` bands of `R` rows each
3. Create buckets by hashing each band's signature, grouping similar documents together

```python
from scipy.integrate import quad as integrate

def optimal_param(
    threshold: float,
    num_perm: int,
    false_positive_weight: float = 0.5,
    false_negative_weight: float = 0.5,
):
    """
    Compute the optimal `MinHashLSH` parameter that minimizes the weighted sum
    of probabilities of false positive and false negative, taken from datasketch.

    Parameters
    ----------
    threshold : float
        The threshold for similarity.
    num_perm : int
        The number of permutations.
    false_positive_weight : float
        The weight of false positive.
    false_negative_weight : float
        The weight of false negative.

    Returns
    -------
    Tuple[int, int]
        The optimal `b` and `r` parameters.
        The number of bands, and the number of rows per band respectively.

    Examples
    --------
    >>> optimal_param(0.7, 256)
    (25, 10)
    """

    def false_positive_area(threshold: float, b: int, r: int):
        """Source: `datasketch.lsh`"""

        def area(s):
            return 1 - (1 - s ** float(r)) ** float(b)

        a, _ = integrate(area, 0.0, threshold)
        return a

    def false_negative_area(threshold: float, b: int, r: int):
        """Source: `datasketch.lsh`"""

        def area(s):
            return 1 - (1 - (1 - s ** float(r)) ** float(b))

        a, _ = integrate(area, threshold, 1.0)
        return a

    min_error = float("inf")
    opt = (0, 0)
    for b in range(1, num_perm + 1):
        max_r = int(num_perm / b)
        for r in range(1, max_r + 1):
            fp = false_positive_area(threshold, b, r)
            fn = false_negative_area(threshold, b, r)
            error = fp * false_positive_weight + fn * false_negative_weight
            if error < min_error:
                min_error = error
                opt = (b, r)
    return opt
```

```python
# Choose B bands and R rows per band such that B · R = num_perm.
B, R = optimal_param(LSH_THRESHOLD, K) # Default 0.7 , 64
print(B, R, K)

# Verify that B * R = K
assert B * R == K
```

Before we move to band generation, we need to hash our `index_col` to `int` to make downstream processing easier.
We will keep track of the map and introduce a new column with a monotonically increasing id.

```python
from daft.functions import monotonically_increasing_id

df_minhash = df_minhash.with_column("node_id", monotonically_increasing_id())
id_map = df_minhash.select(index_col, "node_id").distinct()
```

### LSH Band Generation

**Previously** we calculated the MinHashes for our `content_text` where we hashed each word token into an 8-byte integer, taking only 64 samples (at a uniform random sample).

**Next** we take those 64 hashes and chunk them into 8 lists of 8 values.

```python
# Band Generation
df_bands = (
    df_minhash
    .with_column("bands", col("min_hashes").list.chunk(R))
)
df_bands.select(index_col, content_col, "min_hashes", "bands").show(3)
```

**Now** we will explode our bands into new rows, keeping track of their position in the band using `band_idx`.

```python
@daft.func()
def get_band_idx(band: list[int], B: int) -> list[int]:
    return list(range(min(len(band), B)))

df_bands_exploded = (
    df_bands
    .with_column("band_idx", get_band_idx(col("bands"), B))
    .explode("bands", "band_idx")
)
df_bands_exploded.select("node_id", "band_idx", "bands").show(3)
```

### Grouping bands

We then group the bands against their 'signature', which is a combination of their band index and the band itself. If two segments are duplicates, we expect their signatures to match.

```python
# Grouping Bands
df_grouped = (
    df_bands_exploded
    .groupby(col("band_idx"), col("bands"))
    .agg(col("node_id").agg_list().alias("nodes"))
)
df_grouped.select("band_idx", "bands", "nodes").show(3)
```

```python
# Inspecting bands with multiple nodes
df_grouped.where(col("nodes").list.length() > 1).select("band_idx", "bands", "nodes").show(3)
```

## Connected Components

Every band whose **nodes** have more than one entry are now candidates for consideration. But there is something wrong... Our nodes are repeated across different band indices!

In order to reduce our candidates into their unique set, we leverage a few tricks from graph theory to isolate the duplicates. Here we get to implement one the most important algorithms in distributed computing. [*Connected Components in MapReduce and Beyond*](https://dl.acm.org/doi/pdf/10.1145/2670979.2670997) is a seminal paper from 2014 written by researchers at Google.

We’ll follow the paper’s star‑contraction recipe: alternate a Large‑star and Small‑star pass that repeatedly points each node to the smallest ID in its neighborhood. After a few rounds the edge set stabilizes; the “parent” each node points to is its component representative.

Concretely, we’ll collapse band groups into a simple graph:

- Treat each document as a node.
- For every band with multiple nodes, connect each node to the group’s minimum ID (drop self-loops and duplicates).
- This produces an undirected edge list that captures “co-occurred somewhere” linkage.

From there we use star-contraction (Kiveris et al., 2014) to snap clusters together:

- Large-star: for each node, point to the smallest ID in its neighborhood (including itself). Emit edges (v, m(u)) only where v > u.
- Small-star: canonicalize edges so u ≥ v, recompute the same “point to the minimum,” and emit (v, m(u)) for all neighbors.

Repeat Large-star then Small-star until the edge set stops changing. The final “parent” each node points to is its component representative (typically the component’s minimum ID). It’s fast, scalable, and after a handful of rounds, the clusters just fall out!

```python
# First we must convert our list of nodes into an edge list
df_edges = (
    df_grouped
    .with_column("u", col("nodes").list.min())
    .explode("nodes")
    .select("u", v=col("nodes"))
    .where(col("u") != col("v"))
    .where(~col("u").is_null())
    .where(~col("v").is_null())
    .distinct()
)
df_edges.show(5)
```

First we need a few utilities

```python
from daft import struct, Expression, DataFrame

def ee(u: Expression, v: Expression):
    """Create a struct Expression with fields 'u' and 'v' for representing edges."""
    return struct(u.alias("u"), v.alias("v"))

def canonicalize(edges: DataFrame) -> DataFrame:
    """Order edges so u < v and deduplicate for canonical representation."""
    return (
        edges
        .with_column("u_can", (col("u") < col("v")).if_else(col("u"), col("v")))
        .with_column("v_can", (col("u") < col("v")).if_else(col("v"), col("u")))
        .select(col("u_can").alias("u"), col("v_can").alias("v"))
        .distinct()
    )

def symmetrize(edges: DataFrame) -> DataFrame:
    """Make edge list undirected by adding reverse edges."""
    return (
        edges
        .select("u", "v")
        .union_all(edges.select(col("v").alias("u"), col("u").alias("v")))
        .collect()
    )

def pairs_equal(a: DataFrame, b: DataFrame) -> bool:
    """Check if two DataFrames have identical (u, rep) pairs via anti-joins."""
    left_minus  = a.join(b, on=["u","rep"], how="anti").count_rows()
    right_minus = b.join(a, on=["u","rep"], how="anti").count_rows()
    return (left_minus == 0) and (right_minus == 0)
```

### The Alternating Algorithm - Star Contraction with Daft

We will iteratively compress the graph using two alternating phases until convergence:

- Large-star: Every node points to the minimum ID in its neighborhood (including itself). This quickly pulls nodes toward low-ID “hubs.”
- Small-star: Re-orient edges to ensure u < v (canonicalize) and repeat contraction, which merges local hubs together.
- Repeat large-star then small-star until nothing changes. The “parent” each node ends up pointing to is its component representative.

### Large-star

- Group neighbors by u.
- Compute min_neighbor = min(neighbors).
- Use min(u, min_neighbor) as the node’s “parent.”
- Emit edges (u, parent) but only where parent > u to avoid self-loops and duplicates.

```python
def large_star(edges: DataFrame) -> DataFrame:
    """Perform large-star operation: connect nodes to min in extended neighborhood."""
    # 1. Emit U,V and V,U
    undirected = (
        edges
        .select("u", "v")
        .union_all(edges.select(col("v").alias("u"), col("u").alias("v")))
        .collect()
    )

    # Step 2: Group by u, and aggregate the list of v's
    neigh = (
        undirected
        .groupby("u").agg_list("v")
        .with_column("nbrs", col("v"))
    )

    # Step 3: Compute m = min over nbrs union {u}
    neigh = neigh.with_column("m", col("nbrs").list.min())
    neigh = neigh.with_column(
        "m",
        col("m").is_null().if_else(
            col("u"),
            (col("u") < col("m")).if_else(col("u"), col("m"))
        )
    )

    # Step 4: Emit (v, m(u)) for v > u
    out = (
        neigh.explode("nbrs")
            .where(col("nbrs") > col("u"))
            .select(col("nbrs").alias("u"), col("m").alias("v"))
            .where(col("u") != col("v"))
            .distinct()
            .collect()
    )

    return out

```

### Small-star

- Re-orient all edges so u < v (canonical).
- Group neighbors by u, compute min_neighbor, connect (u, parent) like above.
- This step merges local minima across previously separate stars.

```python
def small_star(edges: DataFrame) -> DataFrame:
    """Perform small-star operation: connect to min in direct smaller neighborhood."""
    # Step 1: For each edge, emit to the larger node as key, smaller as value
    directed =  (
        edges.select(
            (col("u") < col("v")).if_else(
                ee(col("u"), col("v")),
                ee(col("v"), col("u"))
            ).alias("e"))
        .select(col("e")["*"])
        .where(col("u") != col("v"))
        .distinct()
    )

    # Step 2: Group by larger u, nbrs are smaller neighbors
    neigh = (
        directed
        .groupby("u").agg_list("v")
        .with_column("nbrs", col("v"))
    )

    # Step 3: Compute m = min over nbrs union {u}
    neigh = neigh.with_column("m", col("nbrs").list.min())
    neigh = neigh.with_column(
        "m",
        col("m").is_null().if_else(
            col("u"),
            (col("u") < col("m")).if_else(col("u"), col("m"))
        )
    )

    # Emit (v, m(u)) for all v in N(u)
    out = (
        neigh.explode("nbrs")
            .select(col("nbrs").alias("u"), col("m").alias("v"))
            .where(col("u") != col("v"))
            .distinct()
            .collect()
    )

    return out
```

### Convergence check - Canonical Set Equality (strict)

- Compare a stable summary of edges before/after
- If stable, stop; otherwise repeat.

```python
def check_canonical_set_equality(prev_edges: DataFrame, curr_edges: DataFrame) -> bool:
    """Check if two edge DataFrames represent the same set after canonicalization."""
    prev_can = canonicalize(prev_edges).collect().to_pydict()
    curr_can = canonicalize(curr_edges).collect().to_pydict()
    prev_set = set(zip(prev_can["u"], prev_can["v"]))
    curr_set = set(zip(curr_can["u"], curr_can["v"]))
    return prev_set == curr_set
```

### Full Algorithm

```python
# The Alternating Algorithm
b = df_edges
while True:
    a = large_star(b)
    b_next = small_star(a)

    if check_canonical_set_equality(b, b_next):
        b = b_next
        break
    b = b_next

b_final = b
```

### Constructing Component Assignments

After the alternating star operations converge, we have a **stable edge list** that implicitly defines connected components.
The final step is to turn this edge list into an explicit **assignment table**:
`[node_id → component_representative]`.

We do this in three small, deterministic steps:

1. **Collect every node** that appears in the graph (sources *and* destinations).
2. **For each node**, find the **smallest node ID** it is directly connected to (its tentative root).
   - Nodes with no outgoing edges simply become their own root.
3. **Materialize the result** as a DataFrame with columns `["u", "rep"]`, where
   - `u` is the original node ID,
   - `rep` is the globally smallest node in its component (the canonical representative).

This table is what we use to filter duplicates: keep only the row whose `index` equals its `rep`, discarding the rest.

```python
# Build the set of all unique node IDs that appear in the edge list
# (both as source 'u' and destination 'v')
nodes = (
    b.select(col("u").alias("u"))          # grab all source nodes
        .union_all(b.select(col("v").alias("u")))  # grab all destination nodes
        .distinct()                           # deduplicate to get unique nodes
)

# For every node, compute the smallest node ID it is connected to
# (i.e., its tentative representative / root in the current component)
rep_map = (
    b
    .groupby("u")                          # group edges by source node
    .agg(col("v").min().alias("rep"))      # find the smallest neighbor
)

# Join each node with its tentative representative.
# Nodes that have no outgoing edges (and thus no entry in rep_map)
# become their own representative.
assignments = (
    nodes
    .join(rep_map, on="u", how="left")     # left join to keep all nodes
    .with_column(
        "rep",
        col("rep").is_null()               # if no neighbor was found
        .if_else(col("u"), col("rep"))     # use the node itself as rep
    )
    .select("u", "rep")                    # keep only node and its rep
    .distinct()                            # deduplicate any duplicates
    .collect()                             # materialize the result
)

assignments.show()
```

## Validation with igraph

[igraph](https://python.igraph.org) is a high-performance graph analysis library that provides robust implementations of fundamental graph algorithms. We use it here as our ground truth for connected component detection because:

- **Battle-tested**: igraph's connected components algorithm has been validated across millions of use cases
- **Deterministic**: It guarantees consistent results regardless of node ordering or edge insertion sequence
- **Efficient**: Handles large graphs with millions of nodes/edges using optimized C implementations
- **Simple API**: Provides direct access to component membership without manual traversal
By comparing our Daft-based implementation against igraph's results, we ensure our custom connected components logic correctly identifies all weakly connected subgraphs in the duplicate detection pipeline.

See docs: [Connected Components](https://python.igraph.org/en/main/tutorials/connected_components.html)

```python
import igraph as ig
import pandas as pd

# Ensure integer dtype and materialize edges
pdf_edges = (
    df_edges
    .select(col("u").cast(daft.DataType.int64()), col("v").cast(daft.DataType.int64()))
    .where(~col("u").is_null()).where(~col("v").is_null())
    .to_pandas()
)

# Build explicit vertex list and index mapping to avoid dtype/label ambiguity
unique_nodes = pd.unique(pd.concat([pdf_edges["u"], pdf_edges["v"]], ignore_index=True))

# Convert to Python ints for stable hashing
node_ids = [int(x) for x in unique_nodes.tolist()]
id_to_idx = {nid: idx for idx, nid in enumerate(node_ids)}

# Map edges to contiguous indices
edges_idx = [(id_to_idx[int(u)], id_to_idx[int(v)]) for u, v in zip(pdf_edges["u"], pdf_edges["v"])]
g = ig.Graph(n=len(node_ids), edges=edges_idx, directed=False)
comps = g.connected_components(mode="weak")

# We can inspect the components to see how many there are and what they look like
print(comps)
```

### Visualizing what a connected component looks like (Top 50)

```python
import matplotlib.pyplot as plt

# Just grab the top 10 most connected nodes and their neighbors
# Get component sizes and sort by number of nodes
comp_sizes = [(i, len(comp)) for i, comp in enumerate(comps)]
top_comp_indices = [i for i, _ in sorted(comp_sizes, key=lambda x: x[1], reverse=True)[:10]]

# Get all nodes from these top components
top_nodes = set()
for comp_idx in top_comp_indices:
    comp_nodes = comps[comp_idx]
    top_nodes.update(comp_nodes)

# Create subgraph from these nodes
subgraph = g.subgraph(list(top_nodes))

fig, ax = plt.subplots(figsize=(10, 10))

layout = subgraph.layout_fruchterman_reingold()
ig.plot(
    subgraph,
    target=ax,
    layout=layout,
    vertex_size=5,
    palette=ig.RainbowPalette(),
    vertex_color=list(map(int, ig.rescale(comps.membership, (0, 250)))),
    edge_width=0.5,
    bbox=(1000, 1000),   # bigger drawing box
    margin=40,
    autocurve=True,      # curves to reduce edge overlap
)
```

### Validating Results against iGraph

```python
ours_grouped = (
    assignments
    .groupby("rep")
    .agg(col("u").agg_list().alias("members"))
    .collect()
)
pdf = ours_grouped.to_pandas()
ours_comps = {frozenset(m) for m in pdf["members"]}
ig_comps = {frozenset(node_ids[i] for i in comp) for comp in comps}
if ours_comps == ig_comps:
    print(f"[VALIDATION] PASSED: components match igraph (n={len(ours_comps)})")
else:
    only_ours = ours_comps - ig_comps
    only_ig = ig_comps - ours_comps
    def _preview(sets, k=3):
        out = []
        for comp in list(sets)[:k]:
            out.append(sorted(list(int(c) for c in comp))[:10])
        return out
    print(f"[VALIDATION] MISMATCH: ours={len(ours_comps)} vs igraph={len(ig_comps)}")
    print(f"  examples only in ours: {_preview(only_ours)}")
    print(f"  examples only in igraph: {_preview(only_ig)}")
```

### Getting our results to match: Global minimum label propagation

Why this is needed:

- After alternating Large-/Small-Star and applying path compression, components can still
    stabilize with multiple local minima (distinct labels) within the same true component.
- This deterministic min-label diffusion ensures every node in a connected component adopts
    the single global minimum node-id as its representative, restoring exact parity to igraph.

**Algorithm:**

1. Symmetrize edges to build an undirected adjacency (both directions present).

2. Initialize labels(u) from assignments.rep.
3. Iterate up to lp_max_iters times:
    - For each node, compute nbr_min(u) = min(label(v)) over neighbors v of u.
    - Update label(u) = min(label(u), nbr_min(u)) with null-safe handling.
    - Deduplicate and compare to prior labels; stop when the (u, label) pair set stabilizes.
4. Return labels as assignments with schema ["u", "rep"].

```python
# Build an undirected view of the graph so labels can flow in both directions
E = symmetrize(b_final)

# Initialize labels from current assignments: rep becomes the working label per node
labels = assignments.select(col("u"), col("rep").alias("label")).collect()

lp_iters = 0
lp_max_iters = 100
while lp_iters < lp_max_iters:
    lp_iters += 1

    # For each node u, compute the minimum label among its neighbors
    nbr_min = (
        E
        .join(labels, left_on="v", right_on="u", how="left")
        .select(col("u").alias("node"), col("label"))
        .groupby("node")
        .agg(col("label").min().alias("nbr_min"))
        .collect()
    )

    # Lower each node's label to min(current_label, neighbor_min_label)
    labels_next = (
        labels
        .join(nbr_min, left_on="u", right_on="node", how="left")
        .with_column(
            "label",
            col("nbr_min").is_null().if_else(
                col("label"),
                (col("label") <= col("nbr_min")).if_else(col("label"), col("nbr_min")),
            ),
        )
        .select(col("u"), col("label"))
        .distinct()
        .collect()
    )

    # Convergence: compare pair sets after casting back to (u, rep)
    a = assignments.select(col("u"), col("rep").alias("label")).select(col("u"), col("label").alias("rep"))
    b = labels_next.select(col("u"), col("label").alias("rep"))
    if pairs_equal(a, b):
        break

    # Continue iterating with updated assignments/labels
    assignments = labels_next.select(col("u"), col("label").alias("rep")).collect()
    labels = labels_next

assignments_globally_reduced = assignments
```

Checking one more time

```python
ours_grouped = (
    assignments_globally_reduced
    .groupby("rep")
    .agg(col("u").agg_list().alias("members"))
    .collect()
)
pdf = ours_grouped.to_pandas()
ours_comps = {frozenset(m) for m in pdf["members"]}
ig_comps = {frozenset(node_ids[i] for i in comp) for comp in comps}
if ours_comps == ig_comps:
    print(f"[VALIDATION] PASSED: components match igraph (n={len(ours_comps)})")
else:
    only_ours = ours_comps - ig_comps
    only_ig = ig_comps - ours_comps
    def _preview(sets, k=3):
        out = []
        for comp in list(sets)[:k]:
            out.append(sorted(list(int(c) for c in comp))[:10])
        return out
    print(f"[VALIDATION] MISMATCH: ours={len(ours_comps)} vs igraph={len(ig_comps)}")
    print(f"  examples only in ours: {_preview(only_ours)}")
    print(f"  examples only in igraph: {_preview(only_ig)}")
```

## Merge Results

```python
# First, create a mapping from node IDs to their string representations
assignments_unique = (
    assignments_globally_reduced
    .groupby("u")
    .agg(col("rep").min())
)
```

```python
# Join the assignments with the ID mapping to get string representations
a1 = assignments_unique.join(id_map.with_column_renamed(index_col, "__u_str"), left_on="u", right_on="node_id")
a2 = a1.join(id_map.with_column_renamed(index_col, "__rep_str"), left_on="rep", right_on="node_id")
assignments_unique_str = a2.select(
    col("__u_str").alias(index_col),
    col("__rep_str").alias("component")
)
assignments_unique_str.show()
```

```python
# Filter to columns of interest
df_content = df_text.select("WARC-Record-ID", index_col, "block")

# Join back to original df and filter to keep only rows where the row is its own representative or isolated
df_joined = df_content.join(assignments_unique_str, on=index_col, how="left").collect()
```

```python
# Return the deduplicated dataset with only unique representatives
deduplicated_df = (
    df_joined
    .filter(
        col("component").is_null() |
        (col("component") == col(index_col))
    )
    .exclude("component")
).collect()
deduplicated_df.show()
```

```python
# Create a dataframe of the duplicates by filtering for rows that are NOT their own representative
duplicates_df = (
    df_joined
    .filter(
        col("component").not_null() &
        (col("component") != col(index_col))
    )
    .where(
        col("block")!=" ")
    .exclude("component")
).collect()

print(f"Found {len(duplicates_df)} duplicate blocks")
print("\nSample of duplicates:")
duplicates_df.show(10)
```

## Conclusion

In this notebook, we built an end‑to‑end, scalable deduplication pipeline for web text:

- Ingested WARC from Common Crawl (S3) and extracted meaningful HTML blocks
- Normalized text to a consistent, noise‑reduced representation
- Computed MinHash signatures (K, ngram_size, seed) and bucketed with LSH (optimal B, R)
- Collapsed buckets into a graph and found connected components via alternating Large‑/Small‑Star
- Verified components against igraph and applied global minimum label propagation for a single representative per component
- Produced both a deduplicated dataset and a duplicates table for inspection

Why this is helpful/important

- Reduces memorization and regurgitation in LLMs, improving generalization and safety
- Eliminates redundant tokens to lower training cost and sharpen downstream evaluations
- Transparent, parameterized method that scales with Daft and S3; easy to tune and reproduce

Where to take it next

- Your use case will most likely specialize in a specific domain or area of expertise so filter content for what's most relevant to you.
- Experiment with Tuning K and the LSH similarity threshold to balance recall vs precision at your scale
- Persist intermediate artifacts (MinHashes, bands, edges) to accelerate iterations
- Add domain/language filters and stronger boilerplate removal before hashing
- Operationalize as a scheduled job over new Common Crawl snapshots
- Integrate additional quality signals (toxicity, heuristics) pre‑/post‑dedup

Outputs to expect

- A deduplicated view (`deduplicated_df`) ready for downstream training
- A `duplicates_df` sample to spot‑check clusters and validate quality
