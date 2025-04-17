# Installation

To install Daft, run this from your terminal:

```bash
pip install -U daft
```

## Extra Dependencies

Some Daft functionality may also require other dependencies, which are specified as "extras":

To install Daft with the extra dependencies required for interacting with AWS services, such as AWS S3, run:

```bash
pip install -U daft[aws]
```

To install Daft with the extra dependencies required for running distributed Daft on top of a [Ray cluster](https://docs.ray.io/en/latest/index.html), run:

```bash
pip install -U daft[ray]
```

To install Daft with all extras, run:

```bash
pip install -U daft[all]
```

## Legacy CPUs

If you see the text `Illegal instruction` when trying to run Daft, it may be because your CPU lacks support for certain instruction sets such as [AVX](https://en.wikipedia.org/wiki/Advanced_Vector_Extensions). For those CPUs, use the `daft-lts` package instead:

```bash
pip install -U daft-lts
```

!!! tip "Note"
    Because `daft-lts` is compiled to use a limited CPU instruction set, the package is unable to take advantage of more performant vectorized operations. Only install this package if your CPU does not support running the `daft` package.

## Advanced Installation

### Installing Nightlies

If you wish to use Daft at the bleeding edge of development, you may also install the nightly build of Daft which is built every night against the `main` branch:

```bash
pip install -U daft --pre --extra-index-url https://d1p3klp2t5517h.cloudfront.net/builds/nightly
```

### Installing Daft from source

```bash
pip install -U https://github.com/Eventual-Inc/Daft/archive/refs/heads/main.zip
```

Please note that Daft requires the Rust toolchain in order to build from source.
