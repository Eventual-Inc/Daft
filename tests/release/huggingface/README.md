# Hugging Face release audit

This audit runs on release tags before PyPI publication, or through manual dispatch of `huggingface-release-audit.yml`. It does not join normal PR or nightly integration runs. The manual workflow builds a candidate wheel first; the release workflow uses the existing build artifact.

The catalog has five revision-pinned public repositories per category: CSV, JSON/JSONL, native Parquet, HF-converted ("optimized") Parquet, ImageFolder, AudioFolder, text, Arrow files, and uncompressed WebDataset TAR. Converted snapshots use the existing native Parquet reader. Arrow files use the optional `datasets` fallback, not Flight. Source-format cases explicitly force the fallback so a successful viewer conversion cannot conceal a source-loader failure.

AudioFolder preparation in the pinned `datasets` version requires its optional audio dependencies: PyTorch, TorchCodec, and FFmpeg shared libraries. The workflow installs matching CPU-only PyTorch/TorchCodec versions explicitly. These are not silently added to Daft's ordinary HF extra. For local audio audits, install `datasets[audio]` and a compatible FFmpeg installation; see the [HF installation guide](https://huggingface.co/docs/datasets/en/installation) and [TorchCodec compatibility table](https://github.com/meta-pytorch/torchcodec#compatibility-with-torch-versions).

Each case verifies repository/revision resolution, a nonempty schema and result, projection/filter/limit, required columns where declared, and image/audio payload decoding where declared. The entire repository must be under 50 MB with known file sizes before an eager fallback: `.limit()` does not bound that work. Native WebDataset cases with an explicit directory use the pinned directory's file sizes instead. A separate process bounds each case to 120 seconds and isolates HF caches and implicit credentials. Network retries use `tests/_hf_retry.py`, but exhausted retries fail rather than skip. No failing format case is silently removed.

Discovery exclusions: `Sh1man/example_webdataset` at `79e63333fc941b5842020c4559fcaee4400a2b31` and `Gatozu35/test-webdataset` at `f6e9e729b3bec22357cf1bce24b75a2fb8d0c2fd` contain gzip-compressed content despite `.tar` names. Live native reads rejected both with the expected unsupported-compression error. They are not positive uncompressed-format cases; compressed support remains a documented limitation, not a passing compatibility claim.

The `diffusion-course` directory of `huggingface-course/documentation-images` at `96e71ad956904da670e6827274fc2c9b735b09d5` is mixed-format: HF's source-loader inference selected a text dataset, not ImageFolder. It is not an ImageFolder-positive case; format tags and a directory containing pictures alone are insufficient. A JSONL-metadata ImageFolder repository replaces that candidate. Explicit source-builder/file-pattern selection for ambiguous repositories is outside this API change.

WebDataset cases temporarily use `scantask_max_parallel=1`. They validate the serial workaround, **not** default-concurrency safety. The separate concurrency bug is not fixed by this suite.

Local source-build audit:

```sh
.venv/bin/python .github/ci-scripts/huggingface_release_audit.py --validate
.venv/bin/python .github/ci-scripts/huggingface_release_audit.py --output .context/hf-audit-local
```

For a built-wheel environment, add `--require-wheel`. Workers use Python's isolated mode and reject importing Daft from the checkout. For diagnosis, `--category imagefolder` runs one category; category-only results are marked as incomplete coverage and are not what the release workflow runs.

The output directory contains a JSON report plus per-case logs and successful schemas, revisions, version/module provenance, and timings. Publication is blocked by errors, timeouts, and persistent HF outages. Review that gate policy before merging. Catalog maintenance requires selecting a public immutable replacement, checking its format and byte budget, running it locally, and preserving five cases per category; pin refreshes are intentional reviewable changes.

This is a bounded compatibility audit, not an exhaustive corpus-correctness, throughput, Ray, authentication, or concurrency benchmark. The mocked contract tests separately cover partial conversion, missing selections, auth failures, and metadata invariants. BenchCAD's image annotations have an additional local live check recorded in the image PR.
