# Developer guide to this crate

This crate follows the standard for developing a Rust library via `cargo`.
The CI is our "ground truth" over the state of the library. Check out the different parts of
the CI to understand how to test the different parts of this library locally.

## Git clone with submodules
The crate comes with additional submodules to aid with testing, to ensure you have them if you plan on testing, using `--recurse-submodules` will clone the submodules alongside the repository.

```bash
git clone --recurse-submodules https://github.com/jorgecarleitao/arrow2
```

## Checks

PRs will run the following checks:

```bash
cargo fmt --all -- --check
cargo clippy --all --features=full --tests -- -D warnings
```

## Testing

The simplest way to test the crate is to run

```bash
cargo test --tests
```

This runs the tests of the crate without features. To run all features, use

```bash
cargo test --features full
```

If you get warnings about parquet files not existing, you can generate the test files by using Python:

```bash
# Ubuntu: sudo apt install python3-pip python3-venv
# Mac: brew install python3
# Archlinux: sudo pacman -Syu python

# Create a virtual environment for python, to keep dependencies contained
python3 -m venv venv

# Activate the virtual environment
source venv/bin/activate

# Make sure pip is up to date
pip install pip --upgrade

# Install pyarrow, version 6
pip install pyarrow==6 pyorc

# Generate the parquet files (this might take some time, depending on your computer setup)
python parquet_integration/write_parquet.py
# generate ORC files
python tests/it/io/orc/write.py

# Get out of venv, back to normal terminal
deactivate
```

If you receive warnings about other files not found (IPC), ensure you have all submodules:
```bash
# If you didn't clone with `git clone --recurse-submodules https://github.com/jorgecarleitao/arrow2`
git submodule update --init --recursive

# Update to the latest submodules
git submodule update --recursive --remote
```

during development of particular parts of the crate, it is usually faster
to reduce the feature set - the tests are gated to only the relevant tests
of that feature set. For example, if improving JSON, you can use

```bash
cargo test --tests --features io_json
```

to only run both non-feature-specific tests and tests for json.

## Merging

We currently do not have maintenance versions and thus only PR and merge to `main`.

We use labels to build a changelog - it is very important to label issues and/or PRs
accordingly. Because our changelog can contain both issues and PRs, when a PR closes
an issue, we favor having the PR on the changelog, since it includes a reference to
the author (credits).

Summary:
* pull requests with both backward-incompatible changes and new
  features/enhancements MUST close at least one issue (the one
  documenting the backward-incompatible change)
* Every other pull request MAY close one issue

issues are only used to document situations whose a single PR adds two entries to
the changelog (e.g. a backward incompatible change + a new enhancement).

Merging a PR to main has the following checklist:

1. Does it close an issue? If yes, add the label `no-changelog` to the issue.
2. Label the PR accordingly (`Testing`, `Documentation`, `Enhancement`, `Feature`, `Bug`)
3. If the PR is backward incompatible:
    1. create a new issue labeled `backward-incompatible` with what changed and how to migrate
       from the old API to the new API
    2. Edit the PR's description with `Closes #...`
4. Adjust the PRs title with a description suitable for the changelog
    * In the past tense
    * backtick code names (e.g. `MutableArray`, not MutableArray)
    * place "(-X%)" or "(Yx)" at the end if it is a performance improvement
5. Press merge (squash merge) and adjust any items added by github to your liking

This will reduce the burden of a release, where we go through every item from the
changelog and adjust titles, labels, etc.

<!---
To be completed.
## Releases

Releasing this library is done by the following steps:

1. Identify or create the commit to release
2. Identify the version to apply to it
3. Create a changelog (see below)
4. Verify that the version is consistent with the changelog
5. Bump the version accordingly
6. Commit the bump and changelog
7. Tag the commit
8. publish to [crates.io](https://crates.io)

## 1. Identify or create commit to release

If from the main branch, it is usually a minor release

## How to generate the changelog

```bash
docker run -it --rm -v "$(pwd)":/usr/local/src/your-app githubchangeloggenerator/github-changelog-generator --user jorgecarleitao --project arrow2 --token TOKEN
```

## How to publish

```bash
cargo publish --features full
```
-->
