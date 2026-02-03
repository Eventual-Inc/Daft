# Contributing to Daft

Daft is an open-source project and we welcome contributions from the community. Whether you're reporting bugs, proposing features, or contributing code, this guide will help you get started.

## Quick Start

- **Found a bug?** 🐛 [Report it here](#reporting-issues)
- **Have a feature idea?** 💡 [Start a discussion](#proposing-features)
- **Want to make your first PR?** 🚀 [Contribute new code](#contributing-code)

## Reporting Issues

To report bugs and issues with Daft, please file an issue on our [issues](https://github.com/Eventual-Inc/Daft/issues) page.

Additionally, please include the following information in your bug report:

1. Operating system
2. Daft version
3. Python version
4. Daft runner (native or Ray)

## Proposing Features

We highly encourage you to propose new features or ideas. Please start a GitHub Discussion in our [Ideas channel](https://github.com/Eventual-Inc/Daft/discussions/categories/ideas).

When proposing features, please include:

1. Feature Summary (no more than 3 sentences)
2. Example usage (pseudo-code to show how it is used)
3. Corner-case behavior (how should this code behave in various corner-case scenarios)

## Contributing Code

For detailed development instructions, see our [Development Guide](https://docs.daft.ai/en/stable/contributing/development/).

## Governance

The Daft project is governed by a community of contributors who have helped shape the project into what it is today. Members contribute in different ways and take on a variety of roles.

### Contributor

A Contributor is anyone who contributes to the Daft project in any form: code, documentation, issues, community support, talks, or tooling. Contributors are the foundation of the project and are encouraged to participate in discussions, propose features, and submit pull requests.

We thank the following community members for their contributions to Daft:

| GitHub | Affiliation | Contributions |
|--------|-------------|---------------|
| @shaofengshi | Datastrato | Apache Gravitino catalog integration |
| @malcolmgreaves | Meaves Industries | Documentation, tutorials, embeddings |
| @ConeyLiu | Tencent | UTF-8 functions, regexp_replace, floor division |
| @Lucas61000 | | SQL STRUCT parsing |
| @j3nkii | | Dashboard fixes, documentation |
| @destroyer22719 | Queen's University | Expression aliases, documentation |
| @kyo-tom | DTStack | OpenAI embedder token limits |
| @gpathak128 | | JSON null field handling |
| @rahulkodali | UT Austin | Multi-input hash function |
| @fenfeng9 | | Embedding dtype fixes |
| @Abyss-lord | ctyun | Logger improvements |
| @datanikkthegreek | | Delta Lake, Unity Catalog (issues & PRs) |
| @djouallah | | SQL/Spark ecosystem feedback |
| @jakajancar | Assertly | Catalog design, SQL features |
| @ion-elgreco | NATO | Ray execution, production feedback |
| @hongbo-miao | Archer Aviation | Installation, PyArrow, UUID support |
| @lhoestq | Hugging Face | HuggingFace ecosystem collaboration |
| @aaron-ang | | Distance/similarity functions, string casing, list operations, SQL |

### Maintainer

A Maintainer is a recognized Contributor who has demonstrated sustained, meaningful contributions to the project. Maintainers are nominated by existing Maintainers or PMC members and approved by a majority vote of the PMC.

- **Maintainer/Read**: Recognized contributors with review (and approval) permissions, but without merge access
- **Maintainer/Write**: Maintainer/Read with merge access to the repository

**Maintainers/Read:**

Maintainers/Read have PR approval permissions and are welcomed to be a part of growing the capabilities of the Daft engine through PR reviews!

| GitHub | Affiliation | Focus Areas |
|--------|-------------|-------------|
| @stayrascal | Bytedance | Storage/I/O layer, TOS support, native CSV/JSON writers |
| @kevinzwang | SkyPilot | Core refactoring (arrow2 migration), UDF system, vLLM, type conversions |
| @conceptofmind | Teraflop AI | Performance and UDF bug reports |
| @everySympathy | ByteDance | Series slicing, partition optimization |
| @VOID001 | ByteDance | Dashboard features, SQL error handling |
| @gweaverbiodev | CloudKitchens | pyiceberg support, UDF improvements |

**Maintainers/Write:**

Maintainers who additionally have merge access to the repository.

| Name | GitHub | Affiliation |
|------|--------|-------------|
| Chris Kellog | @cckellogg | Eventual |
| Desmond Cheong | @desmondcheongzx | Eventual |
| Everett Kleven | @everettVT | Eventual |
| Jay Chia | @jaychia | Eventual |
| Jeev Balakrishnan | @jeevb | Eventual |
| Varun Madan | @madvart | Eventual |
| Oliver Huang | @ohbh | Eventual |
| Colin Ho | @colin-ho | Eventual |
| Rohit Kulshreshtha | @rohitkulshreshtha | Eventual |
| Sammy Sidhu | @samster25 | Eventual |
| Sam Stokes | @samstokes | Eventual |
| Srinivas Lade | @srilman | Eventual |
| Cory Grinstead | @universalmind303 | Eventual |
| YK | @ykdojo | Eventual |
| Kejian Ju | @Jay-ju | Bytedance |
| Zhenchao Wang | @plotor | Bytedance |
| Leilei Hu | @huleilei | Bytedance |
| Zhiping Wu | @stayrascal | Bytedance |
| Can Cai | @caican00 | Xiaomi |

### Project Management Committee (PMC)

The PMC provides oversight and governance for the Daft project. PMC members have voting rights on community decisions, including approving new Maintainers and setting the strategic direction of the project.

| Name | GitHub |
|------|--------|
| Jay Chia | @jaychia |
| Sammy Sidhu | @samster25 |
| Varun Madan | @madvart |
