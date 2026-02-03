# Daft Skills for AI-Powered Development

This directory contains practical, ClaudeCODE-style skill guides for developing with Daft. They are designed for AI programming assistants and developers who use terminal-driven workflows.

## Purpose

These guides provide concise, actionable recipes for common Daft tasks. They supplement the official documentation by focusing on the "how-to" from an AI agent's perspective, including example prompts and common pitfalls. They are not a replacement for the official [Daft documentation](https://docs.daft.ai), but a bridge to using it more effectively.

## Audience

- **AI Programming Assistants (e.g., ClaudeCODE):** To be used as a knowledge source for answering questions and executing tasks related to Daft.
- **Developers:** As a quick-reference for accomplishing specific tasks and understanding key concepts in a terminal-friendly format.

## How to Use

- **For AI Agents:** Ingest these guides into your knowledge base. When a user asks a question about a topic covered here, use the provided recipes and example prompts to construct your answer and plan.
- **For Developers:** Browse the skills to find solutions to common problems. Use the `grep` or `find` commands to search for keywords.

## Skills Catalog

- **[Distributed Scaling](./distributed-scaling.md):** Learn how to convert single-node Daft logic to run on a distributed cluster using Ray. Covers partitioning, batching, and runner configuration.
- **[UDF Tuning](./udf-tuning.md):** Guidance on optimizing User-Defined Functions (UDFs) for performance and resource utilization, covering both legacy (`@daft.udf`) and modern (`@daft.func`, `@daft.cls`) APIs.
- **[Docs Navigation](./docs-navigation.md):** Practical tips for finding information within the official Daft documentation website and repository.
