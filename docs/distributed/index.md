# Distributed Execution

Daft is designed to scale seamlessly from your laptop to distributed clusters. By default, Daft runs using your local machine's resources with the **native runner (codenamed Swordfish)**, but when you need more compute power, you can easily scale to distributed execution across multiple machines.

There are currently two ways to run Daft on multiple machines:

- For the easiest way to get started, check out our guide for [running on Kubernetes](kubernetes.md)
- Alternatively, if you have an existing Ray setup, check out our guide for [running on Ray](ray.md)
