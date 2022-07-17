# Fluent-Bit

Kubernetes deployment manifests for Fluent-Bit copied from [here](https://github.com/aws-samples/amazon-cloudwatch-container-insights/tree/13d99cad441892469f338e7f7b3e3c120bb84cf5/k8s-deployment-manifest-templates/deployment-mode):

- [Daemonset](https://github.com/aws-samples/amazon-cloudwatch-container-insights/tree/13d99cad441892469f338e7f7b3e3c120bb84cf5/k8s-deployment-manifest-templates/deployment-mode/daemonset/container-insights-monitoring/fluent-bit) for sending pod logs to CloudWatch.

## Contents

| File                                                       | Copied from                                                                                                                                                                                                                                              |
| ---------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`fluent-bit-configmap.yaml`](./fluent-bit-configmap.yaml) | [Link](https://github.com/aws-samples/amazon-cloudwatch-container-insights/blob/13d99cad441892469f338e7f7b3e3c120bb84cf5/k8s-deployment-manifest-templates/deployment-mode/daemonset/container-insights-monitoring/fluent-bit/fluent-bit-configmap.yaml) |
| [`fluent-bit.yaml`](./fluent-bit.yaml)                     | [Link](https://github.com/aws-samples/amazon-cloudwatch-container-insights/blob/13d99cad441892469f338e7f7b3e3c120bb84cf5/k8s-deployment-manifest-templates/deployment-mode/daemonset/container-insights-monitoring/fluent-bit/fluent-bit.yaml)           |

## Changes

- Fill static variables in `fluent-bit-configmap.yaml`
- Update `imds_version` in `fluent-bit.yaml` from `v1` to `v2` based on this [comment](https://github.com/fluent/fluent-bit/issues/2840#issuecomment-1117176115)
- Update to `cloudwatch` plugin instead of the `cloudwatch_logs` plugin for extra features (e.g `log_retention_days`, adding kubernetes namespace to `log_group_name`)
- Update `application-log.conf` to include namespace in `log_group_name`
- Add a 180 day retention policy via option: `log_retention_days`
