# CloudWatch Agent

Kubernetes deployment manifests for CloudWatch Agent copied from [here](https://github.com/aws-samples/amazon-cloudwatch-container-insights/tree/13d99cad441892469f338e7f7b3e3c120bb84cf5/k8s-deployment-manifest-templates/deployment-mode):

- [Daemonset](https://github.com/aws-samples/amazon-cloudwatch-container-insights/tree/13d99cad441892469f338e7f7b3e3c120bb84cf5/k8s-deployment-manifest-templates/deployment-mode/daemonset/container-insights-monitoring/cwagent) for node metrics
- [Service](https://github.com/aws-samples/amazon-cloudwatch-container-insights/tree/13d99cad441892469f338e7f7b3e3c120bb84cf5/k8s-deployment-manifest-templates/deployment-mode/service/cwagent-prometheus) for scraping prometheus metrics from other sources

## Contents

| File                                                           | Copied from                                                                                                                                                                                                                                             |
| -------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`cwagent-configmap.yaml`](./cwagent-configmap.yaml)           | [Link](https://github.com/aws-samples/amazon-cloudwatch-container-insights/blob/13d99cad441892469f338e7f7b3e3c120bb84cf5/k8s-deployment-manifest-templates/deployment-mode/daemonset/container-insights-monitoring/cwagent/cwagent-configmap.yaml)      |
| [`cwagent-daemonset.yaml`](./cwagent-daemonset.yaml)           | [Link](https://github.com/aws-samples/amazon-cloudwatch-container-insights/blob/13d99cad441892469f338e7f7b3e3c120bb84cf5/k8s-deployment-manifest-templates/deployment-mode/daemonset/container-insights-monitoring/cwagent/cwagent-daemonset.yaml)      |
| [`cwagent-serviceaccount.yaml`](./cwagent-serviceaccount.yaml) | [Link](https://github.com/aws-samples/amazon-cloudwatch-container-insights/blob/13d99cad441892469f338e7f7b3e3c120bb84cf5/k8s-deployment-manifest-templates/deployment-mode/daemonset/container-insights-monitoring/cwagent/cwagent-serviceaccount.yaml) |
| [`prometheus-eks.yaml`](./prometheus-eks.yaml)                 | [Link](https://github.com/aws-samples/amazon-cloudwatch-container-insights/blob/13d99cad441892469f338e7f7b3e3c120bb84cf5/k8s-deployment-manifest-templates/deployment-mode/service/cwagent-prometheus/prometheus-eks.yaml)                              |

## Changes

- Add tolerations against all NoSchedule and NoExecute node taints.
