variable "aws_region" {
  description = "AWS region to launch the benchmark instance in."
  type        = string
}

variable "ami_id" {
  description = "Amazon Linux 2023 AMI ID for the target region."
  type        = string
}

variable "instance_type" {
  description = "EC2 instance type for the benchmark runner."
  type        = string
  default     = "r7i.xlarge"
}

variable "key_name" {
  description = "Name of the EC2 key pair to use for SSH access."
  type        = string
}

variable "security_group_id" {
  description = "ID of the security group that allows inbound SSH."
  type        = string
}

variable "iam_instance_profile" {
  description = "IAM instance profile name to attach (must grant S3 access to the benchmark bucket)."
  type        = string
}

variable "root_volume_gb" {
  description = "Size of the root EBS volume in GiB."
  type        = number
  default     = 100
}

variable "owner_tag" {
  description = "Value for the Owner tag on the instance."
  type        = string
}
