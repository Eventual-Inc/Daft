terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

data "aws_security_group" "ssh" {
  id = var.security_group_id
}

data "aws_iam_instance_profile" "benchmarking" {
  name = var.iam_instance_profile
}

resource "aws_instance" "benchmark" {
  ami                    = var.ami_id
  instance_type          = var.instance_type
  key_name               = var.key_name
  vpc_security_group_ids = [data.aws_security_group.ssh.id]
  iam_instance_profile   = data.aws_iam_instance_profile.benchmarking.name

  root_block_device {
    volume_type           = "gp3"
    volume_size           = var.root_volume_gb
    delete_on_termination = true
  }

  tags = {
    Name    = "asof-join-benchmark"
    Owner   = var.owner_tag
    Purpose = "asof-join-benchmark"
  }
}
