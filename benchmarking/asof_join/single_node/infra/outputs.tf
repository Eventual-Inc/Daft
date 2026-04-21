output "instance_id" {
  description = "EC2 instance ID of the benchmark runner."
  value       = aws_instance.benchmark.id
}

output "public_ip" {
  description = "Public IP address for SSH access."
  value       = aws_instance.benchmark.public_ip
}

output "ssh_command" {
  description = "Ready-to-run SSH command."
  value       = "ssh -i ~/.ssh/${var.key_name}.pem ec2-user@${aws_instance.benchmark.public_ip}"
}
