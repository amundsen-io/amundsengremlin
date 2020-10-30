variable "environment" {
  type = string
  default = "development"
}

variable "service" {
  type = string
  default = "amundsen-metadata"
}

provider "aws" {
  version = "~> 2.0"
  region = "us-west-2"
  # equivalent to export AWS_PROFILE=... but harder to get wrong
  profile = "${var.service}-${var.environment}--group"
}

# vpc things

resource "aws_default_vpc" "default" {}
locals {
  # or whatever
  vpc_id = aws_default_vpc.default.id
}

data "aws_vpc" "neptune_contained_herein" {
  id = local.vpc_id
}

data "aws_subnet_ids" "neptune_contained_herein" {
  vpc_id = data.aws_vpc.neptune_contained_herein.id
}

resource "aws_security_group" "allow_gremlin" {
  count       = 1
  name        = "allow_gremlin"
  description = "allow inbound to gremlin"
  vpc_id      = data.aws_vpc.neptune_contained_herein.id
}

resource "aws_security_group_rule" "allow_gremlin_ipv4" {
  count           = length(aws_security_group.allow_gremlin)
  type            = "ingress"
  from_port       = 8182
  to_port         = 8182
  protocol        = "tcp"
  prefix_list_ids = []
  # we assume the VPC is isolated
  cidr_blocks = [
    "0.0.0.0/0"
  ]
  ipv6_cidr_blocks = []
  security_group_id = aws_security_group.allow_gremlin[count.index].id
}

resource "aws_security_group_rule" "allow_gremlin_ipv6" {
  count           = length(aws_security_group.allow_gremlin)
  type            = "ingress"
  from_port       = 8182
  to_port         = 8182
  protocol        = "tcp"
  prefix_list_ids = []
  # we assume the VPC is isolated
  ipv6_cidr_blocks = [
    "::/0"
  ]
  cidr_blocks = []
  security_group_id = aws_security_group.allow_gremlin[count.index].id
}

resource "aws_security_group" "allow_outbound" {
  count       = 1
  name        = "allow_outbound"
  description = "allow all outbound"
  vpc_id      = data.aws_vpc.neptune_contained_herein.id
}

resource "aws_security_group_rule" "allow_outbound_ipv4" {
  count           = length(aws_security_group.allow_outbound)
  type            = "egress"
  protocol        = "all"
  from_port       = 0
  to_port         = 0
  prefix_list_ids = []
  cidr_blocks = [
    "0.0.0.0/0"
  ]
  ipv6_cidr_blocks = []
  security_group_id = aws_security_group.allow_outbound[count.index].id
}

resource "aws_security_group_rule" "allow_outbound_ipv6" {
  count           = length(aws_security_group.allow_outbound)
  type            = "egress"
  protocol        = "all"
  from_port       = 0
  to_port         = 0
  prefix_list_ids = []
  ipv6_cidr_blocks = [
    "::/0"
  ]
  cidr_blocks = []
  security_group_id = aws_security_group.allow_outbound[count.index].id
}

data "aws_caller_identity" "current" {}

# neptune things

resource "aws_neptune_cluster_parameter_group" "service" {
  family = "neptune1" # magic, but currently the only family
  description = var.service
  name = var.service

  parameter {
    name = "neptune_enable_audit_log"
    value = 1
  }

  parameter {
    name = "neptune_enforce_ssl"
    value = 1
  }
}

resource "aws_neptune_subnet_group" "service" {
  name       = "service"
  subnet_ids = data.aws_subnet_ids.neptune_contained_herein.ids
}

resource "aws_neptune_cluster" "service" {
  count = 1
  engine = "neptune"
  cluster_identifier = var.service
  iam_database_authentication_enabled = true
  neptune_cluster_parameter_group_name = aws_neptune_cluster_parameter_group.service.name
  vpc_security_group_ids = [
    aws_security_group.allow_gremlin[0].id,
    aws_security_group.allow_outbound[0].id,
  ]
  neptune_subnet_group_name = aws_neptune_subnet_group.service.name
  iam_roles = [
    aws_iam_role.neptune_load_from_s3.arn
  ]

  # this seems to be renamed soon https://www.terraform.io/docs/providers/aws/r/neptune_cluster.html#delete_protection
  deletion_protection = true
  preferred_maintenance_window = "wed:09:49-wed:10:19"
  backup_retention_period = 1
  # waits for the maintenance window, but that may not be what you want
  apply_immediately = false
  enable_cloudwatch_logs_exports = [ "audit" ]
}

resource "aws_neptune_parameter_group" "service" {
  family = "neptune1" # magic, but currently the only family
  description = var.service
  name = var.service

  parameter {
    apply_method = "immediate"
    name = "neptune_query_timeout"
    value = 120000
  }
}

resource "aws_neptune_cluster_instance" "service" {
  count = length(aws_neptune_cluster.service)
  cluster_identifier = aws_neptune_cluster.service[count.index].id
  # reuse the cluster identifier, but it could be anything
  identifier = aws_neptune_cluster.service[count.index].cluster_identifier
  instance_class = "db.t3.medium"
  neptune_parameter_group_name = aws_neptune_parameter_group.service.name
  auto_minor_version_upgrade = true
}

# Supposing you need a public Neptune endpoint, that doesn't happen directly out of the box.
# Our solution is this:
#   * put an NLB (network load balancer) in front of each cluster
#   * that points at an IP address and TCP port (the Neptune server)
#   * the Neptune server terminates TLS
#   * but because the address we connect to is not present as a subject in the Neptune TLS certificate, we install a special
#     SSLContext (for Gremlin/GLV) or HostAdapter (for requests)
# You have questions:
#   * Why not an ALB? And let the it terminate TLS and HTTP?  its health checks on the downstream 
#     Neptune never authenticate well and never go healthy.  and using a TCP health check doesn't
#     seem to work either.
#   * What happens when the Neptune endpoint's IP address changes?  In the canonical setup for this:
#     
#       https://github.com/aws-samples/aws-dbs-refarch-graph/tree/master/src/connecting-using-a-load-balancer
#     
#     ...we'd write a lambda function to watch CloudWatch events post hoc notifying of the change,
#     and have it register those addresses as targets.  There'd be some downtime but like a few seconds.  
#     Or since they won't change very often, and you would just re-run terraform apply and move on.
#   * What about that publicly_accessible attribute on aws_neptune_cluster_instance?  Ha, you wish!  Nope, it doesn't
#     apply to Neptune.  Or maybe it will later?

data "dns_a_record_set" "aws_neptune_cluster_service" {
  count = length(aws_neptune_cluster.service)
  host = aws_neptune_cluster.service[count.index].endpoint
}

data "aws_subnet" "neptune_contained_herein" {
  for_each = data.aws_subnet_ids.neptune_contained_herein.ids
  id = each.value
}

locals {
  aws_subnet_default_by_az = {for s in data.aws_subnet.neptune_contained_herein: s.availability_zone => s }
}

resource "aws_lb" "service" {
  count = length(aws_neptune_cluster.service)
  name = aws_neptune_cluster.service[count.index].cluster_identifier
  internal = false
  load_balancer_type = "network"
  # use just the one subnet
  subnets = [
    local.aws_subnet_default_by_az[aws_neptune_cluster_instance.service[count.index].availability_zone].id
  ]
}

resource "aws_lb_target_group" "service" {
  count = length(aws_neptune_cluster.service)
  name = aws_neptune_cluster.service[count.index].cluster_identifier
  port = aws_neptune_cluster.service[count.index].port
  protocol    = "TCP"
  target_type = "ip"
  vpc_id = data.aws_vpc.neptune_contained_herein.id
  deregistration_delay = 0
}

resource "aws_lb_target_group_attachment" "service" {
  count = length(aws_neptune_cluster.service)
  target_group_arn = aws_lb_target_group.service[count.index].arn
  # for aws_lb_target_group with target_type = "ip", target_id is actually an IP address
  target_id = data.dns_a_record_set.aws_neptune_cluster_service[count.index].addrs[0]
  port = aws_neptune_cluster.service[count.index].port
}

resource "aws_lb_listener" "service" {
  count = length(aws_neptune_cluster.service)
  load_balancer_arn = aws_lb.service[count.index].arn
  port = aws_neptune_cluster.service[count.index].port
  protocol = "TCP"
  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.service[count.index].arn
  }
}

resource "aws_s3_bucket" "bulk_loader" {
  bucket = "${var.service}-${var.environment}-bulk-loader"
  # acl="private" is not the same as the aws_s3_bucket_public_access_block

  lifecycle_rule {
    abort_incomplete_multipart_upload_days = 1
    enabled                                = true
    id                                     = "delete old"
    tags                                   = {}
    expiration {
      days                         = 2
      expired_object_delete_marker = false
    }

    noncurrent_version_expiration {
      days = 1
    }
  }
}

resource "aws_s3_bucket_public_access_block" "bulk_loader" {
  bucket = aws_s3_bucket.bulk_loader.id
  block_public_acls   = true
  block_public_policy = true
  ignore_public_acls  = true
  restrict_public_buckets = true
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id       = aws_default_vpc.default.id
  service_name = "com.amazonaws.${data.aws_region.current.name}.s3"
}

# iam things
resource "aws_iam_role" "neptune_load_from_s3" {
  name = "NeptuneLoadFromS3"
  description = "Allows Neptune to call S3 services on your behalf."

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "rds.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

locals {
  neptune_load_from_s3_policies = [
    "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
  ]
}

resource "aws_iam_role_policy_attachment" "neptune_load_from_s3" {
  role       = aws_iam_role.neptune_load_from_s3.name
  policy_arn = local.neptune_load_from_s3_policies[count.index]
  count = length(local.neptune_load_from_s3_policies)
}

data "aws_region" "current" {}

resource "aws_iam_user" "service" {
  name = var.service
}

resource "aws_iam_user" "dataloader" {
  name = "dataloader"
}

locals {
  # put the GPG public key you wish to encrypt the secrets with here
  # gpg1 --no-default-keyring --keyring ./whatever-key-ring --export whatever-key-id  | base64
  pgp_key = "...."
}

resource "aws_iam_access_key" "service" {
  user = aws_iam_user.service.name
  pgp_key = local.pgp_key
}

resource "aws_iam_access_key" "dataloader" {
  user = aws_iam_user.dataloader.name
  pgp_key = local.pgp_key
}

# terraform output -json service_secret_json
output "service_secret_json" {
  value = {
     # access_key = aws_iam_access_key.service,
     access_key = {
       id = aws_iam_access_key.service.id,
       encrypted_secret = aws_iam_access_key.service.encrypted_secret
     }
     # user = aws_iam_user.service,
     user = {
       name = aws_iam_user.service.name,
       arn = aws_iam_user.service.arn
     }
     # region = data.aws_region.current.name,
     region = {
       name = data.aws_region.current.name
     }
     environment = var.environment
  }
}

# terraform output -json dataloader_secret_json
output "dataloader_secret_json" {
  value = {
     # access_key = aws_iam_access_key.dataloader,
     access_key = {
       id = aws_iam_access_key.dataloader.id,
       encrypted_secret = aws_iam_access_key.dataloader.encrypted_secret
     }
     # user = aws_iam_user.dataloader,
     user = {
       name = aws_iam_user.dataloader.name,
       arn = aws_iam_user.dataloader.arn
     }
     # region = data.aws_region.current.name,
     region = {
       name = data.aws_region.current.name
     }
     environment = var.environment
  }
}

data "aws_iam_policy_document" "allow_access_to_neptune_policy" {
 statement {
   effect = "Allow"
   actions = [
     "neptune-db:*"
   ]
   resources = [for cluster in aws_neptune_cluster.service: cluster.arn]
   sid = "1"
 }

 statement {
   effect   = "Allow"
   actions  = [
     "s3:Get*",
     "s3:List*",
     "s3:Put*",
   ]
   resources = ["${aws_s3_bucket.bulk_loader.arn}/*"]
   sid = "2"
 }

 statement {
   effect   = "Allow"
   actions   = [
     "s3:ListBucket",
   ]
   resources = [aws_s3_bucket.bulk_loader.arn]
   sid = "3"
 }
}

resource "aws_iam_policy" "allow_access_to_neptune" {
  name = "allow-access-to-neptune"
  policy = data.aws_iam_policy_document.allow_access_to_neptune_policy.json
}

resource "aws_iam_user_policy_attachment" "service_allow_access_to_neptune" {
  user       = aws_iam_user.service.name
  policy_arn = aws_iam_policy.allow_access_to_neptune.arn
}

resource "aws_iam_user_policy_attachment" "dataloader_allow_access_to_neptune" {
  user       = aws_iam_user.dataloader.name
  policy_arn = aws_iam_policy.allow_access_to_neptune.arn
}

# or if you wanted a Role that you could assume from EC2
resource "aws_iam_role" "allow_access_to_neptune" {
  name = "AllowAccessToNeptune"
  description = "Allows access to Neptune and the S3 Bulk Loader bucket"
  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

# outputs
output gremlin_url {
  value = [for i in aws_neptune_cluster.service: "wss://${i.endpoint}:${i.port}/gremlin" ]
}

output gremlin_public_url_dict {
  value = {
    for index, cluster in aws_neptune_cluster.service: cluster.cluster_identifier => {
      neptune_endpoint : cluster.endpoint,
      neptune_port : cluster.port,
      uri : "wss://${aws_lb.service[index].dns_name}:${cluster.port}/gremlin",
    }
  }
}
