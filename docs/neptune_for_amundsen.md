# Amundsen on Neptune

[Neptune](https://aws.amazon.com/neptune/) is Amazon's hosted graph database.  The [Getting
Started](https://docs.aws.amazon.com/neptune/latest/userguide/get-started.html) guide is a good
start if you're familiar setting up services in AWS.  Their [Graph Database reference
architecture](https://github.com/aws-samples/aws-dbs-refarch-graph) has even more in that spirit.

If you're coming from another graph database, especially a Tinkerpop Gremlin one like JanusGraph,
see [Gremlin differences](
https://docs.aws.amazon.com/neptune/latest/userguide/access-graph-gremlin-differences.html), and
[Bulk Loader](https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load.html).  


## Neptune setup 

### Summary

* Create a Security Group that allows connections to your Neptune instance.  (safely)
* Create a VPC endpoint for S3, a bucket, and a Role by which Neptune can access that.
* Create Neptune cluster and instances.
* Create user(s) and give them access to Neptune and S3.
* (possibly) Create public endpoint for Neptune.
* Configure amundsen-gremlin


### Create a Security Group that allows connections to your Neptune instance

This greatly depends on the circumstances.  For example, if you're deploying the metadata and
dataloader services within the same VPC, a policy that allows all connections within the VPC is
good.  At the very least, your metadata and loader services need to be able to connect to Neptune on
8182 (over which the services make HTTP and Websocket requests).

The terraform example allows all connections to port 8182, and all outgoing connections within the
VPC.

### Create a VPC endpoint for S3, a bucket, and a Role by which Neptune can access that

The dataloader library uses Neptune's Bulk Loader API.  Briefly: Using that requires putting files
into S3 and then invoking the Neptune Bulk Loader API to load those and check on status.  In order
for the Neptune cluster to be able to access the files, there must be a S3 VPC endpoint in the VPC
in which the Neptune cluster runs.  (It feels like there's a long story behind this.)  Further,
the Neptune cluster needs an assumable that allows it to access the files upon which the Bulk
Loader is invoked.  (This is potentially very simple... but also potentially extremely complicated
because the policies of the S3 VPC Endpoint, the Bucket permissions on the S3 Bucket used, the
per-Object permissions on the files in the Bucket, as well as those of the assumed Role.)

The terraform example creates: the S3 VPC Endpoint, a role for Neptune which gives it readonly
access to all S3 buckets and objects (in that account), and a bucket.

### Create Neptune cluster and instances

Possibly, you will create a Neptune Subnet Group.  If you're using the default VPC, a Neptune
Subnet Group already exists.  Otherwise, you create one. 

Then create a cluster and (at least) one instance, using the Security Group and assumable Role.
The cluster's `cluster_identifier` and the instance's `identifier` will become part of the DNS
names so choose them appropriately.  The `instance_class` configures the amount of RAM, CPU, and
IO available to the instance, as well as being the primary factor in cost.  (Chosing this is
experimental, and beyond the scope of this guide.)  

There are other interesting parameters related to administration: the maintenance window,
automatic minor version upgrades, deletion protection, backup retention, encryption-at-rest and
key management for that, default query execution timeouts.  (These are also beyond the scope of
this.)


The terraform example configures a Neptune Subnet Group, a Neptune Cluster, and 2 Neptune Instances.


### Create user(s) and give them access to Neptune and S3

The metadata and dataloader processes require access to Neptune and S3.  In the most direct setup,
each would use a distinct IAM user and receive just those permissions required to access Neptune
and S3.  In another, you might run the metadata service and dataloader processes in EC2 and you
would grant whatever Role they assume the required permissions for Neptune and S3.

The terraform example creates a Policy which gives the metadata and dataloader services sufficient
access, and creates 2 Users and attaches the Policy.


### Create public endpoint for Neptune

Neptune does not have public endpoints.  The state of the art seems to be using
a [loadbalancer](
https://github.com/aws-samples/aws-dbs-refarch-graph/tree/master/src/connecting-using-a-load-balancer)
as well as glue to allow the mismatch in hostnames.


The terraform sets up a publicly available NLB -- albeit statically configured, so it is only suitable
for development or other deployments where one could tolerate downtime as instances are
reconfigured.  (Or you could apply the CloudWatch + Lambda approach and minimize that downtime.)


### Configure amundsen-gremlin

amundsen-gremlin needs to know about some of the resources you configured with Terraform:

* `AWS_REGION_NAME`: name of the AWS region in which you created the Neptune instance
* `NEPTUNE_BULK_LOADER_S3_BUCKET_NAME`: name of the bucket to use for the Neptune Bulk Loader input
* `NEPTUNE_URL`: either 
  * the URL for the /gremlin endpoint of the Neptune cluster (or instance)
    e.g. `something.cluster-xxxx.region.amazonaws.com:8182/gremlin`, 
  * or if you use the NLB approach, a dict like 
  ```
  {
    "neptune_endpoint": "something.cluster-xxxx.region.amazonaws.com",
    "neptune_port": 8182,
    "uri": "wss://otherthing-yyyyy.elb.region.amazonaws.com:8182/gremlin"
  }  
  ```
* `NEPTUNE_SESSION`: this depends on how you are authenticating your procesess, it could be
  something like ```
  boto3.session.Session(profile_name='youruserprofilehere',
  region_name=AWS_REGION_NAME)```.  
  Or suppose you have some secrets management service and read the file as `secrets_file_yaml` 
  (certainly don't commit that file with your source), it could be something like:
  ```
  boto3.session.Session(
      region_name=secrets_file_yaml['service_region'],
      aws_access_key_id=secrets_file_yaml['aws_access_key_id'],
      aws_secret_access_key=secrets_file_yaml['aws_secret_access_key'])
  ```


## Other Neptunic Considerations


## IAM authentication

You should use IAM authentication.  However, it comes with some mild complications.  The gremlin
transport is usually websockets, and the requests-aws4auth library we use elsewhere is for [requests](https://requests.readthedocs.io/en/master/),
which does not support websockets at all.  In [amazon-neptune-tools](
https://github.com/awslabs/amazon-neptune-tools/tree/master/neptune-python-utils), there is an
example for authenticating gremlin websocket connection which is reused herein.  (But is technically
an example so may or may not work in your circumstances.)


## How to get a gremlin console for Neptune

AWS [has a good recipe](
https://docs.aws.amazon.com/neptune/latest/userguide/iam-auth-connecting-gremlin-java.html).

You may also be interested in using [Jupyter/Sagemaker](
https://aws.amazon.com/blogs/database/analyze-amazon-neptune-graphs-using-amazon-sagemaker-jupyter-notebooks/)
as a Gremlin console w/ some visualization support.


## Export 

The amazon-neptune-tools repo also includes [neptune-export](
https://github.com/awslabs/amazon-neptune-tools/tree/master/neptune-export) which exports the same
.CSV format the Bulk Loader API uses.
