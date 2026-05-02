# PySpark on EMR Cheatsheet

## Launching a (Spark-enabled) EMR Cluster

Create an EMR cluster with Spark installed on it, using the `launch_spark_cluster.py` script in this directory (and ensuring that your AWS `credentials` are correct and updated). Note that you should fill in the `s3_bucket` field with an S3 bucket that you have access to in your account (replacing "YOUR_BUCKET_NAME" with your bucket name). This configuration will automatically save any Jupyter Notebooks you create in your S3 bucket, meaning they will persist even after you terminate your EMR cluster.

In a basic use of the script, you may select the EC2 instance type for your cluster, along with the number of core and primary nodes for the cluster. Recall, that we can request a maximum of 32 vCPUs on AWS Academy (e.g., 1 primary m5.xlarge node and 7 m5.xlarge core nodes). Here we create a m5.xlarge cluster with 1 primary node and 2 core nodes.

```bash
python launch_spark_cluster.py \
    --s3_bucket YOUR_BUCKET_NAME \
    --primary_count 1 \
    --core_count 2 \
    --instance_type "m5.xlarge"
```
If you would like to install additional EMR applications that are not already installed on the cluster or provide additional configuration information (optional), you may add additional details via the `additional_app` and `additional_configs` CLI fields. For instance, this command also installs Zeppelin and changes a Livy configuration setting:

```bash
python launch_spark_cluster.py \
    --s3_bucket YOUR_BUCKET_NAME \
    --primary_count 1 \
    --core_count 2 \
    --instance_type "m5.xlarge" \
    --additional_apps "Zeppelin" \
    --additional_configs '{"Classification": "livy-conf", "Properties": {"livy.server.session.timeout-check": "false"}}'
```
When your cluster is finished launching (it will take around 10-15 minutes for everything to be installed and the `launch_spark_cluster.py` script will print out a message indicating completion), you can then `ssh` into the cluster to run Spark Jobs via the terminal or the JupyterHub server running on the cluster.

## Running PySpark code via the terminal

You can directly `ssh` into your primary node and run PySpark jobs via the `pyspark` interpreter in the same way you logged into regular EC2 instances earlier in the class (replacing the `ec2-...` domain name with the primary node public DNS listed in the EMR console and ensuring your PEM file is in the specified location):

```bash
ssh -i ~/.ssh/vockey.pem hadoop@ec2-23-22-235-142.compute-1.amazonaws.com
```

An additional way to run jobs while `ssh`-ed into your EMR cluster is submit batch jobs via `spark-submit`. For instance, to run `emr_pyspark_script.py` (a script included in this repository that can be uploaded to S3 as in the command below) on your cluster, you can issue the following command on your EMR cluster:

```bash
spark-submit s3://YOUR_S3_BUCKET_NAME/emr_pyspark_script.py YOUR_S3_BUCKET_NAME
```

Note that the script will write its output to an S3 bucket ("YOUR_S3_BUCKET_NAME") and print out logging information related to the job in your terminal window.

## Running interactive jobs via JupyterHub

You can also write PySpark code in a Jupyter Notebook by using `ssh` to forward the port that the JupyterHub server is running on your EMR cluster (9443) to the same port on your local machine. For instance, you could run the following command on your local terminal (again filling in the correct domain name for your primary node and ensuring your PEM file is in the specified location):

```bash
ssh -i ~/.ssh/vockey.pem -NL 9443:localhost:9443 hadoop@ec2-23-22-235-142.compute-1.amazonaws.com
```

Just be sure not to close the terminal window that you run this command on as it will stop port 9443 from being forwarded to your local machine (and you will not be able to access JupyterHub without running the command again). 

After running the above command, navigate to `https://localhost:9443` in your browser (and navigate through all of the security warnings). You should now see a JupyterHub login window. The username is "jovyan" and the password is "jupyter" -- the defaults for JupyterHub. You should now be able to write PySpark code in Jupyter Notebooks if you select the PySpark kernel for execution.

## Terminating your cluster

You can either terminate your cluster in the EMR console (click the checkmark by your cluster and then click the "Terminate" button), or you can use the `terminate_spark_cluster.py` script to terminate your cluster programmatically (replacing the placeholder Cluster ID with your own Cluster ID):

```bash
python terminate_spark_cluster.py --cluster_id j-XXXXXXXXXXXXX
```