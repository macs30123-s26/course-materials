## Lab 7 - PySpark on Midway3

### 1. Set up the environmental variables for PySpark on Midway 3

Follow the instructions in [`spark-sinteractive-setup.md`](/07_Spark/PySpark_EDA_ML/midway/spark-sinteractive-setup.md) to set up the `~/.bashrc`


#### Method 1

Run [`sinteractive_spark.sh`](../midway/pyspark_innitialization/sinteractive_spark.sh) to get into a interactive computing nodes, and then run [`start_spark.sh`](../midway/pyspark_innitialization/start_spark.sh) to get the PySpark start in a jupyter lab.

You can change the settings in these two initialization files, such as the memories and time.

Then, `ssh` the jupyter lab to your local machine by the code below:

```bash
ssh -NL <local_port>:<lab-IP-address>:8888 <your-CNetID>@midway3.rcc.uchicago.edu
```

The lab IP address should be printed out through the innitialization.

#### Method 2

Use [the following sbatch](../midway/pyspark.sbatch) to run PySpark jobs by py files on Midway3:

```bash
#!/bin/bash

#SBATCH --job-name=spark-example
#SBATCH --output=spark.out
#SBATCH --error=spark.err
#SBATCH --nodes=1
#SBATCH --ntasks=10
#SBATCH --mem=40G
#SBATCH --partition=caslake
#SBATCH --account=macs30123

module load python/anaconda-2022.05 spark/3.3.2

export PYSPARK_DRIVER_PYTHON=/software/python-anaconda-2022.05-el8-x86_64/bin/python3
export PYSPARK_PYTHON=/software/python-anaconda-2022.05-el8-x86_64/bin/python3

spark-submit --total-executor-cores 9 --executor-memory 4G --driver-memory 4G YOUR_FILE.py
```

### 2. Exersices

- Ex1. Basic Functions

    - Complete the script in [this python file](../exercise/lab_wk7_spark.py) that processes and analyzes a dataset containing political speeches to identify and count the occurrences of certain policy-related keywords.
        - Filter the speeches, extract keywords and map, and reduce by key
        - Expected output: [('Politician Name 1', count), ('Politician Name 2', count)]

- Ex2. Spark SQL
    - Complete code in [this notebook](../exercise/lab_wk7_sparksql.ipynb).

- Ex3. Spark ML
    - Complete code in [this notebook](../exercise/lab_wk7_pysparkML.ipynb).


