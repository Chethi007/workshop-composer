# Composer Workshop

Welcome to Cloud Composr Workshop. In this workshop we will :
1) Create a Cloud Composer workshop
2) Execute some Basic DAGs on it
3) Execute some specific cloud operators DAGs on it
4) Execute some scenario DAGs on it

For all the DAGs we'll write we'll set `catchup=None`, this will deactivate the backfill as it is not needed for this workshop.

## Creating Environnement

Create a cloud composer environnement as described in [this guide](https://cloud.google.com/composer/docs/how-to/managing/creating).

## Basic Operators & Core Concepts

### Hello world without scheduling

Write a DAG using [BashOperator](https://airflow.apache.org/code.html#airflow.operators.BashOperator) to print some text in the console.

Set `schedule_interval=None` for this DAG.

Push your file into the DAG bucket of your environnement.

Go to the Composer Airflow UI and explore the details of the DAG.

An example can be found in file **01_hello_world_without_scheduling.py**.

### Hello world with scheduling

From the previous DAG, modify the `schedule_interval` property in order to have the DAG executed periodically (every 1 minutes for example).
Modify the command executed by the `BashOperator Dag` to print the execution date (look at the [available variables](https://airflow.apache.org/code.html#default-variables)).

An interesting thing to notice is the difference between the time **at which** the task is executed and the time **for which** it is executed. For example, which a 2mn periodicity a task executed **at** 10:20 is executed **for** the `{{ execution_date }}=10:18`.
Read [this](https://airflow.apache.org/scheduler.html#scheduling-triggers) for the complete explanation.

An example can be found in file **02_hello_world_with_scheduling.py**.

### Task dependencies

From the previous DAG, add a task waiting for some time after the Hello World has been printed and then a third task that prints that the execution is completed:
```print_hello -> wait -> print_finished```

Once executed, you can open the Dag Run details and go to `Gantt`, `Task Duration` or `Landing Times` to see what happened.

An example can be found in file **03_task_dependencies.py**.

### Python operator

Write a DAG using [PythonOperator](https://airflow.apache.org/code.html#airflow.operators.PythonOperator) to print some text in the console.

Try to pass parameters to the Python function that is called by the DAG. Also try to access the Airflow context from the Python function.

An example can be found in file **04_python_operator.py**.

### Using static variables

Go to the `Admin / Variables` menu and create a new variable.
Start from a previous DAG and print the variable content to the console.

We can see that the variable value is static and is shared by all DAGs and all of their execution. We will see in a later step how to use variable scoped locally for a particular DAG execution.

An example can be found in file **05_static_variables.py**.

### Using XComs variables

[XComs variables](https://airflow.apache.org/concepts.html#xcoms) allow to share dynamic information between the different tasks of a DAG or even between DAGs.
An XComs variable is uniquely identified by the name of the task that produced it and the execution time for which it has been produced (a same task will create a new variable for every DAG run).
XComs variables are persisted in the Airflow database and thus can be audited and are present if a previous execution of a DAG must be launched again.

Create a DAG with a Python task pushing a value for a XComs variable, the value could be different for each execution.

Once your DAG is executed you can go to the `Admin / XComs` menu to see the list of all the created XComs variables.

An example can be found in file **06_xcoms_variables.py**.

### Branching

The execution path in a DAG can be [dynamic](https://airflow.apache.org/concepts.html#branching) depending on given predicate (variable value, external system status, ...).

Write a DAG using [BranchPythonOperator](https://airflow.apache.org/code.html#airflow.operators.BranchPythonOperator) to control the execution of a DAG.

An example can be found in file **07_branching.py**.

### Sub-DAGs

Airflow allows to split a complex DAG into several simplier [sub-dags](https://airflow.apache.org/concepts.html#subdags).

Write a DAG using [SubDagOperator](https://github.com/apache/incubator-airflow/blob/master/airflow/operators/subdag_operator.py) to create a DAG composed of one or more sub-dags.

An example can be found in file **08_subdags.py**.

### Custom operator

Airflow provides an extensible framework allowing to create new operators implementing custom behavior. These new operators can extend (sub-class) [existing operators](https://airflow.apache.org/code.html#operator-api) or can be creating from scratch by sub-classing the [base-operator](https://airflow.apache.org/code.html#baseoperator).

Create a DAG with a new custom operator, defined in the same file for simplicity, with templated fields that will be automatically replaced at execution time.

An example can be found in file **09_custom_operators.py**.

### Catchup and backfill

Until now, all the created DAGs were defined with the property `catchup=False`. This had the effect to execute a DAG starting only from the moment we activated it and for all the subsequent intervals as defined by the `schedule_interval` property. 

This can be the desired behavior in some (most ?) cases but sometimes we want Airflow to execute past intervals starting from the `start_date` property.
This can be the case for example if we want to retrieve past data from an external system via API calls.

Create a DAG with a daily schedule, a `start_date` property a few days in the past and with the property `catchup=True`. Activate it and observe how Airflow creates and executes automatically DAG runs for the days between `start_date` and today.

The process of retrieving past data in called backfilling and starting executions for the past intervals is called catchup. [Here](https://airflow.apache.org/scheduler.html#backfill-and-catchup) is the Airflow documentation about it.

An example can be found in file **10_catchup_backfill.py**.


## GCP integration & Cloud Operators

Airflow comes with multiple operators allowing to access several GCP services (Storage, BigQuery, Dataflow, ...).
These operators can be used as this when possible and can also be extended to implement custom behavior.

### Identity management

When accessing GCP services, an identity must be provided to prove that the DAG operators are legitimate to access these services.
This can be your personnal identity (for example when developing DAGs locally) or the credentials of a GCP service account when working on a deployed Airflow. 
In Airflow, this is specified as a [connection](https://airflow.apache.org/configuration.html#connections).

All the operators allowing to access GCP services take one or more identity(ies) as parameter(s).

### GCS Operators
Write a dag that transfers a local file to cloud storage.

`Hint` : use the `FileToGoogleCloudStorageOperator`

An example can be found in file **20_gcs_operators.py**.

### BigQuery Operators
Write a dag that exports a public dataset table from bigquery to cloud storage.

`Hint` : use the `BigQueryToCloudStorageOperator`

An example can be found in file **21_bigquery_operators.py**.

### Dataproc Operators
Create a DAG that completes the following tasks:

1) Creates a Cloud Dataproc cluster
2) Runs an Apache Hadoop wordcount job on the cluster, and outputs its results to Cloud Storage
3) Deletes the cluster

Follow the tutorial described [here](https://cloud.google.com/composer/docs/quickstart).

An example can be found in file **23_dataproc_wordcount.py**.

### Kubernetes Operators
Create a DAG that launches a php command on a kubernetes pod.

Adapt the tutorial described [here](https://cloud.google.com/composer/docs/how-to/using/using-kubernetes-pod-operator).

An example can be found in file **24_kubernetes_php_pod.py**.


### Dataflow Operators
Create a DAG that launches a dataflow python job.

The wordcount pipeline is provided in file **dataflow_wordcount.py**

`Hint` : use the `DataFlowPythonOperator`

An example can be found in file **25_dataflow_wordcount.py**.

### MySQL Operators
Create a DAG that exports a table from Mysql to Cloud Storage.

#### Prerequisites
1) Create a Cloud SQL Instance that is reachable from anywhere
2) Create a schema on it and execute the **26_schema.sql**
3) Create a MySQL Connection with id `workshop_sql_conn_id` that points to your Cloud SQL instance. This connexion will be used in the DAG.

`Hint` : use the `MySqlToGoogleCloudStorageOperator`

An example can be found in file **26_sql_operators.py**.

## Scenarios

### Scenarios 1
Create a DAG that :
1) Uploads a file to cloud storage.
2) Processes a wordcount dataflow pipeline on this file
3) loads the result on Cloud SQL (with the schema **30_schema.sql**) and Bigquery

#### Prerequisites
Create a Bigquery Dataset that will hold your data
Create Cloud SQL table that will hold your data

`Hint` : Combine the operators seen before
An example can be found in file **30_scenario.py**.

### Scenarios 2
Create a DAG that :
1) Uploads an orders file(**31_order.json**) to cloud storage.
2) Processes a dataflow pipeline on this file that transforms the data (use the pipeline **31_dataflow.py**)
3) loads the result on Cloud SQL (use the schema **31_schema.sql**)

`Hint` : Combine the operators seen before
An example can be found in file **31_scenario.py**.