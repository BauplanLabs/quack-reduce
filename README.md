# quack-reduce
A playground for running duckdb as a stateless query engine over a data lake

## Overview

TBC

This is the companion repo to this blog post LINK. Please refer to the post for more context on the project, some background information about the motivation and use cases behind the code.

## Setup

This project is pretty self-contained and requires only introductory-level familiarity with cloud services and frameworks, and a bit of Python.

### Accounts

Make sure you have:

* a working AWS account;
* Python 3.x and Node.js properly installed on your machine (while you can create the container on ECR manually and then the lambda from the console, the instruction below assumes you just use the CLI for the entire setup).

### Global variables

In the `src` folder, you should copy `local.env` to `.env` (do *not* commit it) and fill it with proper values:

| value                 | type | description                                          |                   example |
|-----------------------|------|------------------------------------------------------|--------------------------:|
| AWS_ACCESS_KEY_ID     | str  | User key for AWS access                              | AKIAIOSFODNN7EXAMPLE      |
| AWS_SECRET_ACCESS_KEY | str  | Secret key for AWS access                            | wJalr/S3_BUCKET_NAME      |
| S3_BUCKET_NAME        | str  | Bucket to host the data (must be unique)             | my-duck-bucket-130dcqda0u |

These variables will be used by the setup script and the runner to communicate with AWS (S3 and Lambdas). Make sure the user has the permissions to:

* create a bucket and upload files to it;
* invoke the lambda that we create below.

### Duckdb lambda

The `src/serverless` folder is a self-contained lambda project that uses Python to leverage duckdb querying capabilities over files in an object storage. For readers not familiar with [serverless](https://www.serverless.com/framework/) projects, it contains:

* a Dockerfile, which starts from the public AWS lambda image for Python (`public.ecr.aws/lambda/python:3.9`) and add the few dependencies we need;
* an `app.py` file, containing the actual code our lambda will execute;
* a `../serverless.yml` file, which ties all these things together in the infra-as-code fashion, and allows us to deploy and manage the function from the CLI.
* a `../package.json` file, which contains the few dependencies we need to use the serverless CLI.

To install all the required dependencies, you need to cd into `src` and run `make nodejs-init` (under the hood the `npm install` command will be executed); finally you can build the docker image and deploy the lambda with `make serverless-deploy` (under the hood the `npx serverless deploy` command will be executed). The first time, deployment will take a while as it needs to create the image, ship it to AWS and create the lambda stack - note that this is _a "one-off" thing_:

![Confirmation in the terminal of the successful creation of our lambda.](images/serverless.png)

Note that sometimes you may get a `403 Forbidden` error when building the docker: in our experience, this usually goes away with `aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws`.

Once the deployment is finished, you can check your AWS account to see the lambda (you can even test it from the console, as we do in [this video](https://www.loom.com/share/97785a387af84924b830b9e0f35d8a1e)).

### Python environment

Create and activate a virtual environment, and install the few dependencies by `cd` into the `src` folder and by running `make python-init` (under the hood a new venv will be created, activated and all the required depedencies will be installed).

Then you need to run a quick setup script: `make run_me_first`. This will accomplish few things:

* it will download the NYC taxi dataset and upload it to the bucket, both as a unique file and as a hive-partitioned directory;
* it will print out few stats and meta-data on the dataset.

If you check your AWS console, your bucked should now have a `partitioned` folder with this structure:

![Bucket partitioning after the upload.](images/s3.png)

Note that this project has been developed and tested on Python 3.9+.

### Optional: dbt

If you want to see how this architecture can bridge the gap between offline pipelines preparing artifacts, and real-time querying for BI (or other use cases), we recommend you running the [dbt DAG](https://docs.getdbt.com/terms/dag) we prepared to simulate:

* running some SQL transformations over the original dataset;
* dump the results of the transformation (the equivalent of a dashboard view) in the data lake;
* use our serverless query engine to power cheap and fast real-time visualization queries.

To do that, you will need a dbt setup. To simplify the project, we included a version that works well with [duck-dbt](https://github.com/jwills/dbt-duckdb): however, if you have dbt on Snowflake, the same exact principles apply (as in, you can export from Snowflake your final artifact and then querying it with this lambda).

The quickest setup is running dbt locally, so you will need to set up a dbt [profile](https://docs.getdbt.com/docs/core/connection-profiles) named `duckdb-taxi` (see [here](https://github.com/jwills/dbt-duckdb) for examples), for example:

```yaml
# ~/.dbt/profiles.yml
duckdb-taxi:
  outputs:
   dev:
     type: duckdb
     path: ':memory:'
     extensions:
        - httpfs
        - parquet
     settings:
        s3_region: us-east-1
        s3_access_key_id: YOUR_S3_USER
        s3_secret_access_key: YOUR_S3_KEY
  target: dev
```

Please note that the dbt project is by design extremely simple and unsophisticated: we care about the overall design pattern here, not so much about the specific modalities of how transformation happens.

## Running the project

### A serverless query engine

Make sure the setup is completed, you are in the right Python environment and inside the `src` folder. You can test everything is working by running:

`make test`

The script will activate the venv and run `python quack.py`, a simple count query on the file and print out the results.

If all looks good, you can now run arbitrary queries, e.g. (replacing `MY_BUCKET_NAME` with your value); make sure to manually activate your venv with `source ./.venv/bin/activate:

`python quack.py -q "SELECT pickup_location_id AS location_id, COUNT(*) AS counts FROM read_parquet(['s3://MY_BUCKET_NAME/dataset/taxi_2019_04.parquet']) WHERE pickup_at >= '2019-04-01' AND pickup_at < '2019-04-03' GROUP BY 1 ORDER BY 2 DESC"`

to get the most popular pickup location (IDs) for the first few days of April, or

`python quack.py -q ...`

to get BLAH BLAH.

Since the amount of data that can be returned by a lambda is limited, the lambda will automatically limit your rows if you don't specific a limit in the script. You can get more data back with:

`python quack.py -q ... -limit 100`

but be mindful of the infrastructure constraints!

### Serverless BI architecture (Optional)

While you can run *totally* use the dashboard we designed directly on the dataset file in S3, we orchestrate a slightly more complex and realistic scenarios:

* a batch pipeline that produces a final artifact, from raw data;
* a dashboard allowing interactive queries on this final table (leveraging our serverless design).

As mentioned above, we assume you have completed a dbt setup successfully. Now run `make dbt-run`.

At the end of the tiny DAG, you will end up with a new folder (and file) in the target bucket (note that we use the `external` strategies for duck-dbt to produce a the view, and different warehouses would need slightly different configurations here to achieve the same result). This file represents the materialized view we need to serve from our BI:

![Dashboard artifact is materialized.](images/dashboard.png)

> NOTE: the sql files in `dbt/models/taxi` reference directly files in the bucket, so any changes you made to the setup script should be reflected here as well.

Now that we have a materialized view produced by our pipeline, it is time to query it! To run the front-end (a dashboard built with streamlit) run `make dashboard`. A page should open in the browser, displaying a chart:

![Chart of popular locations in the BI tool.](images/streamlit.png)

You can use the form to interact in real time with the dataset (video [here](https://www.loom.com/share/9d5de3ba822a445d9d117225c1b0307f)), through the serverless infrastructure we built.

### From quack to quack-reduce (Optional)

As we mention in the blog post (LINK HERE AGAIN), an intriguing possibility of this design (going a bit more abstract compared to the immediate use cases above) is that the staless execution of SQL over an object storage (and therefore, using duckdb not really as a db, but basically as "just" a query engine) coupled with the parallel nature of AWS lambdas opens up interesting optimization possibilities.

In particular, we could rephrase (some) SQL queries through a map-reduce programming pattern *with other SQL queries*, and execute them all at the same time. To consider a trivial example, a query such as:

`SELECT COUNT(*) FROM myTable WHERE DATE BETWEEN 04/01/2022 AND 04/05/2022`

can be rewritten as the SUM of the results of these smaller queries:

`SELECT COUNT(*) FROM myTable WHERE DATE BETWEEN 04/01/2022 AND 04/02/2022` +
`SELECT COUNT(*) FROM myTable WHERE DATE BETWEEN 04/02/2022 AND 04/03/2022` +
...

As the number of files increases (as in a typical hive-partitioned data lake), scanning the object storage (in duckdb syntax `parquet_scan('folder/', HIVE_PARTITIONING=1)`) may take much longer than reading single _k_ files directly through ideally _k_ parallel functions, drastically improving query performances.

To test out this hypothesis, we built a tiny script that compares the same engine across different deployment patterns - local, remote etc. You can run the bechmarks with default values with `make benchmark`. The script is minimal and not very configurable, but should be enough to give you a feeling of how the different setups perform compared to each other, and the trade-offs involved (check the code for how it's built, but don't expect much!). 

A typical run (ADD VIDEO LINK HERE WITH LOOM) will result in something like the following table (numbers will vary, but the layout should be the same):

![Table benchmarking different architectures.](images/benchmarks.png)

Please refer to the blogpost for more musings on this opportunity (and the non-trivial associated challenges).

NOTE: if you have never raised your concurrency limits on AWS lambda, you may need to request through the console for an increase in parallel execution, otherwise AWS will not allowed the scaling out of the function.

## License

All the code is released without warranty, "as is" under a MIT License. 

This started a fun week-end project and should be treated with the appropriate sense of humour.