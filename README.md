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
* the [serverless framework](https://www.serverless.com/framework/) properly [installed and configured](https://www.serverless.com/framework/docs/getting-started) (while you can create the container on ECR manually and then the lambda from the console, the instruction below assumes you just use the CLI for the entire setup).

### Global variables

In the `src` folder, you should copy `local.env` to `.env` (do *not* commit it) and fill it with proper values:

| value     | type | description                                          |                   example |
|-----------|------|------------------------------------------------------|--------------------------:|
| S3_USER   | str  | User key for AWS access                              | AKIAIOSFODNN7EXAMPLE      |
| S3_SECRET | str  | Secret key for AWS access                            | wJalr/bPxRfiCYEXAMPLEKEY  |
| S3_BUCKET | str  | Bucket to host the data (must be unique) | my-duck-bucket-130dcqda0u |

These variables will be used by the setup script and the runner to communicate with AWS (S3 and Lambdas). Make sure the user has the permissions to:

* create a bucket and upload files to it;
* invoke the lambda that we create below.

### Duckdb lambda

The `src/serverless` folder is a self-contained lambda project that uses Python to leverage duckdb querying capabilities over files in an object storage. For readers not familiar with [serverless](https://www.serverless.com/framework/) projects, it contains:

* a Dockerfile, which starts from the public AWS lambda image for Python (`public.ecr.aws/lambda/python:3.9`) and add the few dependencies we need;
* a `local.env` file, containing a template for the env variables the lambda needs. Create a copy in the `serverless` folder named `.env` (do *not* commit it) and fill it with the proper values - make sure these credentials allow you to read the files we uploaded in the step above;
* an `app.py` file, containing the actual code our lambda will execute;
* a `serverless.yml` file, which ties all these things together in the infra-as-code fashion, and allows us to deploy and manage the function from the CLI.

If you have the serverless CLI setup correctly, deploying the lambda is easy as `cd` into the `serverless` folder (if you're not there already), and run `serverless deploy`. The first time, deployment will take a while as it needs to create the image, ship it to AWS and create the lambda stack - note that this is _a "one-off" thing_: 

![Confirmation in the terminal of the successful creation of our lambda.](images/serverless.png)

Note that sometimes you may get a `403 Forbidden` error when building the docker: in our experience, this usually goes away with `aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws`.

Once the deployment is finished, you can check your AWS account to see the lambda (you can even test it from the console, as we do in [this video](https://www.loom.com/share/97785a387af84924b830b9e0f35d8a1e)).

### Python environment

Create and activate a virtual environment, and install the few dependencies:

```
python -m venv venv
source venv/bin/activate
pip install -r requirements
```

Then `cd` into `src` and run a quick setup script: `python run_me_first.py`. This will accomplish few things:

* it will create a bucket on s3 with the name;
* it will download the NYC taxi dataset and upload it to the bucket, both as a unique file and as a hive-partitioned directory;
* it will print out few stats and meta-data on the dataset.

If you check your AWS console, your bucked should now have a `partitioned` folder with this structure:

![Bucket partitioning after the upload.](images/s3.png)

Note that this project has been developed and tested on Python 3.9.

### Optional: dbt

If you want to see how this architecture can bridge the gap between offline pipelines preparing artifacts, and real-time querying for BI (or other use cases), we recommend you running the [dbt DAG](https://docs.getdbt.com/terms/dag) we prepared to simulate:

* running some SQL transformations over the original dataset;
* dump the results of the transformation (the equivalent of a dashboard view) in the data lake;
* use our serverless query engine to power cheap and fast real-time visualization queries.

To do that, you will need a dbt setup. To simplify the project, we included a version that works well with [duck-dbt](https://github.com/jwills/dbt-duckdb): however, if you have dbt on Snowflake, the same exact principles apply (as in, you can export from Snowflake your final artifact and then querying it with this lambda). 

The quickest setup is running dbt locally, so you will need to set up a dbt [profile](https://docs.getdbt.com/docs/core/connection-profiles) named `duckdb-taxi` (see [here](https://github.com/jwills/dbt-duckdb) for examples), for example:

```
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

`python quack.py`

(with no arguments). When no arguments are provided, the script will run a simple count query on the file and print out the results.

If all looks good, you can now run arbitrary queries, e.g.

`python quack.py -q ...`

to get BLAH BLAH, or 

`python quack.py -q ...`

to get BLAH BLAH.

Since the amount of data that can be returned by a lambda is limited, the lambda will automatically limit your rows if you don't specific a limit in the script. You can get more data back with:

`python quack.py -q ... -limit 100`

but be mindful of the infrastructure constraints!

### Serverless BI architecture (Optional)

While you can run *totally* use the dashboard we designed directly on the dataset file in S3, we orchestrate a slightly more complex and realistic scenarios:

* a batch pipeline that produces a final artifact, from raw data;
* a dashboard allowing interactive queries on this final table (leveraging our serverless design).

As mentioned above, we assume you have completed a dbt setup successfully. Now:

* make sure the value of `S3_BUCKET` is available as env variable;
* `cd` into `dashboard/dbt` and type `dbt run`. 

At the end of the tiny DAG, you will end up with a new folder (and file) in the target bucket (note that we use the `external` strategies for duck-dbt to produce a the view, and different warehouses would need slightly different configurations here to achieve the same result). This file represents the materialized view we need to serve from our BI:

![Dashboard artifact is materialized.](images/dashboard.png)

[ NOTE: the sql files in `dbt/models/taxi' reference directly files in the bucket, so any changes you made to the setup script should be reflected here as well. ]

Now that we have a materialized view produced by our pipeline, it is time to query it! To run the front-end (a dashboard built with streamlit) go into the `dashboard` folder and run `streamlit run dashboard.py`. A page should open in the browser, displaying a chart:

![Chart of popular locations in the BI tool.](images/streamlit.png)

You can use the form to interact in real time with the dataset (video [here](https://www.loom.com/share/9d5de3ba822a445d9d117225c1b0307f)), through the serverless infrastructure we built.

### From quack to quack-reduce

TBC

## License

All the code is released without warranty, "as is" under a MIT License. This was a fun week-end project and should be treated with the appropriate sense of humour.