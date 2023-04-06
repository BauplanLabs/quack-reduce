# quack-reduce
A playground for running duckdb as a stateless query engine over a data lake

## Overview

TBC

## Setup

### Accounts

Make sure to have:

* a working AWS account;
* the [serverless framework](https://www.serverless.com/framework/) properly [installed and configured](https://www.serverless.com/framework/docs/getting-started) (while you can create the container on ECR manually and then the lambda from the console, the instruction below assumes you just use the CLI for the entire setup).

### Global variables

In the `src` folder, you should copy `local.env` to `.env` (do *not* commit it) and fill it with proper values:

| value     | type | description                                          |                   example |
|-----------|------|------------------------------------------------------|--------------------------:|
| S3_USER   | str  | User key for AWS access                              | AKIAIOSFODNN7EXAMPLE      |
| S3_SECRET | str  | Secret key for AWS access                            | wJalr/bPxRfiCYEXAMPLEKEY  |
| S3_BUCKET | str  | Bucket to host the data (must be unique) | my-duck-bucket-130dcqda0u |

This variables will be used by the setup script and the runner to communicate with AWS (S3 and Lambdas). Make sure the user has the permissions to:

* create a bucket and upload files to it;
* invoke the lambda that we create below.

### Duckdb lambda

The `src/serverless` folder is a self-contained lambda project that uses Python to leverage duckdb querying capabilities over files in an object storage. For readers not familiar with [serverless](https://www.serverless.com/framework/) projects, it contains:

* a Dockerfile, which starts from the public AWS lambda image for Python (`public.ecr.aws/lambda/python:3.9`) and add the few dependencies we need;
* a `local.env` file, containing a template for the env variables the lambda needs. Create a copy in the `serverless` folder named `.env` (do *not* commit it) and fill it with the proper values - make sure these credentials allow you to read the files we uploaded in the step above;
* an `app.py` file, containing the actual code our lambda will execute;
* a `serverless.yml` file, which ties all these things together in the infra-as-code fashion, and allows us to deploy and manage the function from the CLI.

If you have the serverless CLI setup correctly, deploying the lambda is easy as `cd` into the `serverless` folder (if you're not there already), and run `serverless deploy` (the first time, deployment will take a while as it needs to create the image, ship it to AWS and create the lambda stack - note that this is a "one-off" thing).

Once the deployment is finished, you can check your AWS account to see the lambda (you can even test it from the console, as we do in this video LINK).

### Python environment

Create a virtual environment:

```
python -m venv venv
source venv/bin/activate
pip install -r requirements
```

Then `cd` into `src` and run a quick setup script: `python run_me_first.py`. This will accomplish few things:

* it will create a bucket on s3 with the name;
* it will download the NYC taxi dataset and upload it to the bucket, both as a unique file and as a hive-partitioned directory;
* it will print out few stats and meta-data on the dataset.

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

### Building serverless BI

TBC

### From quack to quack-reduce

TBC

## License

All the code is released without warranty, "as is" under a MIT License. This was a fun week-end project and should be treated with the appropriate sense of humour.