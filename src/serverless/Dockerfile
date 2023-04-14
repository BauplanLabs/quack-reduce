FROM public.ecr.aws/lambda/python:3.9

# Install pip and other dependencies
RUN pip3 install --upgrade pip \
    && yum install gcc gcc-c++ -y \
    && pip3 install pandas==1.5.3 duckdb==0.7.1 --target "${LAMBDA_TASK_ROOT}"

ENV HOME=/home/aws

RUN mkdir /home/aws && python3 -c "import duckdb; duckdb.query('INSTALL httpfs;');"

COPY app.py ${LAMBDA_TASK_ROOT}

# Set the CMD to the lambda handler
CMD [ "app.handler" ]