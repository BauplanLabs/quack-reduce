FROM public.ecr.aws/lambda/python:3.9

# Install pip and other dependencies
RUN pip3 install --upgrade pip
RUN yum install gcc -y
RUN yum install gcc-c++ -y

COPY app.py ${LAMBDA_TASK_ROOT}

# Install python dependencies
RUN  pip3 install duckdb==0.6.0 --target "${LAMBDA_TASK_ROOT}"
RUN  pip3 install pandas==1.5.3 --target "${LAMBDA_TASK_ROOT}"

# Set the CMD to the lambda handler
CMD [ "app.handler" ]