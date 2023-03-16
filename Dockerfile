FROM ubuntu:latest
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    openjdk-17-jre-headless

RUN pip install "dask[dataframe]"
RUN  pip install dask_sql psycopg2-binary pyarrow sqlalchemy scipy matplotlib seaborn
RUN pip install -i https://test.pypi.org/simple/ biopredict-python-tool

