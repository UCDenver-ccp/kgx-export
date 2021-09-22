
# Use the official lightweight Python image.
# https://hub.docker.com/_/python
FROM python:3.9

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# Copy local code to the container image.
ENV APP_HOME .
WORKDIR $APP_HOME
COPY . ./

# Install production dependencies.
RUN pip install SQLAlchemy mysqlclient pymysql google-cloud-storage git+https://github.com/GoogleCloudPlatform/cloud-sql-python-connector

ENTRYPOINT ["python", "targeted.py"]
