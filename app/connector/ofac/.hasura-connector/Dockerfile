FROM ghcr.io/hasura/ndc-python-lambda:v0.2.0

COPY requirements.txt /functions/

WORKDIR /functions

RUN python3 -m venv venv && \
    . venv/bin/activate && \
    pip install -r requirements.txt

COPY ./ /functions

# create the group and user
RUN adduser -u 1000 python

RUN chown -R python:python /functions

# stating USER before WORKDIR means the directory is created with the non-root proper ownership
USER python