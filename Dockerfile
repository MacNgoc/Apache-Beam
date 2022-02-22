#syntax=docker/dockerfile:1.3
ARG beam_version=2.35.0

FROM apache/beam_python3.8_sdk:${beam_version}
WORKDIR /app

COPY requirements.txt /app/
RUN pip install -r requirements.txt

# just for build time
#RUN --mount=type=secret,id=authen_google,target=/app/mlopsmac-080afb6fa874.json
#RUN --mount=type=secret,id=aws,target=/root/.aws/credentials aws s3 cp s3://... ...


ENV GOOGLE_APPLICATION_CREDENTIALS=/app/mlopsmac-080afb6fa874.json

COPY . /app/



