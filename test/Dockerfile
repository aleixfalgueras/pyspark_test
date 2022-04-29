ARG IMAGE_VARIANT=slim-buster
ARG OPENJDK_VERSION=8
ARG PYTHON_VERSION=3.6.9

FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT} AS py3
FROM openjdk:${OPENJDK_VERSION}-${IMAGE_VARIANT}

COPY --from=py3 / /

ARG PYSPARK_VERSION=2.4.7
RUN pip --no-cache-dir install pyspark==${PYSPARK_VERSION}

WORKDIR testing/test
COPY test.sh test.sh
RUN ["chmod", "+x", "test.sh"]