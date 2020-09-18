# FROM directive instructing base image to build upon
FROM python:3.8-slim

COPY requirements.txt /requirements.txt

RUN pip install -r /requirements.txt

# apt-utils needs to be installed separately
RUN apt-get update && \ 
    apt-get install -y --no-install-recommends build-essential && \
    apt-get clean -y

WORKDIR /app/
COPY . /app/

# Sets the local timezone of the docker image
ENV TZ=America/Detroit
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

CMD ["python", "validate.py"]

# Done!