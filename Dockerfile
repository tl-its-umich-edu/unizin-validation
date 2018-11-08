# FROM directive instructing base image to build upon
FROM python:3.6

COPY requirements.txt /requirements.txt

RUN pip install -r /requirements.txt

# apt-utils needs to be installed separately
RUN apt-get update && \ 
    apt-get install -y --no-install-recommends python3-dev xmlsec1 cron && \
    apt-get clean -y

# COPY startup script into known file location in container
COPY start.sh /start.sh

WORKDIR /unizin-csv-validation/
COPY . /unizin-csv-validation/

CMD ["/start.sh"]