#!/bin/bash

if [ -z "${CRONTAB_SCHEDULE}" ]; then
    echo "CRONTAB_SCHEDULE environment variable not set, crontab cannot be started. Please set this to a crontab acceptable format. Just running command."
    python /unizin-csv-validation/validate.py -o 5
else
        # in cron pod
        echo Running cron job pod
        echo "CRONTAB_SCHEDULE is ${CRONTAB_SCHEDULE}, RUN_AT_TIMES is ${RUN_AT_TIMES}"

        # Make the log file available
        touch /var/log/cron.log

        # Get the environment from docker saved
        # https://ypereirareis.github.io/blog/2016/02/29/docker-crontab-environment-variables/
        printenv | sed 's/^\([a-zA-Z0-9_]*\)=\(.*\)$/export \1="\2"/g' >> $HOME/.profile

        echo "${CRONTAB_SCHEDULE} . $HOME/.profile; python /unizin-csv-validation/validate.py -o 5 >> /var/log/cron.log 2>&1" | crontab
        crontab -l && cron -L 15 && tail -f /var/log/cron.log
fi