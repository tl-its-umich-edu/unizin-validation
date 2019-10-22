# unizin-validation
Unizin UDW Validation Scripts

These scripts are used for validating that the UDW data loaded into Unizin is correct.

To connect to this database you'll likely need a VPN or Ethernet that has been granted permission.

To run this
1. Copy the .env.sample file to .env this has one entry ENV_FILE
2. Copy the env_sample.json to some local directory or somewhere on the file system
3. Edit the values in this file to add your Google Cloud Database values

docker build -t unizin-sis-validate . && docker run -d --env-file .env unizin-sis-validate

You can also run this on localhost, just pass the location of your environment file. Make sure all requirements are installed or you're in a virtual environment.

`ENV_FILE=/Users/jonespm/mylasecrets/env-unizin.json python validate.py`
