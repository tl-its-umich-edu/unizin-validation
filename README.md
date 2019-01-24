# unizin-sis-validation
Unizin SIS Validation Scripts

These scripts are used for validating that the SIS data loaded into Unizin is correct.

To connect to this database you'll likely need a VPN or Ethernet that has been granted permission.

To run this
1. Copy the .env.sample file to .env
2. Edit the values in this file to add your Google Cloud Database values
3. Manually Download the CSV files that are loaded from the S3 bucket (This may be automated in the future for automated validation)
4. If they don't match our file names, edit dbqueries.py and edit the sis_files values to match up with your csv file names

If SMTP is needed to test you can run a local SMTP server as described in sample env.

`python -m smtpd -n -c DebuggingServer localhost:1025`

These scripts have currently only been tested for Michigan. They also do not 100% validate yet and some of the empty columns and are a work in progress as of 2018-09-10. 

Run the python script validate.py selecting first option 1 to download the data from the Postgres in Google.

The run Option 2 to validate all tables. 

Option 3 will run all the tables except course_section_enrollment which currently has a lot of columns that don't match up.

To build this on docker you can do

docker build -t unizin-sis-validate . && docker run -d --env-file .env unizin-sis-validate

TODO: 
Add automatic download from S3 bucket for SIS CSV files using boto3
