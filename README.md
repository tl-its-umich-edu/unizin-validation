# unizin-sis-validation
Unizin SIS Validation Scripts

These scripts are used for validating that the SIS data loaded into Unizin is correct.

It needs a .env file configured with the record(s) below. In addition to connect to this database at all you'll likely need a VPN.

#This configures the PostGres DSN to the entity store for the SIS data
DSN=dbname=entity_store host=192.168.0.1 user=user_readonly password=user_password connect_timeout=5

TODO: 
Add automatic download from S3 bucket for SIS CSV files using boto3
