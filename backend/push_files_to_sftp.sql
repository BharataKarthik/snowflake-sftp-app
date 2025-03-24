CREATE OR REPLACE PROCEDURE push_files_to_sftp(
    sftp_remote_path STRING,
    sftp_server STRING,
    port INTEGER,
    internal_stage STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
HANDLER = 'check_sftp_connection'
EXTERNAL_ACCESS_INTEGRATIONS = (SFTP_INTEGRATION)
SECRETS = ('cred' = sftp_cred)
PACKAGES = ('pysftp', 'pandas', 'snowflake-snowpark-python==*')
AS
$$
import pysftp
import pandas as pd
import os
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import _snowflake

def check_sftp_connection(session: Session, sftp_remote_path, sftp_server, port, internal_stage):
    # Fetch SFTP credentials from Snowflake secrets
    sftp_cred = _snowflake.get_username_password('cred')
    sftp_host = sftp_server
    sftp_username = sftp_cred.username
    sftp_password = sftp_cred.password

    # Set up the SFTP connection options
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None  # Disable host key verification for simplicity

    try:
        # Connect to the SFTP server
        with pysftp.Connection(
            host=sftp_host,
            username=sftp_username,
            password=sftp_password,
            port=port,
            cnopts=cnopts
        ) as sftp:
            print("SFTP connection successful!")

            files_to_transfer = []

            # Get the list of files from the Snowflake stage
            stage_files = session.sql(f"LIST @SFTP_STORAGE_DB.SFTP_STORAGE_SCHEMA.{internal_stage}").collect()

            print(stage_files)
            
            # Extract file names from the stage files
            for file in stage_files:
                files_to_transfer.append(file['name'].split('/')[-1])

            print("Files to transfer:", files_to_transfer)
            
            # Navigate to the remote directory on the SFTP server
            with sftp.cd("TEST"):
                for file_name in files_to_transfer:
                    session.file.get(f"@SFTP_STORAGE_DB.SFTP_STORAGE_SCHEMA.push_internal_stage/{file_name}", "/tmp")
                    sftp.put(f"/tmp/{file_name}")
                    print(f"Uploaded to SFTP TEST folder: {file_name}")

                    os.remove(f"/tmp/{file_name}")
                    

    except Exception as e:
        return f"SFTP connection failed. Error: {e}"

    return "SFTP transfer completed successfully."
$$;


CALL push_files_to_sftp(
    '/TEST',                 
    'eu-west-1.sftpcloud.io',      
    22,                      
    'push_internal_stage'
);
