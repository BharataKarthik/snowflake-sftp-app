CREATE OR REPLACE PROCEDURE pull_files_from_sftp(
    sftp_remote_path STRING,
    sftp_server STRING,
    port INTEGER,
    internal_stage STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
HANDLER = 'fetch_files_from_sftp'
EXTERNAL_ACCESS_INTEGRATIONS = (SFTP_INTEGRATION)
SECRETS = ('cred' = sftp_cred)
PACKAGES = ('pysftp', 'pandas', 'snowflake-snowpark-python==*')
AS
$$
import pysftp
import os
import _snowflake
from snowflake.snowpark import Session

def fetch_files_from_sftp(session: Session, sftp_remote_path, sftp_server, port, internal_stage):
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

            # Change to the remote directory
            sftp.cwd(sftp_remote_path)
            remote_files = sftp.listdir()

            if not remote_files:
                return "No files found in the specified SFTP directory."

            print("Files available for download:", remote_files)

            for file_name in remote_files:
                local_temp_path = f"/tmp/{file_name}"

                # Download the file from SFTP
                sftp.get(file_name, local_temp_path)
                print(f"Downloaded file: {file_name}")

                # Upload file to Snowflake internal stage
                session.file.put(local_temp_path, f"@SFTP_STORAGE_DB.SFTP_STORAGE_SCHEMA.{internal_stage}")
                print(f"Uploaded {file_name} to Snowflake stage: {internal_stage}")

                # Remove the local temporary file
                os.remove(local_temp_path)

    except Exception as e:
        return f"SFTP connection failed. Error: {e}"

    return "SFTP files pulled and saved to Snowflake internal stage successfully."
$$;


CALL pull_files_from_sftp(
    '/TEST',                  
    'eu-west-1.sftpcloud.io',      
    22,                      
    'pull_internal_stage'
);
