import datetime
import os
import sqlalchemy
from google.cloud import secretmanager

#fetching project id from generic environment variable
project_id = os.environ["GCP_PROJECT"]

#creating a client for secret manager service
client = secretmanager.SecretManagerServiceClient()

#fetch latest version of secret with respect to the specific secret name from secret manager
db_user_secret_name = "db-username"
resource_name = f"projects/{project_id}/secrets/{db_user_secret_name}/versions/latest"
response = client.access_secret_version(resource_name)
db_user = response.payload.data.decode('UTF-8')
db_pass = db_user

db_name = "classicmodels"
cloud_sql_connection_name = "gcp-time-savers"

#Constructing a connection string to connect to CloudSQL using SQLAlchemy phyton libarary toolkit
db = sqlalchemy.create_engine(
    # Equivalent URL:
    # mysql+pymysql://<db_user>:<db_pass>@/<db_name>?unix_socket=/cloudsql/project-id:region-name:<cloud_sql_instance_name>
    sqlalchemy.engine.url.URL(
        drivername="mysql+pymysql",
        username=db_user,
        password=db_pass,
        database=db_name,
        query={"unix_socket": "/cloudsql/gcp-time-savers:us-central1:{}".format(cloud_sql_connection_name)},
    ),
    pool_size=5,
    max_overflow=2,
    pool_timeout=30,
    pool_recycle=1800,
)

#Simple method to demonstrate executing an action against the database. In this specific case, it is an update statement
def execute_db_action():
    with db.connect() as conn:
        conn.execute(
            "update products set productDescription='me' where productCode='S72_3212';"
        )
        return 'DB Action Executed!!'
#Main method
def connect_to_cloud_sql(request):
    request_json = request.get_json()
    if request.args and 'message' in request.args:
        return request.args.get('message')
    elif request_json and 'message' in request_json:
        return request_json['message']
    else:
        result = execute_db_action()
        return result
