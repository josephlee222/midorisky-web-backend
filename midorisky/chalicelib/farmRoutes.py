from chalice import Blueprint
import boto3
import json
from .authorizers import farmer_authorizer
import pymysql

farm_routes = Blueprint(__name__)
idp_client = boto3.client('cognito-idp')
ssm_client = boto3.client('ssm')

# RDS connection details from environment variables
HOST = ssm_client.get_parameter(Name='/midori/rds_host', WithDecryption=True)['Parameter']['Value']
USER = ssm_client.get_parameter(Name='/midori/rds_user', WithDecryption=True)['Parameter']['Value']
PASSWORD = ssm_client.get_parameter(Name='/midori/rds_password', WithDecryption=True)['Parameter']['Value']
DB_NAME = ssm_client.get_parameter(Name='/midori/db_name', WithDecryption=True)['Parameter']['Value']

@farm_routes.route('/farms', authorizer=farmer_authorizer, cors=True)
def get_farms():
    connection = pymysql.connect(host=HOST, user=USER, password=PASSWORD, database=DB_NAME, charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    sql = "SELECT * FROM `Farms`"

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
            return result