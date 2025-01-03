from chalice import Blueprint
import boto3
import json
from .authorizers import farmer_authorizer
import pymysql
from .connectHelper import connection

farm_routes = Blueprint(__name__)

@farm_routes.route('/farms', authorizer=farmer_authorizer, cors=True)
def get_farms():
    sql = "SELECT * FROM `Farms`"

    with connection.cursor() as cursor:
        cursor.execute(sql)
        connection.commit()
        result = cursor.fetchall()
        return result