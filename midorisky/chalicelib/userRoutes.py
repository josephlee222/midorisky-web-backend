from chalice import Blueprint
import boto3
import json
from .authorizers import admin_authorizer

user_routes = Blueprint(__name__)
idp_client = boto3.client('cognito-idp')
ssm_client = boto3.client('ssm')

pool_id = ssm_client.get_parameter(Name='/midori/user_pool_id', WithDecryption=True)['Parameter']['Value']

@user_routes.route('/admin/users', authorizer=admin_authorizer, cors=True)
def get_users():
    users = idp_client.list_users(
        UserPoolId=pool_id,
        AttributesToGet=[
            'name',
            'phone_number',
            'email'
        ]
    )["Users"]

    output_users = []

    for user in users:
        output_user = {"username": user["Username"], "create_at": user["UserCreateDate"],
                       "modified_at": user["UserLastModifiedDate"], "enabled": user["Enabled"],
                       "user_status": user["UserStatus"]}

        for a in user["Attributes"]:
            output_user[a["Name"]] = a["Value"]

        output_users.append(output_user)

    return json.loads(json.dumps(output_users, default=str))