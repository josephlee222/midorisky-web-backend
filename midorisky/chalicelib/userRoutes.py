from chalice import Blueprint, BadRequestError
import boto3
import json
from .authorizers import admin_authorizer

user_routes = Blueprint(__name__)
idp_client = boto3.client('cognito-idp')
ssm_client = boto3.client('ssm')

pool_id = ssm_client.get_parameter(Name='/midori/user_pool_id', WithDecryption=True)['Parameter']['Value']

@user_routes.route('/admin/users', authorizer=admin_authorizer, cors=True, methods=['GET'])
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


@user_routes.route('/admin/users', authorizer=admin_authorizer, cors=True, methods=['POST'])
def create_user():
    request = user_routes.current_request
    body = request.json_body

    username = body["username"]
    name = body["name"]
    email = body["email"]
    group = body["group"]

    try:
        created_user = idp_client.admin_create_user(
            UserPoolId=pool_id,
            Username=username,
            UserAttributes=[
                {
                    'Name': 'name',
                    'Value': name
                },
                {
                    'Name': 'email',
                    'Value': email
                },
            ],
            DesiredDeliveryMediums=[
                'EMAIL'
            ]
        )
    except idp_client.exceptions.UsernameExistsException:
        raise BadRequestError("Username already exists")

    if group != "normal" and (group in ["admin", "farmManager", "farmer"]):
        group_res = idp_client.admin_add_user_to_group(
            UserPoolId=pool_id,
            Username=username,
            GroupName=group
        )

    return {"message": "User created successfully"}