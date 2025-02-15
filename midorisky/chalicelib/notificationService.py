from chalice import Blueprint, BadRequestError, WebsocketDisconnectedError
import json
import os
from .connectHelper import create_connection
from .authorizers import login_authorizer
from .helpers import json_serial
from .wsService import Sender
import boto3

notification_service = Blueprint(__name__)
ses = boto3.client('ses')
sqs = boto3.client('sqs')
cognito_idp = boto3.client('cognito-idp')


def create_notification(itemType, id, title, message, action=None):
    # Create SQS message
    message = {
        'type': itemType,
        'id': id,
        'title': title,
        'message': message,
        'action': action
    }

    # Send SQS message
    response = sqs.send_message(
        QueueUrl=os.environ.get('SQS_URL'),
        MessageBody=json.dumps(message)
    )

@notification_service.route('/notifications', methods=['GET'], cors=True, authorizer=login_authorizer)
def get_notifications():
    username  = notification_service.current_request.context['authorizer']['principalId']
    sql = "SELECT id, username, title, subtitle, action_url, action FROM Notifications WHERE username = %s AND is_read = false ORDER BY created_at DESC LIMIT 5"

    with create_connection().cursor() as cursor:
        cursor.execute(sql, (username))
        result = cursor.fetchall()
        return result

@notification_service.route('/notifications/read', methods=['GET'], cors=True, authorizer=login_authorizer)
def read_all_notifications():
    username  = notification_service.current_request.context['authorizer']['principalId']
    sql = "UPDATE Notifications SET is_read = 1 WHERE username = %s"

    with create_connection().cursor() as cursor:
        cursor.execute(sql, (username))
        return

@notification_service.route('/notifications/read/{id}', methods=['GET'], cors=True, authorizer=login_authorizer)
def read_notification(id):
    username  = notification_service.current_request.context['authorizer']['principalId']
    sql = "UPDATE Notifications SET is_read = 1 WHERE id = %s AND username = %s"

    with create_connection().cursor() as cursor:
        cursor.execute(sql, (id, username))
        return