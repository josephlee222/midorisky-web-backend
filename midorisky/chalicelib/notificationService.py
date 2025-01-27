from chalice import Blueprint, BadRequestError
import json
import os
from .connectHelper import create_connection
from .helpers import json_serial
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


@notification_service.on_sqs_message(queue='midori-queue', batch_size=5)
def handle_sqs_message(event):
    print("Hello world")
    for record in event:
        # Parse the record
        data = json.loads(record.body)
        print(data)

        type = data['type']
        id = data['id']
        title = data['title']
        message = data['message']

        if type == 'task':
            # query item by id
            sql = "SELECT * FROM midori.Tasks WHERE id = %s"

            with create_connection().cursor() as cursor:
                cursor.execute(sql, (id))
                item = cursor.fetchone()

                #print(json.dumps(item, default=json_serial))

                if item:
                    taskId = item['id']
                    # query notification subscribers based on categoryId
                    sql = "SELECT * FROM midori.TasksAssignees WHERE taskId = %s"

                    with create_connection().cursor() as cursor:
                        cursor.execute(sql, (taskId))
                        assignees = cursor.fetchall()

                        #print(json.dumps(assignees, default=json_serial))

                        emails = []
                        if assignees:
                            for assignee in assignees:
                                # query user by username
                                user = cognito_idp.admin_get_user(
                                    UserPoolId=os.environ.get('USER_POOL_ID'),
                                    Username=assignee['username']
                                )

                                for attribute in user['UserAttributes']:
                                    if attribute['Name'] == 'email':
                                        emails.append(attribute['Value'])
                                        break

                            print(emails)
                            # send email to users
                            response = ses.send_email(
                                Source=os.environ.get('SES_EMAIL'),
                                Destination={
                                    'ToAddresses': emails
                                },
                                Message={
                                    'Subject': {
                                        'Data': title
                                    },
                                    'Body': {
                                        'Text': {
                                            'Data': message
                                        }
                                    }
                                }
                            )

                            for email in emails:
                                print('Email sent to: ' + email)
                else:
                    return