from chalice import Chalice
from boto3.session import Session
import boto3
from chalicelib.userRoutes import user_routes
from chalicelib.farmRoutes import farm_routes
from chalicelib.taskRoutes import task_routes
from chalicelib.weatherRoutes import weather_routes
from chalicelib.deviceRoutes import device_routes
from chalicelib.notificationService import notification_service
from chalicelib.authorizers import auth_functions, admin_authorizer, farmer_authorizer
from chalicelib.wsService import Sender
import json
from chalicelib.connectHelper import create_connection
import os

app = Chalice(app_name='midorisky')
app.websocket_api.session = Session()
app.experimental_feature_flags.update([
    'WEBSOCKETS',
])

sender = Sender(app)
app.register_blueprint(user_routes)
app.register_blueprint(farm_routes)
app.register_blueprint(task_routes)
app.register_blueprint(notification_service)
app.register_blueprint(auth_functions)
app.register_blueprint(weather_routes)
app.register_blueprint(device_routes)


app.api.binary_types.append('multipart/form-data')

@app.route('/', cors=True)
def index():
    return {'message': 'Hello world, from MidoriSKY!'}

@app.route('/test/admin', authorizer=admin_authorizer, cors=True)
def test_admin():
    return {'message': 'You have access to admin routes!'}

@app.route('/test/farmer', authorizer=farmer_authorizer, cors=True)
def test_farmer():
    return {'message': 'You have access to farm routes!'}

@app.route('/test/weather', cors=True)
def test_weather():
    return {'message': 'You have access to weather routes!'}

@app.on_ws_connect()
def connect(event):
    print("Connection established")
    return json.dumps({'message': 'Connected'})


@app.on_ws_disconnect()
def disconnect(event):
    with create_connection().cursor() as cursor:
        # Delete connection ID username association
        cursor.execute("DELETE FROM wsConnections WHERE connection_id = %s", (event.connection_id))

    print("Connection terminated")
    return json.dumps({'message': 'Disconnected'})

@app.on_ws_message()
def message(event):
    print("Message received")

    message = json.loads(event.body)
    connection_id = event.connection_id

    if message['username']:
        # Store connection ID username association
        with create_connection().cursor() as cursor:
            cursor.execute("INSERT INTO wsConnections (connection_id, username) VALUES (%s, %s)", (connection_id, message['username']))

    return

def insert_notification(username, title, subtitle, url, action="View"):
    sql = "INSERT INTO Notifications (username, title, subtitle, action_url, action) VALUES (%s, %s, %s, %s, %s)"
    connections_sql = "SELECT connection_id FROM wsConnections WHERE username = %s"
    connections = []

    with create_connection().cursor() as cursor:
        cursor.execute(sql, (username, title, subtitle, url, action))
        cursor.execute(connections_sql, (username))
        connections = cursor.fetchall()

        # Get notification just created
        cursor.execute("SELECT id, username, title, subtitle, action_url, action FROM Notifications WHERE username = %s ORDER BY created_at DESC LIMIT 1", (username))
        notification = cursor.fetchone()


    # put connections in a list
    connections_list = [connection['connection_id'] for connection in connections]
    sender.broadcast(connections_list, json.dumps({'id': notification['id'], 'type': 'notification', 'title': title, 'subtitle': subtitle, 'action_url': url, 'action': action}))
    return


@app.on_sqs_message(queue='midori-queue', batch_size=5)
def handle_sqs_message(event):

    for record in event:
        # Parse the record
        data = json.loads(record.body)
        print(data)

        type = data['type']

        if type == 'task' or type == 'assignee':
            handleTaskType(data)
        elif type == 'comment':
            handleCommentType(data)


def handleCommentType(data):
    ses = boto3.client('ses')
    sqs = boto3.client('sqs')
    cognito_idp = boto3.client('cognito-idp')

    id = data['id']
    action = data['action']

    # query item by id
    sql = "SELECT * FROM TaskComments WHERE id = %s"

    with create_connection().cursor() as cursor:
        cursor.execute(sql, (id))
        item = cursor.fetchone()

        if item:
            taskId = item['taskId']

            # query task by taskId
            sql = "SELECT * FROM Tasks WHERE id = %s"
            cursor.execute(sql, (taskId))
            task = cursor.fetchone()

            # query task assignees
            sql = "SELECT * FROM TasksAssignees WHERE taskId = %s"
            cursor.execute(sql, (taskId))
            assignees = cursor.fetchall()
            for assignee in assignees:
                username = assignee['username']
                insert_notification(username, task["title"] + " - New Task Comment", "A new comment has been added to a task you are assigned to:\n\n" + item['comment'], '/staff/tasks?task=' + str(taskId), "View Task")



def handleTaskType(data):
    ses = boto3.client('ses')
    sqs = boto3.client('sqs')
    cognito_idp = boto3.client('cognito-idp')


    id = data['id']
    action = data['action']

    # query item by id
    sql = "SELECT * FROM midori.Tasks WHERE id = %s"

    with create_connection().cursor() as cursor:
        cursor.execute(sql, (id))
        item = cursor.fetchone()

        # print(json.dumps(item, default=json_serial))

        if item:
            taskId = item['id']
            # query notification subscribers based on categoryId
            sql = "SELECT * FROM midori.TasksAssignees WHERE taskId = %s"

            cursor.execute(sql, (taskId))
            assignees = cursor.fetchall()

            emailContent = {
                'update': {
                    'title': 'Task Updated',
                    'message': "A task has been updated. Please check the task for more details.\n\nTask Details:\nTitle: " +
                               item['title'] + "\nDescription: " + item['description']
                },
                'create': {
                    'title': 'Task Created',
                    'message': "A task has been created. Please check the task for more details.\n\nTask Details:\nTitle: " +
                               item['title'] + "\nDescription: " + item['description']
                },
                'assignee': {
                    'title': 'Task Assigned',
                    'message': "You have been assigned to a task or its assignees has been changed. Please check the task for more details.\n\nTask Details:\nTitle: " +
                               item['title'] + "\nDescription: " + item['description']
                }
            }

            emails = []
            if assignees:
                for assignee in assignees:
                    # query user by username
                    user = cognito_idp.admin_get_user(
                        UserPoolId=os.environ.get('USER_POOL_ID'),
                        Username=assignee['username']
                    )

                    if action == 'create':
                        insert_notification(assignee['username'], "Task Created", "A new task has been created:\n" + item['title'], '/staff/tasks?task=' + str(taskId), "View Task")
                    elif action == 'update':
                        insert_notification(assignee['username'], "Task Updated", "A task has been updated:\n" + item['title'], '/staff/tasks?task=' + str(taskId), "View Task")
                    elif action == 'assignee':
                        insert_notification(assignee['username'], "Task Assigned", "You have been assigned to a task or its assignees has been changed:\n" + item['title'], '/staff/tasks?task=' + str(taskId), "View Task")

                    for attribute in user['UserAttributes']:
                        if attribute['Name'] == 'email':
                            emails.append(attribute['Value'])
                            break

                # send email to users
                response = ses.send_email(
                    Source=os.environ.get('SES_EMAIL'),
                    Destination={
                        'ToAddresses': emails
                    },
                    Message={
                        'Subject': {
                            'Data': emailContent[action]['title']
                        },
                        'Body': {
                            'Text': {
                                'Data': emailContent[action]['message']
                            }
                        }
                    }
                )

                for email in emails:
                    print('Email sent to: ' + email)
        else:
            return