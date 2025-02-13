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

    #sender.broadcast(connections, json.dumps({'type': 'notification', 'title': title, 'subtitle': subtitle, 'action_url': url, 'action': action}))
    return


@app.on_sqs_message(queue='midori-queue', batch_size=5)
def handle_sqs_message(event):
    ses = boto3.client('ses')
    sqs = boto3.client('sqs')
    cognito_idp = boto3.client('cognito-idp')
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

                    cursor.execute(sql, (taskId))
                    assignees = cursor.fetchall()

                    emails = []
                    if assignees:
                        for assignee in assignees:
                            # query user by username
                            user = cognito_idp.admin_get_user(
                                UserPoolId=os.environ.get('USER_POOL_ID'),
                                Username=assignee['username']
                            )

                            insert_notification(assignee['username'], "Task Updated", "A task you are assigned to has been updated.", '/staff/tasks?task=' + str(taskId), "View")

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


# The view function above will return {"hello": "world"}
# whenever you make an HTTP GET request to '/'.
#
# Here are a few more examples:
#
# @app.route('/hello/{name}')
# def hello_name(name):
#    # '/hello/james' -> {"hello": "james"}
#    return {'hello': name}
#
# @app.route('/users', methods=['POST'])
# def create_user():
#     # This is the JSON body the user sent in their POST request.
#     user_as_json = app.current_request.json_body
#     # We'll echo the json body back to the user in a 'user' key.
#     return {'user': user_as_json}
#
# See the README documentation for more examples.
#
