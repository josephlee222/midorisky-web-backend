from chalice import Blueprint, BadRequestError, ForbiddenError, Response
import boto3
from .authorizers import farmer_authorizer, farm_manager_authorizer
from .connectHelper import create_connection
from .notificationService import create_notification
import json
import os
import traceback
import urllib.parse as urllib
from requests_toolbelt.multipart import decoder
from .helpers import json_serial

task_routes = Blueprint(__name__)
s3 = boto3.client('s3')


@task_routes.route('/tasks', authorizer=farm_manager_authorizer, cors=True, methods=['POST'])
def create_task():
    request = task_routes.current_request
    body = request.json_body

    title = body["title"]
    description = body["description"]
    priority = body["priority"]

    sql = "INSERT INTO Tasks (title, description, priority, created_by) VALUES (%s, %s, %s, %s)"
    assigneeSql = "INSERT INTO TasksAssignees (taskId, username) VALUES (%s, %s)"

    with create_connection().cursor() as cursor:
        cursor.execute(sql, (title, description, priority, task_routes.current_request.context['authorizer']['principalId']))

        # get the last inserted item
        cursor.execute("SELECT * FROM Tasks WHERE id = %s", (cursor.lastrowid))

        result = cursor.fetchone()

        # assign the creator to the task
        cursor.execute(assigneeSql, (cursor.lastrowid, task_routes.current_request.context['authorizer']['principalId']))

        create_notification("task", result["id"], "create")
        return json.loads(json.dumps(result, default=json_serial))


# Get all tasks from the database
@task_routes.route('/tasks/list/{display}', authorizer=farmer_authorizer, cors=True)
def get_all_tasks(display):
    sql = ""
    if display == 'my':
        sql = """
                SELECT t.id, t.title, t.description, t.created_at, t.updated_at, t.created_by, t.status, t.priority,
                       COALESCE(ta_count.users_assigned, 0) as users_assigned
                FROM Tasks as t
                LEFT JOIN (
                    SELECT ta.taskId, COUNT(ta.id) as users_assigned
                    FROM TasksAssignees AS ta
                    GROUP BY ta.taskId
                ) ta_count ON t.id = ta_count.taskId
                LEFT JOIN TasksAssignees AS ta ON t.id = ta.taskId
                WHERE ta.username = %s AND t.hidden = 0
                GROUP BY t.id, t.title, t.description, t.created_at, t.updated_at, t.created_by, t.status
                ORDER BY t.priority ASC
            """
    elif display == 'all':
        sql = "SELECT t.id, t.title, t.description, t.created_at, t.updated_at, t.created_by, t.status, t.priority, count(ta.id) as users_assigned FROM Tasks as t LEFT JOIN TasksAssignees AS ta ON t.id = ta.taskId GROUP BY t.id, t.title, t.description, t.created_at, t.updated_at, t.created_by, t.status ORDER BY t.priority ASC"
    elif display == 'hidden':
        sql = "SELECT t.id, t.title, t.description, t.created_at, t.updated_at, t.created_by, t.status, t.priority, count(ta.id) as users_assigned FROM Tasks as t LEFT JOIN TasksAssignees AS ta ON t.id = ta.taskId WHERE t.hidden = 0 GROUP BY t.id, t.title, t.description, t.created_at, t.updated_at, t.created_by, t.status ORDER BY t.priority ASC"
    elif display == 'outstanding':
        sql = """
            SELECT t.id, t.title, t.description, t.created_at, t.updated_at, t.created_by, t.status, t.priority,
                   COALESCE(ta_count.users_assigned, 0) as users_assigned
            FROM Tasks as t
            LEFT JOIN (
                SELECT ta.taskId, COUNT(ta.id) as users_assigned
                FROM TasksAssignees AS ta
                GROUP BY ta.taskId
            ) ta_count ON t.id = ta_count.taskId
            LEFT JOIN TasksAssignees AS ta ON t.id = ta.taskId
            WHERE ta.username = %s AND t.hidden = 0 AND t.status = 1
            GROUP BY t.id, t.title, t.description, t.created_at, t.updated_at, t.created_by, t.status
            ORDER BY t.priority ASC
            LIMIT 3;
        """

    with create_connection().cursor() as cursor:
        if display == 'my' or display == 'outstanding':
            cursor.execute(sql, task_routes.current_request.context['authorizer']['principalId'])
        else:
            cursor.execute(sql)

        result = cursor.fetchall()
        return json.loads(json.dumps(result, default=json_serial))


@task_routes.route('/tasks/{id}/comments', authorizer=farmer_authorizer, cors=True)
def get_task_comments(id):
    sql = "SELECT * FROM TaskComments WHERE taskId = %s"

    with create_connection().cursor() as cursor:
        cursor.execute(sql, id)
        result = cursor.fetchall()
        return json.loads(json.dumps(result, default=str))


@task_routes.route('/tasks/{id}/comments', authorizer=farmer_authorizer, cors=True, methods=['POST'])
def create_task_comment(id):
    request = task_routes.current_request
    body = request.json_body

    comment = body["comment"]
    sql = "INSERT INTO TaskComments (taskId, comment, username) VALUES (%s, %s, %s)"

    with create_connection().cursor() as cursor:
        cursor.execute(sql, (id, comment, task_routes.current_request.context['authorizer']['principalId']))

        # get the last inserted item
        cursor.execute("SELECT * FROM TaskComments WHERE id = %s", (cursor.lastrowid))

        result = cursor.fetchone()
        create_notification("comment", result['id'], "comment")
        return json.loads(json.dumps(result, default=json_serial))


@task_routes.route('/tasks/{id}/comments/{commentId}', authorizer=farmer_authorizer, cors=True, methods=['DELETE'])
def delete_task_comment(id, commentId):
    sql = "DELETE FROM TaskComments WHERE id = %s AND taskId = %s"

    try:
        with create_connection().cursor() as cursor:
            cursor.execute(sql, commentId)
            return {"message": "Comment deleted successfully!"}
    except Exception as e:
        raise BadRequestError(str(e))


@task_routes.route('/tasks/{id}/comments/{commentId}', authorizer=farmer_authorizer, cors=True, methods=['PUT'])
def edit_task_comment(id, commentId):
    request = task_routes.current_request
    body = request.json_body

    comment = body["comment"]

    getSql = "SELECT * FROM TaskComments WHERE id = %s"
    sql = "UPDATE TaskComments SET comment = %s WHERE id = %s"


    with create_connection().cursor() as cursor:
        cursor.execute(getSql, commentId)

        # check if the comment author is the same as the current user
        comment = cursor.fetchone()
        if comment['createdBy'] != task_routes.current_request.context['authorizer']['principalId']:
            raise ForbiddenError("You are not authorized to update this comment")

        try:
            cursor.execute(sql, (comment, commentId))
            return {"message": "Comment updated successfully!"}
        except Exception as e:
            raise BadRequestError(str(e))


@task_routes.route('/tasks/{id}', authorizer=farmer_authorizer, cors=True)
def get_task(id):
    sql = "SELECT t.id, t.title, t.description, t.created_at, t.updated_at, t.created_by, t.status, t.priority, t.hidden, count(ta.id) as users_assigned FROM Tasks as t LEFT JOIN TasksAssignees AS ta ON t.id = ta.taskId WHERE t.id = %s GROUP BY t.id, t.title, t.description, t.created_at, t.updated_at, t.created_by, t.status"
    assignee_sql = "SELECT username, email FROM TasksAssignees WHERE taskId = %s"

    with create_connection().cursor() as cursor:
        cursor.execute(sql, id)
        taskResult = cursor.fetchone()

        cursor.execute(assignee_sql, id)
        assigneeResult = cursor.fetchall()

        return json.loads(json.dumps({'task': taskResult, 'assignees': assigneeResult}, default=json_serial))


@task_routes.route('/tasks/{id}', authorizer=farm_manager_authorizer, cors=True, methods=['DELETE'])
def delete_task(id):
    sql = "DELETE FROM Tasks WHERE id = %s"

    with create_connection().cursor() as cursor:
        cursor.execute(sql, id)

        # delete the attachments
        response = s3.list_objects_v2(
            Bucket=os.environ.get('S3_BUCKET'),
            Prefix=f'tasks/{id}/'
        )

        if 'Contents' in response:
            for obj in response['Contents']:
                s3.delete_object(
                    Bucket=os.environ.get('S3_BUCKET'),
                    Key=obj['Key']
                )

        return {"message": "Task deleted successfully!"}

@task_routes.route('/tasks/{id}', authorizer=farm_manager_authorizer, cors=True, methods=['PUT'])
def edit_task(id):
    request = task_routes.current_request
    body = request.json_body

    # Dynamically build the SQL query
    params = []
    sql = "UPDATE Tasks SET "
    available_params = ["title", "description", "priority", "status", "hidden"]

    for key in available_params:
        if key in body:
            sql += key + " = %s, "
            params.append(body[key])

    sql = sql[:-2] + " WHERE id = %s"
    params.append(id)

    getSql = "SELECT * FROM Tasks WHERE id = %s"

    try:
        with create_connection().cursor() as cursor:
            cursor.execute(sql, params)
            cursor.execute(getSql, id)
            task = cursor.fetchone()
            create_notification("task", id, "update")
            return {"message": "Task updated successfully!"}
    except Exception as e:
        raise BadRequestError(str(e))

@task_routes.route('/tasks/{id}/hide', authorizer=farm_manager_authorizer, cors=True, methods=['GET'])
def hide_task(id):
    sql = "UPDATE Tasks SET hidden = 1 WHERE id = %s"

    with create_connection().cursor() as cursor:
        cursor.execute(sql, id)
        return {"message": "Task hidden successfully!"}

@task_routes.route('/tasks/{id}/status', authorizer=farmer_authorizer, cors=True, methods=['PUT'])
def update_task_status(id):
    request = task_routes.current_request
    body = request.json_body

    status = body["status"]

    sql = "UPDATE Tasks SET status = %s WHERE id = %s"

    # Check if the user is assigned to the task or if the user is the creator of the task
    check_sql = "SELECT * FROM TasksAssignees WHERE taskId = %s AND username = %s"

    with create_connection().cursor() as cursor:
        cursor.execute(check_sql, (id, task_routes.current_request.context['authorizer']['principalId']))
        result = cursor.fetchone()

        if not result:
            raise ForbiddenError("You are not authorized to update the status of this task")
        cursor.execute(sql, (status, id))

        create_notification("task", id, "update")
        return {"message": "Task status updated successfully!"}


@task_routes.route('/tasks/{id}/assignees', cors=True, methods=['GET'], authorizer=farmer_authorizer)
def get_task_assignees(id):
    sql = "SELECT username FROM TasksAssignees WHERE taskId = %s"

    with create_connection().cursor() as cursor:
        cursor.execute(sql, id)
        result = cursor.fetchall()

        # put usernames in a list
        result = [x['username'] for x in result]
        return result


@task_routes.route('/tasks/{id}/assignees', cors=True, methods=['POST'], authorizer=farm_manager_authorizer)
def set_task_assignees(id):
    request = task_routes.current_request
    body = request.json_body

    assignees = body["assignees"]

    delete_sql = "DELETE FROM TasksAssignees WHERE taskId = %s"
    sql = "INSERT INTO TasksAssignees (taskId, username) VALUES (%s, %s)"

    with create_connection().cursor() as cursor:
        cursor.execute(delete_sql, id)
        for assignee in assignees:
            cursor.execute(sql, (id, assignee))

        create_notification("task", id, "assignee")

    return {"message": "Assignees added successfully!"}


@task_routes.route('/tasks/{id}/attachments', authorizer=farmer_authorizer, cors=True)
def get_task_attachments(id):
    attachments = get_attachments(id)
    return attachments


@task_routes.route('/tasks/{id}/attachments/{filename}', cors=True, methods=['GET'], authorizer=farmer_authorizer)
def get_task_attachment(id, filename):
    filename = urllib.unquote(filename)
    try:
        response = s3.get_object(
            Bucket=os.environ.get('S3_BUCKET'),
            Key="tasks/" + id + "/" + filename
        )
    except Exception as e:
        traceback.print_exc()
        raise BadRequestError("Error fetching attachment")

    return Response(body=response['Body'].read(), headers={'Content-Type': response['ContentType']})


@task_routes.route('/tasks/{id}/attachments/{filename}', cors=True, methods=['DELETE'], authorizer=farm_manager_authorizer)
def delete_task_attachment(id, filename):
    try:
        filename = urllib.unquote(filename)
        s3.delete_object(
            Bucket=os.environ.get('S3_BUCKET'),
            Key=f'tasks/{id}/{filename}'
        )
    except Exception as e:
        traceback.print_exc()
        raise BadRequestError("Error deleting attachment")

    sql = "DELETE FROM taskAttachments WHERE taskId = %s AND filename = %s"

    with create_connection().cursor() as cursor:
        cursor.execute(sql, (id, filename))

    return {'message': 'File deleted successfully'}


@task_routes.route('/tasks/{id}/attachments', cors=True, methods=['POST'], content_types=['multipart/form-data'], authorizer=farm_manager_authorizer)
def upload_task_attachment(id):
    request = task_routes.current_request
    body = request.raw_body

    # decode the multipart form data
    d = decoder.MultipartDecoder(body, request.headers['content-type'])
    file = None
    filename = None

    part = d.parts[1]
    if part.headers[b'Content-Disposition']:
        filename = part.headers[b'Content-Disposition'].decode('utf-8').split('filename=')[1].strip('"')

        file = part.content

    if not file or not filename:
        raise BadRequestError('File not found in request')

    # upload the file to S3
    s3.put_object(
        Bucket=os.environ.get('S3_BUCKET'),
        Key=f'tasks/{id}/{filename}',
        Body=file,
        ContentType=part.headers[b'Content-Type'].decode('utf-8')
    )

    sql = "INSERT INTO taskAttachments (taskId, filename) VALUES (%s, %s)"

    with create_connection().cursor() as cursor:
        cursor.execute(sql, (id, filename))

    return {'message': 'File uploaded successfully'}


def get_attachments(task_id):
    response = s3.list_objects_v2(
        Bucket='midori-bucket',
        Prefix=f'tasks/{task_id}/'
    )

    attachments = []
    if 'Contents' in response:
        for obj in response['Contents']:
            # remove the folder name from the list of attachments
            obj['Key'] = obj['Key'].replace(f'tasks/{task_id}/', '')
            attachments.append(obj['Key'])

    return attachments
