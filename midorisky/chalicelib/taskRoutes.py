from chalice import Blueprint, BadRequestError
import boto3
from .authorizers import farmer_authorizer, farm_manager_authorizer
from .connectHelper import create_connection
import json

task_routes = Blueprint(__name__)

# Get all tasks from the database
@task_routes.route('/tasks/list/{display}', authorizer=farm_manager_authorizer, cors=True)
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
        return json.loads(json.dumps(result, default=str))


@task_routes.route('/tasks/{id}', authorizer=farmer_authorizer, cors=True)
def get_task(id):
    sql = "SELECT t.id, t.title, t.description, t.created_at, t.updated_at, t.created_by, t.status, t.priority, t.hidden, count(ta.id) as users_assigned FROM Tasks as t LEFT JOIN TasksAssignees AS ta ON t.id = ta.taskId WHERE t.id = %s GROUP BY t.id, t.title, t.description, t.created_at, t.updated_at, t.created_by, t.status"
    assignee_sql = "SELECT username, email FROM TasksAssignees WHERE taskId = %s"

    with create_connection().cursor() as cursor:
        cursor.execute(sql, id)
        taskResult = cursor.fetchone()

        cursor.execute(assignee_sql, id)
        assigneeResult = cursor.fetchall()

        return json.loads(json.dumps({'task': taskResult, 'assignees': assigneeResult}, default=str))


@task_routes.route('/tasks/{id}', authorizer=farm_manager_authorizer, cors=True, methods=['DELETE'])
def delete_task(id):
    sql = "DELETE FROM Tasks WHERE id = %s"

    with create_connection().cursor() as cursor:
        cursor.execute(sql, id)
        return {"message": "Task deleted successfully!"}

@task_routes.route('/tasks/{id}', authorizer=farm_manager_authorizer, cors=True, methods=['PUT'])
def edit_task(id):
    request = task_routes.current_request
    body = request.json_body

    title = body["title"]
    description = body["description"]
    priority = body["priority"]

    # Dynamically build the SQL query
    sql = "UPDATE Tasks SET "
    if title:
        sql += "title = %s, "
    if description:
        sql += "description = %s, "
    if priority:
        sql += "priority = %s, "

    sql = sql[:-2] + " WHERE id = %s"

    # Dynamically build the parameters
    params = []
    if title:
        params.append(title)
    if description:
        params.append(description)
    if priority:
        params.append(priority)

    params.append(id)

    try:
        with create_connection().cursor() as cursor:
            cursor.execute(sql, params)
            return {"message": "Task updated successfully!"}
    except Exception as e:
        return BadRequestError(str(e))

@task_routes.route('/tasks', authorizer=farm_manager_authorizer, cors=True, methods=['POST'])
def create_task():
    request = task_routes.current_request
    body = request.json_body

    title = body["title"]
    description = body["description"]
    priority = body["priority"]

    sql = "INSERT INTO Tasks (title, description, priority, created_by) VALUES (%s, %s, %s, %s)"

    with create_connection().cursor() as cursor:
        cursor.execute(sql, (title, description, priority, task_routes.current_request.context['authorizer']['principalId']))
        return {"message": "Task created successfully!"}

