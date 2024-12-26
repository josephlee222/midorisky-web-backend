from chalice import Blueprint
import boto3
from .authorizers import farmer_authorizer, farm_manager_authorizer
import pymysql
import json

task_routes = Blueprint(__name__)
ssm_client = boto3.client('ssm')

# RDS connection details from environment variables
HOST = ssm_client.get_parameter(Name='/midori/rds_host', WithDecryption=True)['Parameter']['Value']
USER = ssm_client.get_parameter(Name='/midori/rds_user', WithDecryption=True)['Parameter']['Value']
PASSWORD = ssm_client.get_parameter(Name='/midori/rds_password', WithDecryption=True)['Parameter']['Value']
DB_NAME = ssm_client.get_parameter(Name='/midori/db_name', WithDecryption=True)['Parameter']['Value']

connection = pymysql.connect(host=HOST, user=USER, password=PASSWORD, database=DB_NAME, charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

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
                WHERE ta.username = %s
                GROUP BY t.id, t.title, t.description, t.created_at, t.updated_at, t.created_by, t.status
                ORDER BY t.priority ASC
            """
    elif display == 'all':
        sql = "SELECT t.id, t.title, t.description, t.created_at, t.updated_at, t.created_by, t.status, t.priority, count(ta.id) as users_assigned FROM Tasks as t LEFT JOIN TasksAssignees AS ta ON t.id = ta.taskId GROUP BY t.id, t.title, t.description, t.created_at, t.updated_at, t.created_by, t.status ORDER BY t.priority ASC"
    elif display == 'hidden':
        sql = "SELECT t.id, t.title, t.description, t.created_at, t.updated_at, t.created_by, t.status, t.priority, count(ta.id) as users_assigned FROM Tasks as t LEFT JOIN TasksAssignees AS ta ON t.id = ta.taskId WHERE t.hidden = 0 GROUP BY t.id, t.title, t.description, t.created_at, t.updated_at, t.created_by, t.status ORDER BY t.priority ASC"

    connection.ping(reconnect=True)
    with connection.cursor() as cursor:
        if display == 'my':
            cursor.execute(sql, task_routes.current_request.context['authorizer']['principalId'])
        else:
            cursor.execute(sql)
        connection.commit()

        result = cursor.fetchall()
        return json.loads(json.dumps(result, default=str))


@task_routes.route('/tasks/{id}', authorizer=farmer_authorizer, cors=True)
def get_task(id):
    sql = "SELECT t.id, t.title, t.description, t.created_at, t.updated_at, t.created_by, t.status, t.priority, t.hidden, count(ta.id) as users_assigned FROM Tasks as t LEFT JOIN TasksAssignees AS ta ON t.id = ta.taskId WHERE t.id = %s GROUP BY t.id, t.title, t.description, t.created_at, t.updated_at, t.created_by, t.status"
    assignee_sql = "SELECT username, email FROM TasksAssignees WHERE taskId = %s"
    connection.ping(reconnect=True)

    with connection.cursor() as cursor:
        cursor.execute(sql, id)
        connection.commit()
        taskResult = cursor.fetchone()

        cursor.execute(assignee_sql, id)
        connection.commit()
        assigneeResult = cursor.fetchall()

        return json.loads(json.dumps({'task': taskResult, 'assignees': assigneeResult}, default=str))