from boto3 import session
from .connectHelper import create_connection
import os

wsSession = session.Session()

# Get from environment variables
wsClient = wsSession.client('apigatewaymanagementapi', endpoint_url="https://" + os.environ.get('WS_API_ID') + ".execute-api." + os.environ.get('REGION') + ".amazonaws.com/api")

class Sender(object):
    """Class to send messages over websockets."""
    def __init__(self, app):
        """Initialize a sender object.
        :param app: A Chalice application object.
        """
        self._app = app

    def send(self, connection_id, message):
        """Send a message over a websocket.

        :param connection_id: API Gateway Connection ID to send a
            message to.

        :param message: The message to send to the connection.
        """
        try:
            wsClient.post_to_connection(ConnectionId=connection_id, Data=message)
        except wsClient.exceptions.GoneException:
            # If the connection is gone, delete it from the database
            with create_connection().cursor() as cursor:
                cursor.execute("DELETE FROM wsConnections WHERE connection_id = %s", (connection_id))


    def broadcast(self, connection_ids, message):
        """Send a message to multiple connections.

        :param connection_id: A list of API Gateway Connection IDs to
            send the message to.

        :param message: The message to send to the connections.
        """
        for cid in connection_ids:
            self.send(cid, message)