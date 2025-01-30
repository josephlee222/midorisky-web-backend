from chalice import Blueprint, Response, BadRequestError
from chalice import Chalice
import boto3
import pandas as pd
import json
from .connectHelper import create_connection

# Create a Blueprint for weather data routes
device_routes = Blueprint(__name__)

s3 = boto3.client('s3')

@device_routes.route('/staff/devices/view-all-devices', methods=['GET'],  cors=True)
def fetch_all_devices():
    """
    Fetch all devices data based on the latest timestamp rounded to the previous 30-minute interval.
    """
    query = """
    SELECT *
    FROM IoTDevices
    """
    # Fetch data from the database
    with create_connection().cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
        return result

@device_routes.route('/staff/devices/create-device', methods=['POST'], cors=True)
def create_device():
    """
    Create a new device and insert it into the IoTDevices table.
    Expects a JSON body with 'IoTType', 'IoTSerialNumber', and 'PlotID'.
    'IoTStatus' will automatically be set to 1 (active), and 'Timestamp' will be set to the current time.
    """
    try:
        # Access the current request
        data = device_routes.current_request.json_body
        if not data:
            raise BadRequestError("Invalid JSON payload.")

        # Validate required fields
        required_fields = ['IoTType', 'IoTSerialNumber', 'PlotID']
        for field in required_fields:
            if field not in data:
                raise BadRequestError(f"Missing required field: {field}")

        # Prepare the SQL query
        query = """
        INSERT INTO IoTDevices (IoTType, IoTSerialNumber, IoTStatus, PlotID)
        VALUES (%s, %s, 1, %s)
        """

        # Execute the query
        with create_connection().cursor() as cursor:
            cursor.execute(query, (
                data['IoTType'],  # IoTType
                data['IoTSerialNumber'],  # IoTSerialNumber
                data['PlotID']  # PlotID
            ))
            create_connection().commit()

        return Response(
            body=json.dumps({"message": "Device created successfully"}),
            status_code=201,
            headers={'Content-Type': 'application/json'}
        )

    except BadRequestError as e:
        return Response(
            body=json.dumps({"error": str(e)}),
            status_code=400,
            headers={'Content-Type': 'application/json'}
        )
    except Exception as e:
        return Response(
            body=json.dumps({"error": str(e)}),
            status_code=500,
            headers={'Content-Type': 'application/json'}
        )

@device_routes.route('/staff/devices/delete-device', methods=['DELETE'], cors=True)
def delete_device():
    """
    Delete a device by ID provided in the query parameters.
    """
    try:
        # Retrieve `device_id` from query parameters
        device_id = device_routes.current_request.query_params.get('id')
        if not device_id:
            raise BadRequestError("Device ID is required.")

        # SQL deletion query
        query = """
        DELETE FROM IoTDevices
        WHERE id = %s
        """

        with create_connection().cursor() as cursor:
            cursor.execute(query, (device_id,))
            create_connection().commit()

        return Response(
            body=json.dumps({"message": "Device deleted successfully"}),
            status_code=200,
            headers={'Content-Type': 'application/json'}
        )

    except BadRequestError as e:
        return Response(
            body=json.dumps({"error": str(e)}),
            status_code=400,
            headers={'Content-Type': 'application/json'}
        )
    except Exception as e:
        return Response(
            body=json.dumps({"error": str(e)}),
            status_code=500,
            headers={'Content-Type': 'application/json'}
        )

