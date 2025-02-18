from chalice import Chalice, Blueprint, Response, BadRequestError
import boto3
import json
import datetime
import random
import os
import hashlib
from .connectHelper import create_connection
from .helpers import json_serial

from botocore.config import Config
from chalice.app import Rate

app = Chalice(app_name='midorisky')
app.debug = True

# Create a Blueprint for weather data routes
device_routes = Blueprint(__name__)

s3 = boto3.client('s3')

@device_routes.route('/staff/devices/view-all-devices', methods=['GET'], cors=True)
def fetch_all_devices():
    """
    Fetch all IoT devices, rounding LastDowntime to the previous 30-minute interval.
    """
    query = """
    SELECT id, IoTType, IoTStatus, IoTSerialNumber, PlotID, 
           DATE_SUB(LastDowntime, INTERVAL MOD(MINUTE(LastDowntime), 30) MINUTE) AS RoundedLastDowntime
    FROM IoTDevicesTest
    """

    try:
        connection = create_connection()
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()

        return Response(
            body=json.dumps(result, default=json_serial),
            status_code=200,
            headers={'Content-Type': 'application/json'}
        )
    except Exception as e:
        return Response(
            body=json.dumps({"error": str(e)}),
            status_code=500,
            headers={'Content-Type': 'application/json'}
        )
    finally:
        if connection:
            connection.close()

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
        INSERT INTO IoTDevicesTest (IoTType, IoTSerialNumber, IoTStatus, PlotID)
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
        DELETE FROM IoTDevicesTest
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


# AWS IoT Client Configuration
iot_client = boto3.client(
    'iot-data',
    endpoint_url=f"https://{os.getenv('IOT_ENDPOINT')}",
    config=Config(signature_version='v4')
)

# UTC Offset for Singapore Time
SGT_OFFSET = datetime.timedelta(hours=8)

# Constants for Downtime Management
DOWNTIME_PROBABILITY = 3  # 10% chance for a device to go inactive
DOWNTIME_COOLDOWN_DAYS = 40

def get_latest_30min_timestamp():
    """Get the latest rounded 30-minute timestamp."""
    now = datetime.datetime.utcnow() + SGT_OFFSET
    rounded_minute = (now.minute // 30) * 30
    return now.replace(minute=rounded_minute, second=0, microsecond=0)

@app.schedule(Rate(30, unit=Rate.MINUTES))
def scheduled_iot_status_update(event):
    """Scheduled event to update IoT device statuses based on probability and cooldown."""
    print("Running scheduled IoT status update...")
    connection = create_connection()
    latest_time = get_latest_30min_timestamp()

    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT id, IoTType, IoTStatus, IoTSerialNumber, PlotID, LastDowntime
                FROM IoTDevicesTest
            """
            )
            devices = cursor.fetchall()

            logs = []
            updates = []

            for device in devices:
                device_id = device["id"]
                serial = device["IoTSerialNumber"]
                current_status = device["IoTStatus"]
                last_downtime = device["LastDowntime"]

                # Default: Keep the same status
                final_status = current_status

                # Check if we should set device to inactive
                if current_status == 1:
                    can_go_inactive = False
                    if last_downtime is None:
                        can_go_inactive = True
                    else:
                        diff_days = (latest_time.date() - last_downtime.date()).days
                        if diff_days >= DOWNTIME_COOLDOWN_DAYS:
                            can_go_inactive = True

                    if can_go_inactive:
                        hash_key = f"{serial}-{latest_time.strftime('%Y-%m-%d %H')}"
                        hash_val = int(hashlib.sha256(hash_key.encode()).hexdigest(), 16)
                        if (hash_val % 100) < DOWNTIME_PROBABILITY:
                            final_status = 0
                            last_downtime = latest_time

                logs.append((
                    device["IoTType"], serial, final_status, latest_time, device["PlotID"], "system"
                ))
                updates.append((final_status, last_downtime, latest_time, serial))

            # Insert logs into IoTDeviceLogTest
            if logs:
                cursor.executemany("""
                    INSERT INTO IoTDeviceLogTest (IoTType, IoTSerialNumber, IoTStatus, Timestamp, PlotID, ChangedBy)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, logs)

            # Update IoTDevicesTest
            cursor.executemany("""
                UPDATE IoTDevicesTest
                SET IoTStatus = %s, LastDowntime = %s, LastUpdated = %s
                WHERE IoTSerialNumber = %s
            """, updates)

            connection.commit()

        return {"message": "IoT statuses updated successfully."}
    except Exception as e:
        print(f"Error: {str(e)}")
        return {"error": str(e)}
    finally:
        connection.close()



