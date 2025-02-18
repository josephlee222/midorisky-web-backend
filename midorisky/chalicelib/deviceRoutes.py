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

@device_routes.route('/staff/devices/view/{device_id}', methods=['GET'], cors=True)
def fetch_device(device_id):
    """
    Fetch a single IoT device by ID.
    """
    query = """SELECT id, IoTType, IoTStatus, IoTSerialNumber, PlotID, LastDowntime 
               FROM IoTDevicesTest WHERE id = %s"""
    try:
        connection = create_connection()
        with connection.cursor() as cursor:
            cursor.execute(query, (device_id,))
            result = cursor.fetchone()

        if not result:
            return Response(
                body=json.dumps({"error": "Device not found"}),
                status_code=404,
                headers={'Content-Type': 'application/json'}
            )

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

@device_routes.route('/staff/devices/create', methods=['POST'], cors=True)
def create_device():
    """
    Create a new IoT device.
    """
    try:
        request = device_routes.current_request.json_body
        IoTType = request.get("IoTType")
        IoTStatus = request.get("IoTStatus")
        IoTSerialNumber = request.get("IoTSerialNumber")
        PlotID = request.get("PlotID")

        if IoTType is None or IoTStatus is None or IoTSerialNumber is None or PlotID is None:
            raise BadRequestError("Missing required fields")

        query = """INSERT INTO IoTDevicesTest (IoTType, IoTStatus, IoTSerialNumber, PlotID, LastDowntime) 
                   VALUES (%s, %s, %s, %s, NOW())"""
        log_query = """INSERT INTO IoTDeviceLogTest (IoTType, IoTStatus, IoTSerialNumber, PlotID, Timestamp, ChangedBy) 
                        VALUES (%s, %s, %s, %s, NOW(), 'admin')"""

        connection = create_connection()
        with connection.cursor() as cursor:
            cursor.execute(query, (IoTType, IoTStatus, IoTSerialNumber, PlotID))
            cursor.execute(log_query, (IoTType, IoTStatus, IoTSerialNumber, PlotID))
            connection.commit()

        return Response(
            body=json.dumps({"message": "Device created successfully"}),
            status_code=201,
            headers={'Content-Type': 'application/json'}
        )
    except Exception as e:
        return Response(
            body=json.dumps({"error": str(e)}),
            status_code=500,
            headers={'Content-Type': 'application/json'}
        )

@device_routes.route('/staff/devices/edit/{device_id}', methods=['PUT'], cors=True)
def edit_device(device_id):
    """
    Edit an existing IoT device.
    """
    try:
        request = device_routes.current_request.json_body
        print(device_routes.current_request.json_body)
        print(device_routes.current_request)
        IoTType = request.get("IoTType")
        IoTStatus = request.get("IoTStatus")
        IoTSerialNumber = request.get("IoTSerialNumber")
        PlotID = request.get("PlotID")
        print(IoTType, IoTStatus, IoTSerialNumber, PlotID)
        if IoTType is None or IoTStatus is None or IoTSerialNumber is None or PlotID is None:
            raise BadRequestError("Missing required fields")

        query = """UPDATE IoTDevicesTest SET IoTType=%s, IoTStatus=%s, IoTSerialNumber=%s, PlotID=%s 
                   WHERE id=%s"""
        log_query = """INSERT INTO IoTDeviceLogTest (IoTType, IoTStatus, IoTSerialNumber, PlotID, Timestamp, ChangedBy) 
                        VALUES (%s, %s, %s, %s, NOW(), 'admin')"""

        connection = create_connection()
        with connection.cursor() as cursor:
            cursor.execute(query, (IoTType, IoTStatus, IoTSerialNumber, PlotID, device_id))
            cursor.execute(log_query, (IoTType, IoTStatus, IoTSerialNumber, PlotID))
            connection.commit()

        return Response(
            body=json.dumps({"message": "Device updated successfully"}),
            status_code=200,
            headers={'Content-Type': 'application/json'}
        )
    except Exception as e:
        return Response(
            body=json.dumps({"error": str(e)}),
            status_code=500,
            headers={'Content-Type': 'application/json'}
        )


@device_routes.route('/staff/devices/delete/{device_id}', methods=['DELETE'], cors=True)
def delete_device(device_id):
    """
    Delete an IoT device by ID.
    """
    try:
        log_query = """INSERT INTO IoTDeviceLogTest (IoTType, IoTStatus, IoTSerialNumber, PlotID, Timestamp, ChangedBy) 
                        SELECT IoTType, IoTStatus, IoTSerialNumber, PlotID, NOW(), 'admin' FROM IoTDevicesTest WHERE id = %s"""
        delete_query = "DELETE FROM IoTDevicesTest WHERE id = %s"

        connection = create_connection()
        with connection.cursor() as cursor:
            cursor.execute(log_query, (device_id,))
            cursor.execute(delete_query, (device_id,))
            connection.commit()

        return Response(
            body=json.dumps({"message": "Device deleted successfully"}),
            status_code=200,
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
DOWNTIME_PROBABILITY = 0.2  # 10% chance for a device to go inactive
DOWNTIME_COOLDOWN_DAYS = 40

def get_latest_30min_timestamp():
    """Get the latest rounded 30-minute timestamp."""
    now = datetime.datetime.utcnow() + SGT_OFFSET
    rounded_minute = (now.minute // 30) * 30
    return now.replace(minute=rounded_minute, second=0, microsecond=0)

@device_routes.schedule(Rate(30, unit=Rate.MINUTES))
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



