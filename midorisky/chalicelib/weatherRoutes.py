from chalice import Blueprint, Response, BadRequestError
import boto3
import json
from datetime import datetime
from .connectHelper import create_connection
import traceback
from .helpers import json_serial

weather_routes = Blueprint(__name__)

s3 = boto3.client('s3')

# def format_daily_data(result, columns):
#     """
#     Format daily data from the database result.
#     """
#     data = []
#     for row in result:
#         row_dict = dict(zip(columns, row))
#         # convert string to datetime object (2019-01-01 00:00:00.000000)
#         #print(row['timestamp'])
#         row_dict['timestamp'] = row['timestamp']
#         data.append(row_dict)
#     return data

@weather_routes.route('/staff/weather/fetch-weather-data', methods=['GET'], cors=True)
def fetch_weather_data():
    """
    Fetch daily weather data and return JSON for frontend consumption.
    """
    query = """
    SELECT timestamp, temperature, humidity, precipitation, windspeed 
    FROM WeatherSensor 
    WHERE timestamp BETWEEN '2019-01-01' AND CURDATE();
    """
    try:
        # Fetch data from the database
        with create_connection().cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()

        # Check if data exists
        if not result:
            raise BadRequestError("No data found for the specified period.")

        # Format the data
        columns = ['timestamp', 'temperature', 'humidity', 'precipitation', 'windspeed']
        weather_data = result

        return Response(
            body=json.dumps(weather_data, default=json_serial),
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

@weather_routes.route('/staff/weather/fetch-predicted-weather-data', methods=['GET'], cors=True)
def fetch_predicted_weather_data():
    """
    Fetch daily weather prediction data and return JSON for frontend consumption.
    """
    query = """
    SELECT Date, Temperature, Humidity, Precipitation, Windspeed 
    FROM WeatherPrediction 
    WHERE Date > CURDATE();
    """
    try:
        # Fetch data from the database
        with create_connection().cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()

        # Check if data exists
        if not result:
            raise BadRequestError("No data found for the specified period.")

        # Format the data
        columns = ['Date', 'Temperature', 'Humidity', 'Precipitation', 'Windspeed']
        weather_data = result

        return Response(
            body=json.dumps(weather_data, default=json_serial),
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

@weather_routes.route('/staff/weather/fetch-combined-weather-data', methods=['GET'], cors=True)
def fetch_combined_weather_data():
    """
    Fetch combined daily weather data (actual and predicted) and return JSON for frontend consumption.
    """
    sensor_query = """
    SELECT timestamp, temperature, humidity, precipitation, windspeed 
    FROM WeatherSensor 
    WHERE timestamp BETWEEN '2019-01-01' AND CURDATE();
    """
    prediction_query = """
    SELECT Date, Temperature, Humidity, Precipitation, Windspeed 
    FROM WeatherPrediction 
    WHERE Date > CURDATE();
    """
    try:
        # Fetch data from WeatherSensor
        with create_connection().cursor() as cursor:
            cursor.execute(sensor_query)
            sensor_result = cursor.fetchall()

        if not sensor_result:
            raise BadRequestError("No data found for the WeatherSensor table.")

        # Fetch data from WeatherPrediction
        with create_connection().cursor() as cursor:
            cursor.execute(prediction_query)
            prediction_result = cursor.fetchall()

        if not prediction_result:
            raise BadRequestError("No data found for the WeatherPrediction table.")

        # Format the data
        sensor_columns = ['timestamp', 'temperature', 'humidity', 'precipitation', 'windspeed']
        prediction_columns = ['Date', 'Temperature', 'Humidity', 'Precipitation', 'Windspeed']

        sensor_data = sensor_result
        prediction_data = prediction_result

        # Add a type field to distinguish between actual and predicted data
        for data in sensor_data:
            data['Type'] = 'Actual'
        for data in prediction_data:
            data['Type'] = 'Predicted'

        # Combine both datasets
        combined_data = sensor_data + prediction_data

        return Response(
            body=json.dumps(combined_data, default=json_serial),
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
        # Print error stack trace
        print(traceback.format_exc())
        return Response(
            body=json.dumps({"error": str(e)}),
            status_code=500,
            headers={'Content-Type': 'application/json'}
        )

@weather_routes.route('/staff/weather/fetch-closest-weather-data', methods=['GET'], cors=True)
def fetch_closest_weather_data():
    """
    Fetch the closest weather data (to the current time) from WeatherSensor and return JSON.
    """
    sensor_query = """
    SELECT CONVERT_TZ(timestamp, '+00:00', '+08:00') AS timestamp, temperature, humidity, precipitation, windspeed 
    FROM WeatherSensor 
    WHERE timestamp BETWEEN DATE_SUB(NOW(), INTERVAL 1 DAY) AND NOW()
    ORDER BY ABS(TIMESTAMPDIFF(SECOND, timestamp, NOW())) ASC
    LIMIT 1;
    """
    try:
        # Fetch closest data from WeatherSensor
        with create_connection().cursor() as cursor:
            cursor.execute(sensor_query)
            sensor_result = cursor.fetchone()

        if not sensor_result:
            raise Exception("No recent data found in the WeatherSensor table.")

        # Format the data
        columns = ['timestamp', 'temperature', 'humidity', 'precipitation', 'windspeed']
        sensor_data = dict(zip(columns, sensor_result))
        sensor_data['timestamp'] = sensor_data['timestamp'].strftime('%Y-%m-%d %H:%M:%S')

        return Response(
            body=json.dumps([sensor_data], default=json_serial),
            status_code=200,
            headers={'Content-Type': 'application/json'}
        )

    except Exception as e:
        return Response(
            body=json.dumps({"error": str(e)}),
            status_code=500,
            headers={'Content-Type': 'application/json'}
        )

@weather_routes.route('/staff/weather/fetch-current-and-next-days', methods=['GET'], cors=True)
def fetch_current_and_next_days_weather():
    """
    Fetch the average weather data for the current day and the next 3 days (predicted).
    """
    # Query for the current day's weather data
    current_day_query = """
    SELECT CONVERT_TZ(timestamp, '+00:00', '+08:00') AS timestamp, 
           AVG(temperature) AS temperature, 
           AVG(humidity) AS humidity, 
           SUM(precipitation) AS precipitation, 
           AVG(windspeed) AS windspeed 
    FROM WeatherSensor 
    WHERE DATE(CONVERT_TZ(timestamp, '+00:00', '+08:00')) = DATE(CONVERT_TZ(NOW(), '+00:00', '+08:00'))
    GROUP BY DATE(CONVERT_TZ(timestamp, '+00:00', '+08:00'));
    """

    # Query for the next 3 days' predicted weather data
    next_days_query = """
    SELECT Date AS timestamp, 
           AVG(Temperature) AS temperature, 
           AVG(Humidity) AS humidity, 
           SUM(Precipitation) AS precipitation, 
           AVG(Windspeed) AS windspeed 
    FROM WeatherPrediction 
    WHERE Date > DATE(CONVERT_TZ(NOW(), '+00:00', '+08:00')) 
    GROUP BY Date 
    ORDER BY Date ASC 
    LIMIT 3;
    """

    try:
        # Fetch current day's data from WeatherSensor
        with create_connection().cursor() as cursor:
            cursor.execute(current_day_query)
            current_day_result = cursor.fetchall()

        if not current_day_result:
            raise Exception("No data found for the current day in the WeatherSensor table.")

        # Fetch next 3 days' predicted data from WeatherPrediction
        with create_connection().cursor() as cursor:
            cursor.execute(next_days_query)
            next_days_result = cursor.fetchall()

        if not next_days_result:
            raise Exception("No data found for the next 3 days in the WeatherPrediction table.")

        # Format the data
        current_day_data = current_day_result
        next_days_data = next_days_result

        # Combine both datasets
        combined_data = current_day_data + next_days_data

        return Response(
            body=json.dumps(combined_data, default=json_serial),
            status_code=200,
            headers={'Content-Type': 'application/json'}
        )

    except Exception as e:
        return Response(
            body=json.dumps({"error": str(e)}),
            status_code=500,
            headers={'Content-Type': 'application/json'}
        )

@weather_routes.route('/staff/weather/fetch-current-and-next-hours', methods=['GET'], cors=True)
def fetch_current_and_next_hours_weather():
    """
    Fetch the weather data for the current 2 hours (from WeatherSensor) and the next 2 hours (from WeatherPrediction24).
    """
    # Query for the current 2 hours' weather data from WeatherSensor
    current_hours_query = """
    SELECT CONVERT_TZ(Timestamp, '+00:00', '+08:00') AS Timestamp, 
           AVG(Temperature) AS Temperature, 
           AVG(Humidity) AS Humidity, 
           SUM(Precipitation) AS Precipitation, 
           AVG(Windspeed) AS Windspeed 
    FROM WeatherSensor 
    WHERE Timestamp >= DATE_SUB(NOW(), INTERVAL 2 HOUR) 
      AND Timestamp <= NOW()
    GROUP BY FLOOR(UNIX_TIMESTAMP(Timestamp) / 1800)  -- Group by 30-minute intervals
    ORDER BY Timestamp ASC;
    """

    # Query for the next 2 hours' predicted weather data from WeatherPrediction24
    next_hours_query = """
    SELECT CONVERT_TZ(DateTime, '+00:00', '+08:00') AS Timestamp, 
           AVG(Temperature) AS Temperature, 
           AVG(Humidity) AS Humidity, 
           SUM(Precipitation) AS Precipitation, 
           AVG(Windspeed) AS Windspeed 
    FROM WeatherPrediction24 
    WHERE DateTime > NOW()
      AND DateTime <= DATE_ADD(NOW(), INTERVAL 2 HOUR)
    GROUP BY FLOOR(UNIX_TIMESTAMP(DateTime) / 1800)  -- Group by 30-minute intervals
    ORDER BY DateTime ASC;
    """

    try:
        # Fetch current 2 hours' data from WeatherSensor
        with create_connection().cursor() as cursor:
            cursor.execute(current_hours_query)
            current_hours_result = cursor.fetchall()

        if not current_hours_result:
            raise Exception("No data found for the current 2 hours in the WeatherSensor table.")

        # Fetch next 2 hours' predicted data from WeatherPrediction24
        with create_connection().cursor() as cursor:
            cursor.execute(next_hours_query)
            next_hours_result = cursor.fetchall()

        if not next_hours_result:
            raise Exception("No data found for the next 2 hours in the WeatherPrediction24 table.")

        # Format the data
        current_hours_data = current_hours_result
        next_hours_data = next_hours_result

        # Combine both datasets
        combined_data = current_hours_data + next_hours_data

        return Response(
            body=json.dumps(combined_data, default=json_serial),
            status_code=200,
            headers={'Content-Type': 'application/json'}
        )

    except Exception as e:
        return Response(
            body=json.dumps({"error": str(e)}),
            status_code=500,
            headers={'Content-Type': 'application/json'}
        )