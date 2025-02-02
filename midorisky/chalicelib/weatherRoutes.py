from chalice import Blueprint, Response, BadRequestError
import boto3
import pandas as pd
import json
from .connectHelper import create_connection

weather_routes = Blueprint(__name__)

s3 = boto3.client('s3')

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

        # Convert the result to a DataFrame
        df = pd.DataFrame(result, columns=['timestamp', 'temperature', 'humidity', 'precipitation', 'windspeed'])

        # Process the data: format daily data
        df['timestamp'] = pd.to_datetime(df['timestamp'])  # Ensure timestamps are in datetime format
        df['date'] = df['timestamp'].dt.date  # Extract the date

        daily_data = df.groupby('date').agg({
            'temperature': 'mean',
            'humidity': 'mean',
            'precipitation': 'sum',  # Total precipitation per day
            'windspeed': 'mean'
        }).reset_index()

        # Rename columns to match React's expected format
        daily_data = daily_data.rename(columns={
            'date': 'Date',
            'temperature': 'Avg_Temperature',
            'humidity': 'Avg_Humidity',
            'precipitation': 'Avg_Precipitation',
            'windspeed': 'Avg_Windspeed'
        })

        # Serialize the date to a string format
        daily_data['Date'] = daily_data['Date'].astype(str)

        # Convert DataFrame to JSON format
        weather_data_json = daily_data.to_dict(orient='records')

        return Response(
            body=json.dumps(weather_data_json),
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

        # Convert the result to a DataFrame
        df = pd.DataFrame(result, columns=['Date', 'Temperature', 'Humidity', 'Precipitation', 'Windspeed'])

        # Process the data: ensure Date is in the correct format
        df['Date'] = pd.to_datetime(df['Date']).dt.date  # Convert Date to a proper date object if needed

        # Aggregate data (in this case, it is already daily, but this ensures consistency)
        daily_data = df.groupby('Date').agg({
            'Temperature': 'mean',      # Average temperature
            'Humidity': 'mean',         # Average humidity
            'Precipitation': 'sum',     # Total precipitation per day
            'Windspeed': 'mean'         # Average windspeed
        }).reset_index()

        # Rename columns to match React's expected format
        daily_data = daily_data.rename(columns={
            'Date': 'Date',
            'Temperature': 'Avg_Temperature',
            'Humidity': 'Avg_Humidity',
            'Precipitation': 'Avg_Precipitation',
            'Windspeed': 'Avg_Windspeed'
        })

        # Convert dates to string format for JSON serialization
        daily_data['Date'] = daily_data['Date'].astype(str)

        # Convert DataFrame to JSON format
        weather_data_json = daily_data.to_dict(orient='records')

        return Response(
            body=json.dumps(weather_data_json),
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

        # Convert WeatherSensor data to a DataFrame
        sensor_df = pd.DataFrame(sensor_result, columns=['timestamp', 'temperature', 'humidity', 'precipitation', 'windspeed'])
        sensor_df['timestamp'] = pd.to_datetime(sensor_df['timestamp'])  # Ensure timestamp is in datetime format
        sensor_df['Date'] = sensor_df['timestamp'].dt.date  # Extract the date

        daily_sensor_data = sensor_df.groupby('Date').agg({
            'temperature': 'mean',
            'humidity': 'mean',
            'precipitation': 'sum',  # Total precipitation per day
            'windspeed': 'mean'
        }).reset_index()

        # Rename columns to match React's expected format
        daily_sensor_data = daily_sensor_data.rename(columns={
            'temperature': 'Avg_Temperature',
            'humidity': 'Avg_Humidity',
            'precipitation': 'Avg_Precipitation',
            'windspeed': 'Avg_Windspeed'
        })

        # Fetch data from WeatherPrediction
        with create_connection().cursor() as cursor:
            cursor.execute(prediction_query)
            prediction_result = cursor.fetchall()

        if not prediction_result:
            raise BadRequestError("No data found for the WeatherPrediction table.")

        # Convert WeatherPrediction data to a DataFrame
        prediction_df = pd.DataFrame(prediction_result, columns=['Date', 'Temperature', 'Humidity', 'Precipitation', 'Windspeed'])
        prediction_df['Date'] = pd.to_datetime(prediction_df['Date']).dt.date  # Convert Date to proper date object

        daily_prediction_data = prediction_df.groupby('Date').agg({
            'Temperature': 'mean',
            'Humidity': 'mean',
            'Precipitation': 'sum',
            'Windspeed': 'mean'
        }).reset_index()

        # Rename columns to match React's expected format
        daily_prediction_data = daily_prediction_data.rename(columns={
            'Temperature': 'Avg_Temperature',
            'Humidity': 'Avg_Humidity',
            'Precipitation': 'Avg_Precipitation',
            'Windspeed': 'Avg_Windspeed'
        })

        # Add a column to distinguish between actual and predicted data
        daily_sensor_data['Type'] = 'Actual'
        daily_prediction_data['Type'] = 'Predicted'

        # Combine both datasets
        combined_data = pd.concat([daily_sensor_data, daily_prediction_data], ignore_index=True)
        combined_data['Date'] = combined_data['Date'].astype(str)  # Serialize dates to string

        # Convert DataFrame to JSON format
        combined_data_json = combined_data.to_dict(orient='records')

        return Response(
            body=json.dumps(combined_data_json),
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

        # Process the WeatherSensor data into a DataFrame
        sensor_df = pd.DataFrame([sensor_result])
        sensor_df['timestamp'] = pd.to_datetime(sensor_df['timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')

        # Rename columns to match React's expected format
        sensor_df = sensor_df.rename(columns={
            'timestamp': 'Timestamp',
            'temperature': 'Temperature',
            'humidity': 'Humidity',
            'precipitation': 'Precipitation',
            'windspeed': 'Windspeed'
        })

        # Convert DataFrame to JSON format
        sensor_data_json = sensor_df.to_dict(orient='records')

        return Response(
            body=json.dumps(sensor_data_json),
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

        # Convert current day data to a DataFrame
        current_day_df = pd.DataFrame(current_day_result)
        next_days_df = pd.DataFrame(next_days_result)

        # Format timestamps
        current_day_df['timestamp'] = pd.to_datetime(current_day_df['timestamp']).dt.strftime('%Y-%m-%d')
        next_days_df['timestamp'] = pd.to_datetime(next_days_df['timestamp']).dt.strftime('%Y-%m-%d')

        # Rename columns to match React's expected format
        current_day_df = current_day_df.rename(columns={
            'timestamp': 'Timestamp',
            'temperature': 'Temperature',
            'humidity': 'Humidity',
            'precipitation': 'Precipitation',
            'windspeed': 'Windspeed'
        })

        next_days_df = next_days_df.rename(columns={
            'timestamp': 'Timestamp',
            'temperature': 'Temperature',
            'humidity': 'Humidity',
            'precipitation': 'Precipitation',
            'windspeed': 'Windspeed'
        })

        # Combine both dataframes
        combined_df = pd.concat([current_day_df, next_days_df], ignore_index=True)

        # Convert DataFrame to JSON format
        combined_data_json = combined_df.to_dict(orient='records')

        return Response(
            body=json.dumps(combined_data_json),
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

        # Convert current hours' data to a DataFrame
        current_hours_df = pd.DataFrame(current_hours_result, columns=[
            'Timestamp', 'Temperature', 'Humidity', 'Precipitation', 'Windspeed'
        ])

        # Convert next hours' data to a DataFrame
        next_hours_df = pd.DataFrame(next_hours_result, columns=[
            'Timestamp', 'Temperature', 'Humidity', 'Precipitation', 'Windspeed'
        ])

        # Format timestamps
        current_hours_df['Timestamp'] = pd.to_datetime(current_hours_df['Timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
        next_hours_df['Timestamp'] = pd.to_datetime(next_hours_df['Timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')

        # Combine both DataFrames
        combined_df = pd.concat([current_hours_df, next_hours_df], ignore_index=True)

        # Convert DataFrame to JSON format
        combined_data_json = combined_df.to_dict(orient='records')

        return Response(
            body=json.dumps(combined_data_json),
            status_code=200,
            headers={'Content-Type': 'application/json'}
        )

    except Exception as e:
        return Response(
            body=json.dumps({"error": str(e)}),
            status_code=500,
            headers={'Content-Type': 'application/json'}
        )


