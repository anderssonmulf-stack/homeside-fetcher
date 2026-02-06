#!/usr/bin/env python3
"""Compare predicted energy with actual measured energy - Fixed date alignment."""

import os
import sys
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

from influxdb_client import InfluxDBClient

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from customer_profile import CustomerProfile
from energy_models.weather_energy_model import WeatherConditions, SimpleWeatherModel

SWEDISH_TZ = ZoneInfo('Europe/Stockholm')


def main():
    house_id = 'HEM_FJV_Villa_149'
    
    influx_url = os.environ.get('INFLUXDB_URL', 'http://localhost:8086')
    influx_token = os.environ.get('INFLUXDB_TOKEN', '')
    influx_org = os.environ.get('INFLUXDB_ORG', 'homeside')
    influx_bucket = os.environ.get('INFLUXDB_BUCKET', 'heating')
    latitude = float(os.environ.get('LATITUDE', '58.41'))
    longitude = float(os.environ.get('LONGITUDE', '15.62'))
    
    client = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)
    query_api = client.query_api()
    
    # Load profile
    profile = CustomerProfile.load(house_id, 'profiles')
    heat_loss_k = profile.energy_separation.heat_loss_k or 0.06
    target_indoor = profile.comfort.target_indoor_temp
    solar_coeff = profile.learned.weather_coefficients.solar_coefficient_ml2
    wind_coeff = profile.learned.weather_coefficients.wind_coefficient_ml2
    
    weather_model = SimpleWeatherModel(
        solar_coefficient=solar_coeff,
        wind_coefficient=wind_coeff
    )
    
    print(f"Model: k={heat_loss_k:.4f} kW/°C, solar={solar_coeff}, wind={wind_coeff}")
    print()
    
    # Get actual daily energy - timestamp is at 23:00 UTC = represents that UTC day
    actual_query = f'''
        from(bucket: "{influx_bucket}")
        |> range(start: -15d)
        |> filter(fn: (r) => r._measurement == "energy_separated")
        |> filter(fn: (r) => r.house_id == "{house_id}")
        |> filter(fn: (r) => r._field == "heating_energy_kwh")
        |> sort(columns: ["_time"])
    '''
    
    actual_tables = query_api.query(actual_query, org=influx_org)
    
    actual_by_date = {}
    for table in actual_tables:
        for record in table.records:
            ts = record.get_time()
            if ts:
                # 23:00 UTC represents that UTC date (not Swedish date)
                day = ts.strftime('%Y-%m-%d')  # Use UTC date directly
                actual_by_date[day] = record.get_value()
    
    # Get hourly weather data for predictions
    weather_query = f'''
        from(bucket: "{influx_bucket}")
        |> range(start: -15d)
        |> filter(fn: (r) => r._measurement == "heating_system")
        |> filter(fn: (r) => r.house_id == "{house_id}")
        |> filter(fn: (r) => r._field == "outdoor_temperature")
        |> aggregateWindow(every: 1h, fn: mean)
    '''
    
    wind_query = f'''
        from(bucket: "{influx_bucket}")
        |> range(start: -15d)
        |> filter(fn: (r) => r._measurement == "weather_observation")
        |> filter(fn: (r) => r.house_id == "{house_id}")
        |> filter(fn: (r) => r._field == "wind_speed")
        |> aggregateWindow(every: 1h, fn: mean)
    '''
    
    weather_tables = query_api.query(weather_query, org=influx_org)
    wind_tables = query_api.query(wind_query, org=influx_org)
    
    # Build wind lookup
    wind_by_hour = {}
    for table in wind_tables:
        for record in table.records:
            ts = record.get_time()
            if ts:
                wind_by_hour[ts] = record.get_value() or 3.0
    
    # Calculate predictions by day (use UTC dates for alignment)
    predicted_by_date = {}
    outdoor_by_date = {}
    hours_by_date = {}
    
    for table in weather_tables:
        for record in table.records:
            ts = record.get_time()
            outdoor = record.get_value()
            if not ts or outdoor is None:
                continue
            
            day = ts.strftime('%Y-%m-%d')  # UTC date
            wind_speed = wind_by_hour.get(ts, 3.0)
            
            # Calculate effective temp and predicted power
            conditions = WeatherConditions(
                temperature=outdoor,
                wind_speed=wind_speed,
                humidity=70,
                cloud_cover=4,  # Assumed - this is a key source of error!
                latitude=latitude,
                longitude=longitude,
                timestamp=ts
            )
            eff_result = weather_model.effective_temperature(conditions)
            effective_temp = eff_result.effective_temp
            
            temp_diff = target_indoor - effective_temp
            power = heat_loss_k * temp_diff if temp_diff > 0 else 0
            
            if day not in predicted_by_date:
                predicted_by_date[day] = 0
                outdoor_by_date[day] = []
                hours_by_date[day] = 0
            
            predicted_by_date[day] += power
            outdoor_by_date[day].append(outdoor)
            hours_by_date[day] += 1
    
    # Print comparison
    print(f"{'Date':<12} {'Actual':>10} {'Predicted':>10} {'Error':>10} {'Error%':>8} {'Avg Out':>8} {'Hours':>6}")
    print("-" * 76)
    
    total_actual = 0
    total_predicted = 0
    errors = []
    
    for day in sorted(actual_by_date.keys()):
        actual = actual_by_date[day]
        predicted = predicted_by_date.get(day, 0)
        avg_out = sum(outdoor_by_date.get(day, [0])) / max(len(outdoor_by_date.get(day, [1])), 1)
        hours = hours_by_date.get(day, 0)
        
        error = predicted - actual
        error_pct = (error / actual * 100) if actual > 0 else 0
        
        print(f"{day:<12} {actual:>9.1f} {predicted:>9.1f} {error:>+9.1f} {error_pct:>+7.1f}% {avg_out:>7.1f}°C {hours:>6}")
        
        total_actual += actual
        total_predicted += predicted
        errors.append(error_pct)
    
    print("-" * 76)
    total_error = total_predicted - total_actual
    total_error_pct = (total_error / total_actual * 100) if total_actual > 0 else 0
    
    print(f"{'TOTAL':<12} {total_actual:>9.1f} {total_predicted:>9.1f} {total_error:>+9.1f} {total_error_pct:>+7.1f}%")
    print(f"\nMean Error: {sum(errors)/len(errors):+.1f}%")
    print(f"Mean Absolute Error: {sum(abs(e) for e in errors)/len(errors):.1f}%")
    
    # Analysis
    print("\n" + "="*76)
    print("ANALYSIS")
    print("="*76)
    if total_error < 0:
        factor = total_actual / total_predicted
        print(f"\nModel UNDER-predicts by {-total_error_pct:.1f}%")
        print(f"To match reality, k should be: {heat_loss_k * factor:.4f} kW/°C (was {heat_loss_k:.4f})")
    else:
        factor = total_predicted / total_actual
        print(f"\nModel OVER-predicts by {total_error_pct:.1f}%")
        print(f"To match reality, k should be: {heat_loss_k / factor:.4f} kW/°C (was {heat_loss_k:.4f})")
    
    client.close()


if __name__ == '__main__':
    main()
