#!/usr/bin/env python3
"""
Backtest Energy Forecaster - Simplified version

Uses heating_system data to validate energy predictions.
Calculates "actual" energy from measured temperatures and compares
with what the forecaster would have predicted.
"""

import os
import sys
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

from influxdb_client import InfluxDBClient

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from customer_profile import CustomerProfile
from energy_forecaster import EnergyForecaster
from energy_models.weather_energy_model import WeatherConditions, SimpleWeatherModel

SWEDISH_TZ = ZoneInfo('Europe/Stockholm')


def main():
    # Config
    house_id = 'HEM_FJV_Villa_149'
    start_date = datetime(2026, 2, 1, tzinfo=SWEDISH_TZ)
    end_date = datetime(2026, 2, 5, tzinfo=SWEDISH_TZ)
    
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
    
    # Get ML2 coefficients
    solar_coeff = profile.learned.weather_coefficients.solar_coefficient_ml2
    wind_coeff = profile.learned.weather_coefficients.wind_coefficient_ml2
    
    print(f"House: {profile.friendly_name}")
    print(f"Heat loss k: {heat_loss_k:.4f} kW/°C")
    print(f"Target indoor: {target_indoor}°C")
    print(f"Solar coefficient ML2: {solar_coeff}")
    print(f"Wind coefficient ML2: {wind_coeff}")
    
    # Create weather model with learned coefficients
    weather_model = SimpleWeatherModel(
        solar_coefficient=solar_coeff,
        wind_coefficient=wind_coeff
    )
    
    # Query hourly data
    query = f'''
        from(bucket: "{influx_bucket}")
        |> range(start: {start_date.isoformat()}, stop: {end_date.isoformat()})
        |> filter(fn: (r) => r._measurement == "heating_system")
        |> filter(fn: (r) => r.house_id == "{house_id}")
        |> filter(fn: (r) => 
            r._field == "outdoor_temperature" or 
            r._field == "room_temperature" or
            r._field == "supply_temp" or
            r._field == "return_temp"
        )
        |> aggregateWindow(every: 1h, fn: mean)
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> sort(columns: ["_time"])
    '''
    
    # Query weather for wind
    weather_query = f'''
        from(bucket: "{influx_bucket}")
        |> range(start: {start_date.isoformat()}, stop: {end_date.isoformat()})
        |> filter(fn: (r) => r._measurement == "weather_observation")
        |> filter(fn: (r) => r.house_id == "{house_id}")
        |> filter(fn: (r) => r._field == "wind_speed")
        |> aggregateWindow(every: 1h, fn: mean)
    '''
    
    tables = query_api.query(query, org=influx_org)
    weather_tables = query_api.query(weather_query, org=influx_org)
    
    # Build wind speed lookup
    wind_by_hour = {}
    for table in weather_tables:
        for record in table.records:
            ts = record.get_time()
            if ts:
                hour_key = ts.replace(minute=0, second=0, microsecond=0)
                wind_by_hour[hour_key] = record.get_value() or 3.0
    
    # Process data by day
    daily_results = {}
    
    for table in tables:
        for record in table.records:
            ts = record.get_time()
            if not ts:
                continue
            
            outdoor_temp = record.values.get('outdoor_temperature')
            room_temp = record.values.get('room_temperature')
            supply_temp = record.values.get('supply_temp')
            return_temp = record.values.get('return_temp')
            
            if outdoor_temp is None or room_temp is None:
                continue
            
            # Get wind speed
            hour_key = ts.replace(minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
            wind_speed = wind_by_hour.get(hour_key, 3.0)
            
            # Calculate effective outdoor temperature
            conditions = WeatherConditions(
                temperature=outdoor_temp,
                wind_speed=wind_speed,
                humidity=70,  # Assumed
                cloud_cover=4,  # Assumed (partly cloudy)
                latitude=latitude,
                longitude=longitude,
                timestamp=ts
            )
            eff_result = weather_model.effective_temperature(conditions)
            effective_temp = eff_result.effective_temp
            
            # Calculate predicted heating energy (for 1 hour)
            temp_diff = target_indoor - effective_temp
            predicted_power = heat_loss_k * temp_diff if temp_diff > 0 else 0
            predicted_energy = predicted_power  # kWh for 1 hour
            
            # Estimate "actual" heating from supply-return delta
            # When delta is high, heating is active
            actual_power = 0
            if supply_temp and return_temp:
                delta = supply_temp - return_temp
                if delta > 0.5:  # Heating active
                    # Rough estimate: assume flow gives similar power
                    actual_power = heat_loss_k * (target_indoor - outdoor_temp)
            
            # Group by day
            day = ts.astimezone(SWEDISH_TZ).strftime('%Y-%m-%d')
            if day not in daily_results:
                daily_results[day] = {
                    'predicted_kwh': 0,
                    'actual_kwh': 0,
                    'hours': 0,
                    'outdoor_temps': [],
                    'effective_temps': [],
                    'solar_effects': [],
                    'hourly': []
                }
            
            daily_results[day]['predicted_kwh'] += predicted_energy
            daily_results[day]['actual_kwh'] += actual_power
            daily_results[day]['hours'] += 1
            daily_results[day]['outdoor_temps'].append(outdoor_temp)
            daily_results[day]['effective_temps'].append(effective_temp)
            daily_results[day]['solar_effects'].append(eff_result.solar_effect)
            daily_results[day]['hourly'].append({
                'hour': ts.astimezone(SWEDISH_TZ).hour,
                'outdoor': outdoor_temp,
                'effective': effective_temp,
                'solar_effect': eff_result.solar_effect,
                'predicted_kw': predicted_power
            })
    
    # Print results
    print(f"\n{'='*70}")
    print("DAILY PREDICTION ANALYSIS")
    print(f"{'='*70}")
    print(f"\n{'Date':<12} {'Hours':>6} {'Avg Out':>8} {'Avg Eff':>8} {'Solar':>8} {'Predicted':>10}")
    print("-" * 70)
    
    total_predicted = 0
    for day in sorted(daily_results.keys()):
        r = daily_results[day]
        avg_out = sum(r['outdoor_temps']) / len(r['outdoor_temps'])
        avg_eff = sum(r['effective_temps']) / len(r['effective_temps'])
        avg_solar = sum(r['solar_effects']) / len(r['solar_effects'])
        
        print(f"{day:<12} {r['hours']:>6} {avg_out:>7.1f}°C {avg_eff:>7.1f}°C {avg_solar:>+7.1f}°C {r['predicted_kwh']:>9.1f} kWh")
        total_predicted += r['predicted_kwh']
    
    print("-" * 70)
    print(f"{'TOTAL':<12} {'':<6} {'':<8} {'':<8} {'':<8} {total_predicted:>9.1f} kWh")
    
    # Show hourly detail for one day
    print(f"\n{'='*70}")
    print("HOURLY DETAIL: 2026-02-03 (coldest day)")
    print(f"{'='*70}")
    
    if '2026-02-03' in daily_results:
        r = daily_results['2026-02-03']
        print(f"\n{'Hour':>6} {'Outdoor':>10} {'Effective':>10} {'Solar':>10} {'Power':>10}")
        print("-" * 50)
        for h in sorted(r['hourly'], key=lambda x: x['hour']):
            print(f"{h['hour']:>5}h {h['outdoor']:>9.1f}°C {h['effective']:>9.1f}°C {h['solar_effect']:>+9.1f}°C {h['predicted_kw']:>9.2f} kW")
    
    client.close()


if __name__ == '__main__':
    main()
