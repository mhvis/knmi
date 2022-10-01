"""Inserts the latest 10-minute observation into a PostgreSQL database."""
import argparse

import psycopg2
from netCDF4 import Dataset

from knmi import get_latest

SQL_INSERT = """INSERT INTO knmi_data (
    station,
    time,
    wind_from_direction,
    wind_speed,
    wind_speed_of_gust,
    air_temperature,
    relative_humidity,
    air_pressure_at_sea_level,
    visibility_in_air,
    precipitation_duration,
    cloud_cover,
    global_solar_radiation,
    precipitation_rate,
    duration_of_sunshine,
    dew_point_temperature,
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('station', help="5-digit identifier of the station to upload.")
    parser.add_argument('database_dsn', help="The PostgreSQL connection DSN.")
    parser.add_argument('api_key', help="The KNMI API key.")
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    date, contents = get_latest(args.api_key)

    rootgrp = Dataset('in_memory', memory=contents)
    stations = list(rootgrp['station'])
    station_index = stations.index(args.station)

    def val(col: str):
        return float(rootgrp[col][station_index][0])

    row = (
        # Station identifier
        int(args.station),
        # Measurement time
        date,
        val('dd'),  # wind_from_direction (degrees)
        val('ff'),  # wind_speed (m/s)
        val('gff'),  # wind_speed_of_gust (m/s)
        val('ta'),  # air_temperature (degrees Celcius)
        val('rh'),  # relative_humidity (%)
        val('pp'),  # air_pressure_at_sea_level (hPa)
        val('zm'),  # visibility_in_air (m)
        val('dr'),  # precipitation_duration (sec)
        val('nc'),  # cloud_cover (octa)
        val('qg'),  # global_solar_radiation (W/m2)
        val('rg'),  # precipitation_rate (mm/h)
        val('ss'),  # duration_of_sunshine (min)
        val('td'),  # dew_point_temperature (degrees Celcius)
    )
    print(row)

    if not args.dry_run:
        with psycopg2.connect(args.database_dsn) as conn:
            with conn.cursor() as curs:
                curs.execute(SQL_INSERT, row)


if __name__ == '__main__':
    main()
