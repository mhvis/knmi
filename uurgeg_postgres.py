"""Import an hourly measurements datafile into PostgreSQL.

Data from here: https://www.knmi.nl/nederland-nu/klimatologie/uurgegevens
"""
import argparse
from datetime import datetime, timezone, timedelta
from typing import Tuple, Dict, Optional

import psycopg2

# Expected columns in the input file.
COLUMNS = (
    'STN', 'YYYYMMDD', 'HH', 'DD', 'FH', 'FF', 'FX', 'T', 'T10N', 'TD', 'SQ', 'Q', 'DR', 'RH', 'P', 'VV', 'N', 'U',
    'WW', 'IX', 'M', 'R', 'S', 'O', 'Y'
)

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
    dew_point_temperature
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""


def visibility_to_meters(vv: int) -> Optional[int]:
    """Converts VV visibility number to meters."""
    if not vv:
        return None
    if vv <= 49:
        return (vv + 1) * 100
    elif vv == 50:
        return 6000
    elif 56 <= vv <= 79:
        return (vv - 56 + 7) * 1000
    elif 80 <= vv <= 89:
        return (vv - 80 + 7) * 5000
    print(f"Unexpected visibility value: {vv}.")
    return None


def parse_line(r: str) -> Dict[str, int]:
    row = tuple(int(w) if w else None for w in (v.strip() for v in r.split(',')))
    if len(row) != len(COLUMNS):
        raise ValueError(f"Unexpected row: {row}")
    return {COLUMNS[i]: row[i] for i in range(len(row))}


def row_to_database(r: Dict[str, int], station: int) -> Tuple:
    """Converts an input row to a database row."""
    d = str(r['YYYYMMDD'])
    # Hour ranges from 1 to 24. We subtract 1 to make the range 0..23 and then add 1 hour back.
    date = datetime(int(d[0:4]), int(d[4:6]), int(d[6:8]), r['HH'] - 1, tzinfo=timezone.utc) + timedelta(hours=1)

    return (
        station,
        date,
        r['DD'],  # wind_from_direction
        r['FF'] / 10.0,
        r['FX'] / 10.0,
        r['T'] / 10.0,
        r['U'],  # relative_humidity
        r['P'] / 10.0,
        visibility_to_meters(r['VV']),
        None,  # precipitation_duration
        r['N'],  # cloud_cover
        (r['Q'] / 3600.0) * 10000.0,  # global_solar_radiation, conversion from J/cm2 to W/m2
        0 if r['RH'] == -1 else r['RH'],  # precipitation_rate
        (r['SQ'] / 10.0) * 60 / 6,  # duration_of_sunshine, converted to minutes in the last 10 minutes
        r['TD'] / 10.0
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('filename')
    parser.add_argument('station', help='The station identifier used during insertion.', type=int)
    parser.add_argument('database_dsn')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()

    with open(args.filename) as f:
        # Skip header lines.
        for i in range(31):
            f.readline()
        # Verify that the columns in the file are as expected.
        file_columns = tuple(c.strip() for c in f.readline()[1:].split(','))
        if file_columns != COLUMNS:
            raise ValueError("File has unexpected columns.")
        # Line 33 should be empty.
        line = f.readline()
        if line.strip():
            raise ValueError("Unexpected line.")
        # Next lines should carry data.
        with psycopg2.connect(args.database_dsn) as conn:
            with conn.cursor() as curs:
                for r in f.readlines():
                    values = row_to_database(parse_line(r), args.station)
                    if args.verbose:
                        print(values)
                    curs.execute(SQL_INSERT, values)
                if args.dry_run:
                    raise RuntimeError("Dry run: not committing.")


if __name__ == '__main__':
    main()
