import argparse
import csv
import sys
from datetime import datetime, timezone, timedelta

from netCDF4 import Dataset


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('filename')
    parser.add_argument('--print-columns', action='store_true', help="Print column attributes instead of data.")
    parser.add_argument('--print-time', action='store_true', help="Print time of measurement.")
    args = parser.parse_args()

    rootgrp = Dataset(args.filename)

    # Get the data variables. The filtered variables have an incorrect dimension.
    variables = [v for v in rootgrp.variables if v not in ('time', 'iso_dataset', 'product', 'projection')]
    station_count = rootgrp.dimensions['station'].size

    if args.print_columns:
        for v in variables:
            var = rootgrp[v]

            def g(attr: str):
                return getattr(var, attr, '')

            print(f"{v}: {var.dtype},{var.shape},{g('long_name')},{g('standard_name')},{g('units')},{g('comment')}")

    elif args.print_time:
        t = datetime(1950, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=float(rootgrp['time'][0]))
        print(t)

    else:
        # Print header
        writer = csv.writer(sys.stdout)
        writer.writerow(variables)
        # Print data
        for i in range(station_count):
            def val(v):
                if rootgrp[v].shape == (station_count,):
                    return rootgrp[v][i]
                elif rootgrp[v].shape == (station_count, 1):
                    return rootgrp[v][i][0]
                raise ValueError("Unexpected variable shape.")

            writer.writerow(str(val(v)) for v in variables)


if __name__ == '__main__':
    main()
