"""Downloads the latest dataset file."""
import logging

from knmi import get_latest

ANONYMOUS_API_KEY = 'eyJvcmciOiI1ZTU1NGUxOTI3NGE5NjAwMDEyYTNlYjEiLCJpZCI6IjI4ZWZlOTZkNDk2ZjQ3ZmE5YjMzNWY5NDU3NWQyMzViIiwiaCI6Im11cm11cjEyOCJ9'

logging.basicConfig(level=logging.DEBUG)

date, contents = get_latest(ANONYMOUS_API_KEY)

filename = 'observations' + date.strftime('%Y%m%d%H%M') + '.nc'
with open(filename, 'wb') as f:
    f.write(contents)

print(f'Date of latest measurement: {date}, saved at {filename}')
