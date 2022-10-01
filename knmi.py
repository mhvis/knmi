import logging
from datetime import datetime, timedelta, timezone

import requests

API_URL = 'https://api.dataplatform.knmi.nl/open-data/v1/datasets/Actuele10mindataKNMIstations/versions/2/files'

logger = logging.getLogger(__name__)


def _get_session(api_key: str) -> requests.Session:
    session = requests.Session()
    session.headers.update({
        'Authorization': api_key
    })
    return session


def get_filename(d: datetime) -> str:
    """Returns the filename for the dataset at given time."""
    d = d.astimezone(timezone.utc)
    d = d.replace(minute=int(d.minute / 10) * 10)
    d = d.strftime('%Y%m%d%H%M')
    return f'KMDS__OPER_P___10M_OBS_L2_{d}.nc'


def get_time(f: str) -> datetime:
    """Parses the time from a dataset filename."""
    if not f.startswith('KMDS__OPER_P___10M_OBS_L2_') and f.endswith('.nc'):
        raise ValueError('Invalid dataset filename.')
    return datetime(year=int(f[26:30]),
                    month=int(f[30:32]),
                    day=int(f[32:34]),
                    hour=int(f[34:36]),
                    minute=int(f[36:38]),
                    tzinfo=timezone.utc)


def get_latest(api_key: str) -> (datetime, bytes):
    """Returns the most recent available file from the dataset."""
    s = _get_session(api_key)
    # Find the most recent filename.
    start = datetime.now(timezone.utc) - timedelta(hours=1)
    files_data = s.get(API_URL, params={'startAfterFilename': get_filename(start), 'maxKeys': 10}).json()
    logger.debug('Files from last hour: %s', files_data)
    filenames = [f['filename'] for f in files_data['files']]
    if not filenames:
        raise RuntimeError('No dataset files found in the last hour.')
    latest = max(filenames)
    # Get the download URL and fetch contents.
    download_url = s.get(API_URL + f'/{latest}/url').json()['temporaryDownloadUrl']
    contents = requests.get(download_url).content
    return get_time(latest), contents
