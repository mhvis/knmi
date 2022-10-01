import unittest
from datetime import datetime, timezone

from knmi import get_time


class TestCases(unittest.TestCase):
    def test_get_time(self):
        self.assertEqual(get_time('KMDS__OPER_P___10M_OBS_L2_202210011410.nc'),
                         datetime(2022, 10, 1, 14, 10, tzinfo=timezone.utc))


if __name__ == '__main__':
    unittest.main()
