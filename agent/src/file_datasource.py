from csv import reader
from datetime import datetime
from domain.accelerometer import Accelerometer
from domain.gps import Gps
from domain.aggregated_data import AggregatedData
import config


class FileDatasource:
    def __init__(
        self,
        accelerometer_filename: str,
        gps_filename: str,
    ) -> None:
        self.accelerometer_filename = accelerometer_filename
        self.gps_filename = gps_filename
        self.accelerometer_file = None
        self.gps_file = None

    def read(self) -> AggregatedData:
        accelerometer_data = None
        gps_data = None

        if self.accelerometer_file:
            accelerometer_data = self._read_accelerometer_data()

        if self.gps_file:
            gps_data = self._read_gps_data()

        return AggregatedData(
            accelerometer_data,
            gps_data,
            datetime.now(),
            config.USER_ID,
        )

    def start_reading(self, *args, **kwargs):
        self.accelerometer_file = open(self.accelerometer_filename, 'r')
        self.gps_file = open(self.gps_filename, 'r')

    def stop_reading(self):
        if self.accelerometer_file:
            self.accelerometer_file.close()
        if self.gps_file:
            self.gps_file.close()

    def _read_accelerometer_data(self):
        # Format: x, y, z
        row = next(reader(self.accelerometer_file))
        return Accelerometer(*map(float, row))

    def _read_gps_data(self):
        # Format: longitude, longitude
        row = next(reader(self.gps_file))
        return Gps(*map(float, row))
