from csv import reader
from datetime import datetime
from domain.accelerometer import Accelerometer
from domain.aggregated_parking import AggregatedParking
from domain.gps import Gps
from domain.aggregated_data import AggregatedData
import config
from domain.parking import Parking


class FileDatasource:
    def __init__(
        self,
        accelerometer_filename: str,
        gps_filename: str,
        parking_filename: str
    ) -> None:
        self.accelerometer_filename = accelerometer_filename
        self.gps_filename = gps_filename
        self.parking_filename = parking_filename
        self.accelerometer_file = None
        self.gps_file = None
        self.parking_file = None

    def read(self) -> AggregatedData:
        accelerometer_data = None
        gps_data = None

        try:
            accelerometer_data = self._read_accelerometer_data()
            gps_data = self._read_gps_data()
        except:
            self.renew_connections()
            accelerometer_data = self._read_accelerometer_data()
            gps_data = self._read_gps_data()

        return AggregatedData(
            accelerometer_data,
            gps_data,
            datetime.now(),
            config.USER_ID,
        )

    def read_parking(self) -> AggregatedParking:
        parking_data = None

        try:
            parking_data = self._read_parking_data()
        except:
            self.renew_connections()
            parking_data = self._read_parking_data()

        return AggregatedParking(
            parking_data,
            datetime.now(),
            config.USER_ID,
        )

    def start_reading(self, *args, **kwargs):
        self.accelerometer_file = open(self.accelerometer_filename, 'r')
        self.gps_file = open(self.gps_filename, 'r')
        self.parking_file = open(self.parking_filename, 'r')

        # Skipping the rows with column names
        next(self.accelerometer_file)
        next(self.gps_file)
        next(self.parking_file)

    def stop_reading(self):
        if self.accelerometer_file:
            self.accelerometer_file.close()
        if self.gps_file:
            self.gps_file.close()
        if self.parking_file:
            self.parking_file.close()

    def renew_connections(self):
        self.stop_reading()
        self.start_reading()

    def _read_accelerometer_data(self):
        # Format: x, y, z
        row = next(reader(self.accelerometer_file))
        return Accelerometer(*map(float, row))

    def _read_gps_data(self):
        # Format: latitude, longitude
        row = next(reader(self.gps_file))
        return Gps(*map(float, row))

    def _read_parking_data(self):
        # Format: latitude, longitude, empty_count
        row = next(reader(self.parking_file))
        latitude, longitude, empty_count = map(float, row)
        gps = Gps(latitude, longitude)
        return Parking(empty_count, gps)
