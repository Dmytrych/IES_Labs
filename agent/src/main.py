from paho.mqtt import client as mqtt_client
import time
from schema.aggregated_data_schema import AggregatedDataSchema
from file_datasource import FileDatasource
import config
from schema.aggregated_parking_schema import AggregatedParkingSchema


def connect_mqtt(broker, port):
    """Create MQTT client"""
    print(f"CONNECT TO {broker}:{port}")

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print(f"Connected to MQTT Broker ({broker}:{port})!")
        else:
            print("Failed to connect {broker}:{port}, return code %d\n", rc)
            exit(rc)  # Stop execution

    client = mqtt_client.Client()
    client.on_connect = on_connect
    client.connect(broker, port)
    client.loop_start()
    return client


def publish(client, datasource, delay, get_update_data_callbacks):
    datasource.start_reading()
    while True:
        time.sleep(delay)
        for get_data_callback in get_update_data_callbacks:
            topic, msg = get_data_callback(datasource)
            result = client.publish(topic, msg)
            result: [0, 1]
            status = result[0]
            if status == 0:
                pass
                print(f"Send `{msg}` to topic `{topic}`")
            else:
                print(f"Failed to send message to topic {topic}")


def get_mqtt_parking_data_callback(topic):
    def get_mqtt_parking_data(datasource):
        data = datasource.read_parking()
        msg = AggregatedParkingSchema().dumps(data)
        return [topic, msg]

    return get_mqtt_parking_data


def get_mqtt_accelerometer_data_callback(topic):
    def get_mqtt_accelerometer_data(datasource):
        data = datasource.read()
        msg = AggregatedDataSchema().dumps(data)
        return [topic, msg]

    return get_mqtt_accelerometer_data


def run():
    # Prepare mqtt client
    client = connect_mqtt(config.MQTT_BROKER_HOST, config.MQTT_BROKER_PORT)
    # Prepare datasource
    datasource = FileDatasource("data/accelerometer.csv", "data/gps.csv", "data/parking.csv")

    publishers = [get_mqtt_parking_data_callback(config.MQTT_PARKING_DATA_TOPIC),
                  get_mqtt_accelerometer_data_callback(config.MQTT_ACCELEROMETER_DATA_TOPIC)]

    # Infinity publish data
    publish(client, datasource, config.DELAY, publishers)


if __name__ == "__main__":
    run()
