import configparser
import datetime
import ssl
from typing import Optional, List

import pytz

try:
    import queue
except ImportError:
    import Queue as queue
import paho.mqtt.client as hes_mqtt


# print(Py_object)
# print(Py_object['Meters'][0])
#
# try:
#     #decoded = json.loads(json_input)
#
#     # Access data
#     for x in Py_object['Meters']:
#         print(x['ID'])
#         print(x['IP'])
#
# except (ValueError, KeyError, TypeError):
#     print("JSON format error")


class GCServer:
    """Class that manages pàho.mqtt.client"""
    hes_client: hes_mqtt.Client

    def __init__(self, device_id: str, device_id_number: str, meter_ip_full: str, instance_name: str):
        self.name = device_id
        self.meter_id = device_id_number
        self.meter_ip = meter_ip_full
        self.instance_name = instance_name
        self.config_object = configparser.ConfigParser()
        self.counter = 0
        self.IST = pytz.timezone('Asia/Kolkata')
        # fileObject = open("meterlist.json","r")
        # self.jsonMeterObject = json.load(fileObject)
        # fileObject.close()

        try:
            self.config_object.read("./app_config.ini")
            self.mqttInfo = self.config_object["MQTT_HES_Config"]
        except Exception:
            print("ERROR, Reading Config Object failed, MQTT_LOCAL_Config in Module")

        self.rxMessageQueue: queue.Queue = queue.Queue(30)

    def say_hi(self):
        print(f'Hello, my name is {self.name}')

    def build_topic(self, prefix: str, topic_id: str) -> str:
        # print(f"{datetime.datetime.now(self.IST)} [ {self.meter_id} ] [ {self.meter_ip} ] [HES] [TOPIC] {prefix}{self.name}/{topic_id}")
        # print(f"{prefix}{self.name}/{topic_id}")
        return f"{prefix}{self.name}/{topic_id}"

    # @staticmethod
    def mqtt_connect_result_code(argument):
        reasons = {
            0: "Connection successful",
            1: "Connection refused - incorrect protocol version",
            2: "Connection refused - invalid client identifier",
            3: "Connection refused - server unavailable",
            4: "Connection refused - bad username or password",
            5: " Connection refused - not authorised",
        }
        return reasons.get(argument, "nothing")

    def callback_hes_on_connect(self, _client, _userdata, _flags, rc):
        # print(self.meter_id, f"Connected with result code {rc} :- {self.mqtt_connect_result_code(rc)}")
        print(
            f"{datetime.datetime.now(self.IST)} [ {self.meter_id} ] [ {self.meter_ip} ] [HES] Connected with result code {rc}")
        # for meter_id in self.jsonMeterObject['Meters']:
        # for meter_id in self.jsonMeterObject['Meters']:
        #     self.hes_client.subscribe(
        #         self.BuildTOPIC(self.mqttInfo.get("MQTT_DLMS_COMMAND_MESSAGE_PREFIX"),
        #                         meter_id['ID']),
        #                         self.mqttInfo.getint("MQTT_QOS"))

        self.hes_client.subscribe(
            self.build_topic(self.mqttInfo.get("MQTT_DLMS_COMMAND_MESSAGE_PREFIX"), self.meter_id),
            self.mqttInfo.getint("MQTT_QOS")
        )

    def callback_hes_on_disconnect(self, _client, _userdata, rc):
        print(
            f"{datetime.datetime.now(self.IST)} [ {self.meter_id} ] [ {self.meter_ip} ] [HES] client :- {_client}  disconnecting reason {rc}")
        # print(f"{self.meter_id} disconnecting reason {rc}")
        self.hes_client.reconnect()

    def callback_hes_on_message(self, _client, _userdata, msg):
        self.counter += 1
        print(
            f"{datetime.datetime.now(self.IST)} [ {self.meter_id} ] [ {self.meter_ip} ] [HES] [From HES] {msg.topic}")
        print(
            f"{datetime.datetime.now(self.IST)} [ {self.meter_id} ] [ {self.meter_ip} ] [HES] [Data < --] {msg.payload}")

        # print(msg.topic, self.counter)
        t_message = [msg.topic, msg.payload]
        if self.rxMessageQueue.full():
            self.rxMessageQueue.queue.clear()
            print(
                f"{datetime.datetime.now(self.IST)} [ {self.meter_id} ] [ {self.meter_ip} ] [HES] {msg.topic} [Queue Cleared]")
        try:
            self.rxMessageQueue.put_nowait(t_message)
        except Exception as e:  # Try being more specific about the exception you are handling
            print(
                f"{datetime.datetime.now(self.IST)} [ {self.meter_id} ] [ {self.meter_ip} ] [HES] {msg.topic} [exception triggered] {e}")
            # exit(1)
            # raise

    def callback_hes_on_publish(self, _client, _userdata, mid):
        print(
            f"{datetime.datetime.now(self.IST)} [ {self.meter_id} ] [ {self.meter_ip} ] [HES] [PUBLISH DONE] client :- {_client}  publish mid :- {mid}")

    # @staticmethod
    def callback_hes_on_subscribe(self, _client, _userdata, mid, _granted_qos):
        print(
            f"{datetime.datetime.now(self.IST)} [ {self.meter_id} ] [ {self.meter_ip} ] [HES] subscribe mid :- {mid}")
        # print(f"subscribe mid :- {mid}")

    def send(self, data: tuple):
        topic = self.build_topic(self.mqttInfo.get("MQTT_DLMS_RESPONSE_MESSAGE_PREFIX"), data[0])
        print(
            f"{datetime.datetime.now(self.IST)} [ {self.meter_id} ] [ {self.meter_ip} ] [HES] [Sending to HES] [Data -->] {topic} {data[1]}")

        self.hes_client.publish(
            topic,
            data[1],
            self.mqttInfo.getint("MQTT_QOS")
        )

    def v_init(self):
        self.hes_client = hes_mqtt.Client(
            client_id=f"{self.name} {self.meter_id}_{self.instance_name}",
            clean_session=format(self.mqttInfo["MQTT_CLEAN_SESSION"]),
            userdata=None
        )

        self.hes_client.max_queued_messages_set(self.mqttInfo.getint("MQTT_CLIENT_QUEUE_SIZE"))
        self.hes_client.reconnect_delay_set(
            min_delay=self.mqttInfo.getint("MQTT_RECONNECT_DELAY"),
            max_delay=self.mqttInfo.getint("MQTT_RECONNECT_MAX_DELAY")
        )
        # self.hes_client.will_set(
        #     self.BuildTOPIC(self.mqttInfo.get("MQTT_DLMS_COMMAND_MESSAGE_PREFIX"),self.mqttInfo.get("MQTT_MESSAGE_ID_WILL_MESSAGE")),
        #     payload=self.mqttInfo.get("MQTT_WILL_MESSAGE"),
        #     qos=1,
        #     retain=False
        # )
        self.hes_client.username_pw_set(self.mqttInfo.get("MQTT_USERNAME"), password=self.mqttInfo.get("MQTT_PASSWORD"))
        self.hes_client.tls_set(
            ca_certs=self.mqttInfo.get("MQTT_CA_CERTIFICATE"),
            certfile=self.mqttInfo.get("MQTT_CLIENT_CERTIFICATE"),
            keyfile=self.mqttInfo.get("MQTT_CLIENT_KEY"),
            cert_reqs=ssl.CERT_REQUIRED,
            tls_version=ssl.PROTOCOL_TLSv1_2,
            ciphers=None
        )
        self.hes_client.tls_insecure_set(True)
        self.hes_client.on_connect = self.callback_hes_on_connect
        self.hes_client.on_disconnect = self.callback_hes_on_disconnect
        self.hes_client.on_message = self.callback_hes_on_message
        self.hes_client.on_publish = self.callback_hes_on_publish
        self.hes_client.on_subscribe = self.callback_hes_on_subscribe
        self.hes_client.connect(
            self.mqttInfo.get("MQTT_BROKER_ADDRESS"),
            port=self.mqttInfo.getint("MQTT_BROKER_PORT"),
            keepalive=self.mqttInfo.getint("MQTT_KEEP_ALIVE_TIME")
        )

    def v_run(self) -> Optional[List[str]]:
        self.hes_client.loop_start()
        if not self.rxMessageQueue.empty():
            t_message = self.rxMessageQueue.get_nowait()
            parse_topic = t_message[0].split("/")
            msg_to_return = ["DLMS", parse_topic[2], t_message[1]]
            # print(msgToReturn)
            return msg_to_return
        return None
