import datetime
import json
import queue
import socket
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, List

import pytz
import requests

import hes_mqtt_server as gi_hes
from serversocket import ServerSocket

print("Version", "1.1.0.21")


class Client:
    def __init__(self) -> None:

        self.LOG_MODE_ON = True

        self.TCPSocketTimeout = 90  # seconds
        self.TCPSocketPort = 4059

        self.IST = pytz.timezone('Asia/Kolkata')

        self.POST_URL = "https://ibotmdegrpcapicalling20220204220357.azurewebsites.net/api/mdmsconnectd"
        self.headers = {
            'Ocp-Apim-Subscription-Key': '9640c68c7c3e445bbddf732801c6ec6a',
            'Ocp-Apim-Trace': 'true',
            'Content-Type': 'text/plain'
        }
        self.Payload = {"date_updated": "2022-01-10T12:40:00", "phase_id": "1"}

        print(f"Total arguments passed: {len(sys.argv)}")
        for arg in range(len(sys.argv)):
            print(f"Argument {arg}: {sys.argv[arg]}")

        print(f"\nName of Python script: {sys.argv[0]}")

        self.InstanceName = sys.argv[1]

        self.path = "../"  # Path to the folder where the files are stored

        with open('meterlist.json') as meterList:
            self.meterlist_json = json.load(meterList)

        self.TOTAL_WORKER = len(self.meterlist_json['Meters'])
        print(self.TOTAL_WORKER)

    def print_log(self, msg: str):
        """Print a message with the datetime using timezopne IST before it"""
        if self.LOG_MODE_ON:
            print(f"{datetime.datetime.now(self.IST)} {msg}")

        else:
            ...

    def create_tcp_clinet(self, client_ip: str, device_id: str) -> Optional[socket.socket]:
        """
        Create a TCP connection using python built-in socket module
        :param client_ip: Ip of the client to connect to.
        :returns: A socket connection if succesful, else `None`
        """
        socket_address: Optional[socket.socket] = None
        try:
            self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] [SOCK-STARTED-OPENING] ")
            socket_address = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
            socket_address.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1);
            socket_address.connect((client_ip, self.TCPSocketPort))

            # socket_address.settimeout(self.TCPSocketTimeout)

            # socket_address.setblocking(1)
            #
            socket_address.setblocking(0)
            # print(datetime.datetime.now(), "[SOCK-]", socket_address)
            self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] [PULL] [SOCK-SUCCESSFULL-OPEN]  {socket_address} ] ")
            return socket_address

        except Exception as e:  # Try being more specific about the exception you are handling
            self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] [PULL] [SOCK-CONN-FAIL] [Exception] {e}")
            # print(datetime.datetime.now(), "[SOCK-CONN-FAIL]")
            return None

    def destroy_tcp_clinet(self, txsocket_address: socket.socket, client_ip: str, device_id: str) -> None:
        """
        Destroys a given tcp connection
        :param txsocket_address: socket address to destoy
        :param client_ip: IP
        :param device_id: String with the device id
        """
        try:
            self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] [PULL] [SOCK Closing Started] ")
            txsocket_address.shutdown(socket.SHUT_RDWR)
            txsocket_address.close()
            self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] [PULL] [SOCK Closing Done]")
            time.sleep(1)
        except Exception as e:

            self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] [PULL] [SOCK Closing bad] [Exception] {e}")
            # print(datetime.datetime.now(), "[SOCK Closing bad]")

    def recreate_tcp_client(self, txsocket_address: socket.socket, client_ip: str, device_id: str) -> Optional[
        socket.socket]:
        """
        Destroys and newy create a given tcp connection
        :param txsocket_address: socket address to destoy
        :param client_ip: IP
        :param device_id: String with the device id
        :returns: A socket connection if succesful else `None`
        """
        self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] [PULL] [SOCK Recreating] ")
        self.destroy_tcp_clinet(txsocket_address, client_ip, device_id)
        return self.create_tcp_clinet(client_ip, device_id)

    def datatxrx(self, socket_address: socket.socket, datatotx: bytes, device_id: str, client_ip: str,
                 meter_to_hes_dlmsmessagequeue: queue.Queue) -> bool:
        """
        Tries sending a data through a socket
        :param socket_address: Socket to perform the connection
        :param datatotx: Bytes to send
        :param device_id: String
        :param client_ip: String
        :param meter_to_hes_dlmsmessagequeue: Queue
        :return: Bool indicating if the sending was successful
        """
        try:
            byte_sent = socket_address.send(datatotx)
            time.sleep(0.2)
        except Exception as e:
            self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] [PULL] [Data Sending Failed] [Exception] {e}")
            return False

        if byte_sent == len(datatotx):
            self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] [PULL] [Sending to Meter SUCCESS] [Data-->] [{len(datatotx)}] {datatotx.hex()} ")
            # start_time = datetime.datetime.now()
            # end_time = start_time + datetime.timedelta(seconds=15)
            # while loop_counter <= 2:
            # loop_counter = 0
            # socket_address.settimeout(self.TCPSocketTimeout)
            # self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] [SOCK-TIMEOUT]  {socket_address.gettimeout()} ] ")
            try:

                data = self.recv_timeout(socket_address, 90, device_id, client_ip)
                if len(data) > 0:
                    dlms_message = device_id, data
                    self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] [PULL] [From Meter] [Data<--] [{len(data)}] {data.hex()}")
                    if meter_to_hes_dlmsmessagequeue.full():
                        self.print_log(
                            f"[ {device_id} ] [ {client_ip} ] [TCP] [PULL] Clearning a Queue")
                        meter_to_hes_dlmsmessagequeue.queue.clear()
                    meter_to_hes_dlmsmessagequeue.put_nowait(dlms_message)
                    # self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] Loop Counter {loop_counter}")
                    return True
                else:
                    self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] [PULL] Read Timeout")
                    return False
                # ready = select.select([socket_address], [], [], 140)
                # if ready[0]:
                # data = mysocket.recv(4096)
                # data = socket_address.recv(1024)
                # if len(data) > 0:
                #    dlms_message = device_id, data
                #    self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] [From Meter] [Data<--]  {data.hex()}")
                #    if meter_to_hes_dlmsmessagequeue.full():
                #        self.print_log(
                #            f"[ {device_id} ] [ {client_ip} ] Clearning a Queue")
                #        meter_to_hes_dlmsmessagequeue.queue.clear()
                #    meter_to_hes_dlmsmessagequeue.put_nowait(dlms_message)
                # self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] Loop Counter {loop_counter}")
                #    return True
                # else:
                #  #  socket_address.settimeout(self.TCPSocketTimeout)
                #    self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] Data Rx Timeout  1st time, Let's Retry Once the Receving")
                #    time.sleep(10)
                #    data = socket_address.recv(1024)
                #    if len(data) > 0:
                #        dlms_message = device_id, data
                #        self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] [From Meter] [Data<--]  {data.hex()}")
                #        if meter_to_hes_dlmsmessagequeue.full():
                #            self.print_log(
                #                f"[ {device_id} ] [ {client_ip} ] Clearning a Queue")
                #            meter_to_hes_dlmsmessagequeue.queue.clear()
                #        meter_to_hes_dlmsmessagequeue.put_nowait(dlms_message)
                #        # self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] Loop Counter {loop_counter}")
                #        return True
                # loop_counter += 1
                #    self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] Data Rx Timeout Happened 2nd Time")
                # self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] Loop Counter {loop_counter}")
                #    return False

                # else:
                #    loop_counter += 1
                #    self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] Loop Counter {loop_counter}")
                #    self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] Read Timeout")
                #   return False
                # break
            except Exception as e:
                self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] [PULL] [Exception] [In Receive] {e}")
                # self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] [SOCK-RX FAIL] Loop Counter {loop_counter}")
                return False
                # loop_counter += 1
                # break
            time.sleep(1)
            # for i in range(10000000):  # 1 seconds time delay, depends on your computer frequency, you can change the value
            #    pass
        else:

            self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] [PULL] [Data Send to Meter FAIL]")
            return False
        return False

    def recv_timeout(self, socket_address, timeout, device_id, client_ip):
        # make socket non blocking
        socket_address.setblocking(0)

        # total data partwise in an array
        total_data = [];
        data = '';

        # beginning time
        begin = time.time()
        while 1:
            # if you got some data, then break after timeout
            if total_data and time.time() - begin > timeout:
                self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] [PULL] [From Meter]")
                break

            # if you got no data at all, wait a little longer, twice the timeout
            elif time.time() - begin > timeout * 2:
                self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] [PULL] [Receive Timeout Happen]")
                break

            # recv something
            try:
                data = socket_address.recv(1024)
                if data:
                    total_data.append(data)
                    self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] [PULL] [From Meter]")
                    break
                    # change the beginning time for measurement
                    begin = time.time()
                else:
                    # sleep for sometime to indicate a gap
                    time.sleep(0.1)
            except:
                pass

        # join all parts to make final string
        return data

    def tcp_create_monitor(self, client_ip: str, device_id: str) -> None:
        """
        Create a tcp monitor
        :param client_ip: String
        :param device_id: String
        """
        # print("Thread %s: starting", device_id)
        # print(clinentInfo)
        # print(clinentInfo[0],clinentInfo[1])
        hes_to_meter_dlms_message_queue: queue.Queue = queue.Queue(5)
        # hes_to_meter_retry_dlms_message_queue: queue.Queue = queue.Queue(10)
        meter_to_hes_dlms_message_queue: queue.Queue = queue.Queue(5)
        try:
            go_hes = gi_hes.GCServer("SYNC", device_id, client_ip, self.InstanceName)
            go_hes.v_init()
        except Exception as e:
            self.print_log(f"[ {device_id} ] [ {client_ip} ] [HES] [PULL] [exception triggered at GCServer()] {e}")
            exit(1)
            raise

        # hesClientRUN(device_id,hes_to_meter_dlms_message_queue,meter_to_hes_dlms_message_queue)
        socket_address: Optional[socket.socket] = None
        total_tcpmqtt_transaction = 0
        total_tcpmqtt_failed_transaction = 0
        total_tcp_fail_transaction = 0
        total_mqtt_fail_transaction = 0
        total_queue_fail_transaction = 0
        while True:
            time.sleep(0.4)
            # for i in range(4000000): # it creates time delay
            #    pass
            # print("Thread %s: starting", device_id)
            # print_log(f"[ {device_id} ] [ {client_ip} ] [Looping] ")
            try:
                dlms_from_hes = go_hes.v_run()
            except Exception as e:
                total_mqtt_fail_transaction += 1
                self.print_log(f"[ {device_id} ] [ {client_ip} ] [HES] [PULL]"
                               f"Total FAILED Transaction :- {total_mqtt_fail_transaction}")
                self.print_log(f"[ {device_id} ] [ {client_ip} ] [HES] [PULL] [exception triggered at v_run() ] {e}")
                # exit(1)
                # raise
            try:
                if dlms_from_hes is not None:
                    # print(dlms_from_hes)
                    if hes_to_meter_dlms_message_queue.full():
                        hes_to_meter_dlms_message_queue.queue.clear()
                        self.print_log(f"[ {device_id} ] [ {client_ip} ] [QUEUE] [PULL] [Clearing Queue, HES to Meter]")
                    hes_to_meter_dlms_message_queue.put_nowait(dlms_from_hes)
                    self.print_log(f"[ {device_id} ] [ {client_ip} ] [QUEUE] [PULL] [Data Added to Queue, HES to Meter]")
            except Exception as e:  # Try being more specific about the exception you are handling
                total_queue_fail_transaction += 1
                self.print_log(f"[ {device_id} ] [ {client_ip} ] [QUEUE] [PULL] "
                               f"Total Failed Transaction :- {total_queue_fail_transaction}")
                self.print_log(f"[ {device_id} ] [ {client_ip} ] [QUEUE] [exception triggered at Queue] {e}")
                # exit(1)
                # raise

            try:
                if not meter_to_hes_dlms_message_queue.empty():
                    self.print_log(f"[ {device_id} ] [ {client_ip} ] [QUEUE] [PULL] [Taking Data from Queue, Meter to HES]")
                    dlms_from_meter = meter_to_hes_dlms_message_queue.get_nowait()
                    data_to_send = device_id, dlms_from_meter[1]
                    self.print_log(f"[ {device_id} ] [ {client_ip} ] [QUEUE] [PULL] [Sending Data from Queue, Meter to HES]")
                    go_hes.send(data_to_send)
            except Exception as e:  # Try being more specific about the exception you are handling
                # print_log(f"{datetime.datetime.now(IST)} [ {device_id} ] [ {client_ip} ] [exception triggered] {e}")
                total_queue_fail_transaction += 1
                self.print_log(f"[ {device_id} ] [ {client_ip} ] [QUEUE] [PULL] "
                               f"Total Failed Transaction :- {total_queue_fail_transaction}")
                self.print_log(f"[ {device_id} ] [ {client_ip} ] [QUEUE] [Exception triggered at Queue] {e}")
                # exit(1)
                # raise

            if not hes_to_meter_dlms_message_queue.empty():
                self.print_log(f"[ {device_id} ] [ {client_ip} ] [QUEUE] [PULL] [Data in Queue]")

                if socket_address is None:
                    for retry_cnt in range(1, 3):
                        self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] [PULL] [Creating Socket]")
                        socket_address = self.create_tcp_clinet(client_ip, device_id)
                        if socket_address is not None:
                            break
                        else:
                            total_tcp_fail_transaction += 1
                            self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] [PULL]"
                                           f"Total Failed Transaction :- {total_tcp_fail_transaction}")
                            retry_cnt = retry_cnt + 1
                            if retry_cnt >= 3:
                                self.print_log(
                                    f"[ {device_id} ] [ {client_ip} ] [TCP] [PULL] [Creating socket Failing for multiple times :- {retry_cnt}] discarding the msg now")
                                hes_to_meter_dlms_message_queue.get_nowait()
                            self.print_log(
                                f"[ {device_id} ] [ {client_ip} ] [TCP] [PULL] [Retrying the socket opening...] [Retry Cnt :- {retry_cnt}]")
                        time.sleep(10)

                if not hes_to_meter_dlms_message_queue.empty() and socket_address is not None:
                    self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] [PULL] [Processing from queue]")
                    dlms_for_meter1 = hes_to_meter_dlms_message_queue.get_nowait()
                    try:
                        ret_val = self.datatxrx(socket_address, dlms_for_meter1[2], device_id, client_ip,
                                                meter_to_hes_dlms_message_queue)

                        if ret_val is False:
                            self.destroy_tcp_clinet(socket_address, client_ip, device_id)
                            socket_address = None
                            # if retry_cnt <= 2:
                            #     if hes_to_meter_retry_dlms_message_queue.full() == True:
                            #         hes_to_meter_retry_dlms_message_queue.queue.clear()
                            #     hes_to_meter_retry_dlms_message_queue.put_nowait(dlms_for_meter1[2])
                            #     retry_cnt = retry_cnt + 1
                            #     print(datetime.datetime.now(IST),"[",device_id,"]","[",client_ip,"]","[SOCK-TX Will retry as RX timeout happen]",retry_cnt)
                        else:
                            total_tcpmqtt_transaction += 1
                            self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] [PULL] [TCP-HES SUCCESS] "
                                           f"Total Transaction :- {total_tcpmqtt_transaction}")
                            self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] [PULL] [Closing Socket Now]")
                            #self.destroy_tcp_clinet(socket_address, client_ip, device_id)

                            # socket_address.shutdown(2)
                            # socket_address.close()
                            #socket_address = None
                            data = {}
                            data['date_updated'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                            data['phase_id'] = "1"
                            json_data = json.dumps(data)
                            parameter = {}
                            parameter['pkValue'] = device_id
                            parameter['tableName'] = "total_4g_meters"
                            parameter['accessKey'] = "123"
                            r = requests.post(self.POST_URL, params=parameter, headers=self.headers, data=json_data)
                            # print content of request
                            # print(r.text)
                    except Exception as e:
                        total_tcpmqtt_failed_transaction += 1
                        self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP-HES] [PULL]"
                                       f"DEBUG API CALL FAILED  :- {total_tcpmqtt_failed_transaction}")
                        # assert type(socket_address) == socket.socket, 'SocketAdress is supposed to be set at this point'
                        self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] [PULL] [Exception at tcpmqtt failed] {e}")
                        #self.destroy_tcp_clinet(socket_address, client_ip, device_id)
                        # socket_address.close()
                        #socket_address = None
                        #self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] [PULL] [Closing Socket Now]")
                        # exit(1)
                        # raise


class Server:
    def __init__(self) -> None:
        self.LOG_MODE_ON = True
        self.TCPServerSocketTimeout = 120  # seconds
        self.TCPServerSocketPort = 3392
        self.meter_to_dlms_message_queue = queue.Queue(100)
        self.IST = pytz.timezone('Asia/Kolkata')
        self.numberOfPush = 0
        self.InstanceName = sys.argv[1] + "_PUSH"
        with open('meterlist.json') as meterList:
            self.meterlist_json = json.load(meterList)

    def echo(self, ip, queue, data):
        dlms_message = [ip[0], data]
        self.numberOfPush += 1
        self.print_log(f"[ {ip} ] [TCP] [PUSH] [ {data} ] " f"Total TCP Push Rx :- {self.numberOfPush}")
        if self.meter_to_dlms_message_queue.full() == True:
            self.meter_to_dlms_message_queue.queue.clear()
        self.meter_to_dlms_message_queue.put_nowait(dlms_message)

    def print_log(self, msg: str):
        """Print a message with the datetime using timezopne IST before it"""
        if self.LOG_MODE_ON:
            print(f"{datetime.datetime.now(self.IST)} {msg}")

        else:
            ...

    def tcp_push_monitor(self) -> None:
        """
        Create tcp Server &
        """
        try:
            go_hes = gi_hes.GCServer("SYNC", "PUSH_DEVICE", "PUSH_IP", self.InstanceName)
            go_hes.v_init()
        except Exception as e:
            self.print_log(f"[HES] [PUSH] [exception triggered at GCServer()] {e}")
            exit(1)
            raise
        while 1:
            try:
                go_hes.v_run()
                if self.meter_to_dlms_message_queue.empty() == False:
                    self.print_log(f"[QUEUE] [PUSH] [Taking Data from Queue & Finding IP]")
                    dlms_from_meter = self.meter_to_dlms_message_queue.get_nowait()
                    # parse the IP and then send to DLMS
                    for meterIP in self.meterlist_json['Meters']:
                        #print(meterIP['IP'],dlms_from_meter[0])
                        if meterIP['IP'] == dlms_from_meter[0]:
                            self.print_log(f" [TCP] [PUSH] [IP v ID Found]")
                            self.print_log(
                                f"[TCP] [PUSH] [ {meterIP['ID']} ] [ {meterIP['IP']} ] {[dlms_from_meter[1].hex()]} ")
                            data_to_send = [meterIP['ID'], dlms_from_meter[1]]
                            go_hes.send(data_to_send)
                            break
                        else:
                            print('.', end='')

                time.sleep(0.1)
            except Exception as e:
                time.sleep(0.2)
                self.print_log(f"[HES] [PUSH] [exception triggered] {e}")

    def destroy_tcp_clinet(self, txsocket_address: socket.socket) -> None:
        """
        Destroys a given tcp connection
        :param txsocket_address: socket address to destoy
        :param client_ip: IP
        :param device_id: String with the device id
        """
        try:
            #self.print_log(f"[ {device_id} ] [ {client_ip} ] [TCP] [SOCK Closing Started] ")
            txsocket_address.shutdown(socket.SHUT_RDWR)
            txsocket_address.close()
            self.print_log(f" [TCP] [PUSH] [SOCK Closing Done]")
            time.sleep(1)
        except Exception as e:
            self.print_log(f"[ [TCP] [PUSH] [SOCK Closing bad] [Exception] {e}")

    def TCPServerRUN(self):
        while 1:
            try:
                self.print_log(f"[TCP] [PUSH] [Socket Opening] ")
                self.serversocket = ServerSocket(
                    "0:0:0:0:0:0:0:0", 3392, self.echo, 1000, 2048
                )
                # server = TCPServer("0:0:0:0:0:0:0:0", 3392, self.echo)
                self.print_log(f"[TCP] [PUSH] [Successfully Created] ")
                break
            except Exception as e:
                self.print_log(f"[TCP] [PUSH] [TCP Server Socket Issue Exception :- ] {e} ")
            self.print_log(f"[TCP] [PUSH] [TCP Server Retry]")
            time.sleep(10)
        self.print_log(f"[TCP] [PUSH][TCP Server Socket Starting Loop] ")
        while 1:
            try:
                self.serversocket.run()
            except Exception as e:
                self.print_log(f"[TCP] [PUSH] [TCP Server Run Issue Exception :- ] {e} ")
            time.sleep(0.1)


if __name__ == '__main__':
    counter = 0
    thread_tcp_createmonitor: List = []
    client = Client()
    server = Server()
    with ThreadPoolExecutor(max_workers=client.TOTAL_WORKER + 2) as executor1:
        for meterIP in client.meterlist_json['Meters']:
            print(meterIP)
            executor1.submit(client.tcp_create_monitor, meterIP['EIP'], meterIP['ID'])
        executor1.submit(server.tcp_push_monitor)
        executor1.submit(server.TCPServerRUN)
