import socket 
import threading 
import time
import os
import datetime
import logging

#서버의 호스트 이름과 포트 번호
server_host = '127.0.0.1' 
server_port = 10001

# incomm으로 전달할 excute 메시지
seq = 1000000
def fnMakeExecuteMessage():
    global seq 
    seq += 1
    #message = f"""{{(00000130){"cmd":"execute","seq"{seq}:,"service":"INSUP","token":"Q29uc3V...","reqNo":"20200122134500.A1234","reqBody":{{"apiName":"mcidGetProfile","inputParams":["1","2","3"]}}}}}""" 
    #message = f"""{{"(00000130){{\"cmd\":\"execute\",\"seq\":,\"service\":\"INSUP\",\"token\":\"Q29uc3V...\",\"reqNo\":\"20200122134500.A1234\",\"reqBody\":{{\"apiName\":\"mcidGetProfile\",\"inputParams\":[\"1\",\"2\",\"3\"]}}}}"}}"""
    #message = """(00000130){\"cmd\":\"execute\",\"seq\":1,\"service\":\"INSUP\",\"token\":\"Q29uc3V...\",\"reqNo\":\"20200122134500.A1234\",\"reqBody\":{\"apiName\":\"mcidGetProfile\",\"inputParams\":[\"1\",\"2\",\"3\"]}}"""
    #message = f"(00000130){\"cmd\":\"execute\",\"seq\":1,\"service\":\"INSUP\",\"token\":\"Q29uc3V...\",\"reqNo\":\"20200122134500.A1234\",\"reqBody\":{\"apiName\":\"mcidGetProfile\",\"inputParams\":[\"1\",\"2\",\"3\"]}}"
    #message = f"(00000130){{"cmd":"execute","seq":,"service":"INSUP","token":"Q29uc3V...","reqNo":"20200122134500.A1234","reqBody":{{"apiName":"mcidGetProfile","inputParams":["1","2","3"]}}}}"
    #message = f"(00000130){{"cmd":"execute","seq":1,"service":"INSUP","token":"Q29uc3V...","reqNo":"20200122134500.A1234","reqBody":{{"apiName":"mcidGetProfile","inputParams":["1","2","3"]}}}}"
    #message = """(00000130){\"cmd\":\"execute\",\"seq\":1,\"service\":\"INSUP\",\"token\":\"Q29uc3V...\",\"reqNo\":\"20200122134500.A1234\",\"reqBody\":{\"apiName\":\"mcidGetProfile\",\"inputParams\":[\"1\",\"2\",\"3\"]}}"""
    message = f"""(00000130){{"cmd":"execute","seq":{seq},"service":"INSUP","token":"Q29uc3V...","reqNo":"{seq}","reqBody":{{"apiName":"mcidGetProfile","inputParams":["1","2","3"]}}}}"""
    return message

def millisleep(milliseconds): 
    time.sleep(milliseconds / 1000.0)
    return

#메시지 전송을 담당하는 함수
def send_messages(sock): 
    time.sleep(1)
    #messages_per_second = 1
    #interval = 1.0 / messages_per_second # 메시지 간격 (초) 
    nMilliSecond = 1000
    nCPs = 1000
    sleep_time = nMilliSecond / nCPs 
    #message = "Hello, Server!"
    try :
        while True:
            #
            message = fnMakeExecuteMessage()
            sock.sendall(message.encode())
            logging.info(f"{message}")
            millisleep(sleep_time)
            """
            for _ in range(messages_per_second):
                message = fnMakeExecuteMessage()
                sock.sendall(message.encode())
                print(f"Send from server: {message}")
                time.sleep(interval)
            time.sleep(1)  # 10개의 메시지를 보낸 후 1초간 대기
            """
    except :
        print("send exception. so break.")
        


#서버로부터의 응답을 받는 함수
def receive_responses(sock): 
    try:
        while True: 
            response = sock.recv(4096) 
            if not response: 
                break 
            logging.info(f"Received from server: {response.decode()}")
    finally:
        sock.close()

#메인 함수
def main(): 
    log_name = "simulation"
    log_path = "./log/"

    # log path make
    if not os.path.exists(log_path):
        os.makedirs(log_path)
    current_date = datetime.date.today()
    full_log_path = os.path.join(log_path, log_name + '.' + str(current_date))

    # 로그 생성
    logging.basicConfig(filename=full_log_path, 
                        level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    
    logging.info("Process Start %s", log_name)



    # 소켓 객체 생성 및 서버에 연결 
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    client_socket.connect((server_host, server_port)) 
    print(f"Connected to server at : [{server_host}:{server_port}]")

    # 스레드 생성 및 시작
    sender_thread = threading.Thread(target=send_messages, args=(client_socket,))
    receiver_thread = threading.Thread(target=receive_responses, args=(client_socket,))

    sender_thread.start()
    receiver_thread.start()

    # 스레드가 종료될 때까지 기다림
    sender_thread.join()
    receiver_thread.join()

    # 소켓 닫기
    client_socket.close()
    print("Connection closed.")

    
if __name__ == "__main__":
    main()


