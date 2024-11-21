import json
import logging
import socket
import threading
from queue import Queue

import worker
import traceback

# connection 정보를 list로 관리한다.
listConnectInfo = []
lock = threading.Lock()

# insupc에 전송했다가 받은 nAsId 가지고 전역변수 listConnectionInfo에서 conn 정보를 찾아서 리턴한다.
def funcGetConnection(nAsId):
    with lock :
        for conn in listConnectInfo:
            if conn:
                # 현재 시간을 단축하기 위해 다중접속 기능 개발 안한다. port만으로만 구분하도록 함.
                # 추후 insupc에 쿼리할때 port를 보내는게 아니라 asid를 관리하여 보내도록 하면 그때 변경하자..
                #if conn.getpeername()[0] == strClientIp:
                if conn.getpeername()[1] % 255 == nAsId: # nAsId가 1바이트 제한이라서... 보낼때도 이렇게 보낸다.
                    logging.info(f"find get connection from tcp server {conn.getpeername()[1]} % 255 = {conn.getpeername()[1] % 255} = {nAsId}")
                    return conn
    return None

# conn 정보를 가지고 local과 peer 결과를 string형태로 리턴하자.
def funcGetConnectionInfo(conn):
    strLocalInfo = conn.getsockname()[0] + ":" + str(conn.getsockname()[1])
    strPeerInfo = conn.getpeername()[0] + ":" + str(conn.getpeername()[1])
    return strLocalInfo, strPeerInfo

# json string 메시지를 argument로 받아서 cmd string 메시지를 리턴한다. 
def funcParsingJsonCommandToString(json_str):
    # JSON 문자열을 파이썬 딕셔너리로 변환
    message_dict = json.loads(json_str)
    cmd = message_dict.get("cmd", "").lower()  # cmd 값을 소문자로 변환
    return cmd

# json string 메시지를 argument로 받아서 auth 메시지를 만들자.
def funcMakeAuthResponseMessage(json_str):
    # JSON 문자열을 파이썬 딕셔너리로 변환
    message_dict = json.loads(json_str)
    
    # 리턴할 값을 나열해보자.
    strTransactionId = message_dict.get("reqNo")
    nResult = 0
    strSessionState = "Active"

    # json형식으로 encoding 하자.
    json_response = json.dumps({
        "result": nResult,
        "resultDesc": "Completed successfully",
        "reqNo": strTransactionId,
        "sessionState": strSessionState,
        "token": "temporary"
    })

    # 8자리 길이 prefix를 추가
    strResponseMessage = f"({json_response.__len__():08d}){json_response}"
    return strResponseMessage

# json string 메시지를 argument로 받아서 heartbeat 메시지를 만들자.
def funcMakeHeartBeatResponseMessage(json_str):
    # JSON 문자열을 파이썬 딕셔너리로 변환
    message_dict = json.loads(json_str)
    
    # 리턴할 값을 나열해보자.
    strTransactionId = message_dict.get("reqNo")
    nResult = 0
    strSessionState = "Active"

    # json형식으로 encoding 하자.
    strResponseMessage = json.dumps({
        "result": nResult,
        "resultDesc": "Completed successfully",
        "reqNo": strTransactionId,
        "sessionState": strSessionState,
    }, indent=4)
    return strResponseMessage



# worker queue로 보낼 메시지를 만들어보자.
def funcMakeQueueMessage(strRecvJsonMessage, client_ip, client_port) :
    strClientInfo = {
        "client_ip": client_ip,
        "client_port": client_port
    }
    
    strQueueMessage = json.dumps({"client_info": strClientInfo, "recv_message": strRecvJsonMessage})
    
    return strQueueMessage
    
# 서버에 접속된 client로부터 받은 데이터를 처리하자.
def handle_client(conn):
    try:
        while True:
            try:
                # 먼저 메시지 길이를 읽는다 (10바이트)
                strRecvLength = conn.recv(10).decode('utf-8')
                if not strRecvLength:
                    logging.info(f"Client disconnected: {conn.getpeername()}")
                    break  # while 루프를 벗어나 연결 종료 처리로 이동

                # 받은 길이 정보를 정수형으로 변환
                try:
                    nRecvLength = int(strRecvLength.strip('()'))
                except ValueError:
                    logging.error(f"Invalid length format: {strRecvLength}")
                    continue

                # 실제 메시지를 nRecvLength만큼 받는다
                strRecvMessage = conn.recv(nRecvLength).decode('utf-8')
                if not strRecvMessage:
                    logging.info(f"Client disconnected while receiving message: {conn.getpeername()}")
                    break  # while 루프를 벗어나 연결 종료 처리로 이동

                client_ip = conn.getpeername()[0]
                client_port = conn.getpeername()[1]
                
                # recv 메시지를 바로 logging하자.
                logging.info(f"[{conn.getpeername()[0]}:{conn.getpeername()[1]}->{conn.getsockname()[0]}:{conn.getsockname()[1]}] {strRecvMessage}")

                try:
                    # 메시지에서 JSON 부분만 추출
                    strRecvJsonMessage = strRecvMessage[strRecvMessage.index('{'):strRecvMessage.rindex('}')+1]
                    #logging.info(f"parsing json message >> {strRecvJsonMessage}")
                    
                    cmd = funcParsingJsonCommandToString(strRecvJsonMessage)
                    # logging.info(f"command >>> {cmd}")
                    # JSON 문자열로 command를 simple parsing하여 auth와 heartbeat의 경우 바로 응답하자.
                    if cmd == "auth":
                        strResponseMessage = funcMakeAuthResponseMessage(strRecvJsonMessage)
                        logging.info(f"[{conn.getpeername()[0]}:{conn.getpeername()[1]}<-{conn.getsockname()[0]}:{conn.getsockname()[1]}] {strResponseMessage}")
                        conn.send(strResponseMessage.encode())
                        continue
                    elif cmd == "hearbeat":
                        strResponseMessage = funcMakeHeartBeatResponseMessage(strRecvJsonMessage)
                        logging.info(f"[{conn.getpeername()[0]}:{conn.getpeername()[1]}<-{conn.getsockname()[0]}:{conn.getsockname()[1]}] {strResponseMessage}")
                        conn.send(strResponseMessage.encode())
                        continue
                    elif cmd == "execute":   # 처리해야될 메시지는 worker 큐에 넣어 async 처리하자.
                        strQueueMessage = funcMakeQueueMessage(strRecvJsonMessage, client_ip, client_port)
                        worker.fnQueuePut(strQueueMessage)
                        continue
                    else :
                        logging.error(f"....? what is this? {strRecvMessage}")
                except ValueError:
                    # JSON 형식이 아닌 메시지는 무시하고 다음 recv로 진행
                    continue

            except ConnectionResetError:
                logging.info(f"Connection reset by client: {conn.getpeername()}")
                break
            except ConnectionAbortedError:
                logging.info(f"Connection aborted by client: {conn.getpeername()}")
                break
            except Exception as e:
                logging.error(f"handle_client exception error {e}. {conn.getpeername()} line is {traceback.format_exc()}")
                break  # 예외 발생 시 연결 종료

    finally:
        # 연결 종료 처리
        try:
            strPeerInfo = f"{conn.getpeername()[0]}:{conn.getpeername()[1]}"
            strLocalInfo = f"{conn.getsockname()[0]}:{conn.getsockname()[1]}"
            
            # connection이 close되면 list에서 삭제
            global listConnectInfo
            if conn in listConnectInfo:
                with lock:
                    listConnectInfo.remove(conn)
                logging.info(f"Connection removed from list: client [{strPeerInfo}] / server [{strLocalInfo}]")
            
            conn.close()
            logging.info(f"Connection closed: client [{strPeerInfo}] / server [{strLocalInfo}]")
            
        except Exception as e:
            logging.error(f"Error during connection cleanup: {str(e)}")

# tcp 서버에서 연결을 관리하는 manager 스레드
def handle_manager(server_socket, nMaxConnection):
    global listConnectInfo
    while True:
        try:
            # tcp accept
            conn, addr = server_socket.accept()
            logging.info(f"server socket accept. {conn.getsockname()[0]}:{conn.getsockname()[1]}")

            # connection이 연결되면 list에 append한다.
            if conn not in listConnectInfo :
                logging.info(f"server listConnectInfo append connection: client [{conn.getpeername()[0]}:{conn.getpeername()[1]}] / server [{conn.getsockname()[0]}:{conn.getsockname()[1]}]")
                with lock :
                    listConnectInfo.append(conn) 
            else :
                logging.error(f"server listConnectInfo already have error!: client [{conn.getpeername()[0]}:{conn.getpeername()[1]}] / server [{conn.getsockname()[0]}:{conn.getsockname()[1]}]")
            logging.info(f"server Connection opened: client [{conn.getpeername()[0]}:{conn.getpeername()[1]}] / server [{conn.getsockname()[0]}:{conn.getsockname()[1]}]")

            # tcp accept된 메시지를 처리하기 위해 별도 thread를 생성한다.
            client_handler = threading.Thread(target=handle_client, args=(conn,))
            client_handler.start() 
            
            # 만약 multi connection이 필요하다면 추가로 Thread를 생성해서 처리해야할 것으로 보인다. 그리고 close시 Thread삭제하도록 하자.
            # 일단은 one Thread로 제작함.
            for i in range(nMaxConnection) :
                # client_handler = threading.Thread(target=handle_client, args=(conn,))
                pass
        except:
            # 일단 보류... conn이 생성되지 않을 수도 있다. 이건 try쪽에서 잘 찍어주는게 맞는 것 같다. 나중에 try에서 찍자. 일단 보류.
            # strPeerInfo, strLocalInfo = funcGetConnectionInfo(conn)
            logging.error(f"incomm tcp_server handle_manager except error.")
            break
    return


def fnStart(strServerIp, nServerPort, nMaxConnection):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # SO_REUSEADDR 옵션 설정
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((strServerIp, nServerPort))
    server_socket.listen(nMaxConnection)
    logging.info(f"Server Listening on {strServerIp}:{nServerPort} and waiting client.")

    # 연결되는 accept를 처리하기 위한 스레드를 생성한다.
    handle_thread = threading.Thread(target=handle_manager, args=(server_socket, nMaxConnection))
    handle_thread.start() 
    return 


def test():
    fnStart("127.0.0.1", 10002, 2)
    return
    

if __name__ == '__main__':
    test()