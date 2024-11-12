import socket
import struct
import threading
import logging
import time

# 서버 설정
server_ip = '0.0.0.0'
server_port = 10003

# access 메시지를 byte로 만들어서 리턴하자.
def funcMakeAccessMessage(byteRecvMessage) :
    # 데이터 해제
    header_format = '>HBBBB30s4sB17sI'
    unpacked_data = struct.unpack(header_format, byteRecvMessage)

    nBodyMessageLength = unpacked_data[0]
    nMsgCode = unpacked_data[1]
    byteSvca = unpacked_data[2]
    byteDvca = unpacked_data[3]
    nAsId = unpacked_data[4]
    strSessionId = unpacked_data[5].decode().strip('\0')
    strSvcId = unpacked_data[6].decode().strip('\0')
    nResult = unpacked_data[7]
    strWtime = unpacked_data[8].decode().strip('\0')
    nDummy = unpacked_data[9]

    logging.info(f"헤더 정보: nBodyMessageLength={nBodyMessageLength}, nMsgCode={nMsgCode}, byteSvca={hex(byteSvca)}, byteDvca={hex(byteDvca)}, nAsId={nAsId}, strSessionId={strSessionId}, strSvcId={strSvcId}, nResult={nResult}, strWtime={strWtime}, nDummy={nDummy}")

    # 변경할 메시지를 기재하자.
    new_nMsgCode = 4
    nResult = 1

    # 새로운 메시지 패킹
    byteResponseMessage = struct.pack('>HBBBB30s4sB17sI', nBodyMessageLength, new_nMsgCode, byteSvca, byteDvca, 
                                        nAsId, strSessionId.encode(), strSvcId.encode(), nResult, strWtime.encode(), nDummy)
    
    return byteResponseMessage

# heartbeat 메시지를 byte로 만들어서 리턴하자.
def funcMakeHearBeatMessage(byteRecvMessage) :
    # 데이터 해제
    header_format = '>HBBBB30s4sB17sI'
    unpacked_data = struct.unpack(header_format, byteRecvMessage)

    nBodyMessageLength = unpacked_data[0]
    nMsgCode = unpacked_data[1]
    byteSvca = unpacked_data[2]
    byteDvca = unpacked_data[3]
    nAsId = unpacked_data[4]
    strSessionId = unpacked_data[5].decode().strip('\0')
    strSvcId = unpacked_data[6].decode().strip('\0')
    nResult = unpacked_data[7]
    strWtime = unpacked_data[8].decode().strip('\0')
    nDummy = unpacked_data[9]

    logging.info(f"헤더 정보: nBodyMessageLength={nBodyMessageLength}, nMsgCode={nMsgCode}, byteSvca={hex(byteSvca)}, byteDvca={hex(byteDvca)}, nAsId={nAsId}, strSessionId={strSessionId}, strSvcId={strSvcId}, nResult={nResult}, strWtime={strWtime}, nDummy={nDummy}")

    # 변경할 메시지를 기재하자.
    new_nMsgCode = 6
    nResult = 1

    # 새로운 메시지 패킹
    byteResponseMessage = struct.pack('>HBBBB30s4sB17sI', nBodyMessageLength, new_nMsgCode, byteSvca, byteDvca, 
                                        nAsId, strSessionId.encode(), strSvcId.encode(), nResult, strWtime.encode(), nDummy)
    
    return byteResponseMessage

    
# query 메시지를 byte로 만들어서 리턴하자.
def funcMakeQueryMessage(byteRecvMessage) :
    # 데이터 해제
    header_format = '>HBBBB30s4sB17sI'
    unpacked_data = struct.unpack(header_format, byteRecvMessage)

    nBodyMessageLength = unpacked_data[0]
    nMsgCode = unpacked_data[1]
    byteSvca = unpacked_data[2]
    byteDvca = unpacked_data[3]
    nAsId = unpacked_data[4]
    strSessionId = unpacked_data[5].decode().strip('\0')
    strSvcId = unpacked_data[6].decode().strip('\0')
    nResult = unpacked_data[7]
    strWtime = unpacked_data[8].decode().strip('\0')
    nDummy = unpacked_data[9]

    logging.info(f"헤더 정보: nBodyMessageLength={nBodyMessageLength}, nMsgCode={nMsgCode}, byteSvca={hex(byteSvca)}, byteDvca={hex(byteDvca)}, nAsId={nAsId}, strSessionId={strSessionId}, strSvcId={strSvcId}, nResult={nResult}, strWtime={strWtime}, nDummy={nDummy}")

    # 변경할 메시지를 기재하자.
    new_nMsgCode = 2
    nResult = 1

    # 새로운 메시지 패킹
    byteResponseMessage = struct.pack('>HBBBB30s4sB17sI', nBodyMessageLength, new_nMsgCode, byteSvca, byteDvca, 
                                        nAsId, strSessionId.encode(), strSvcId.encode(), nResult, strWtime.encode(), nDummy)
    
    # 위는 임시코드임. 실제 body가 없음.
    # 아래 body 코드에 대한 packing이 추가로 필요함.
    '''
        Param Num.= 3
    [1] Type = 2:Operation Name
        Value = 'mcidPstnGetInfoV2'
    [2] Type = 4:Sql Output
        1) = '1':1
        2) = '025671033':9
        3) = '10':2
        4) = '0':1
        5) = '1':1
        6) = '50500':5
        7) = '1':1
        8) = '10100':5
        9) = '025671033':9
    [3] Type = 5:Sql Result
        Value = [0x00 0x00]
    '''
    return byteResponseMessage

def handle_client(client_socket):
    try:
        while True:
            # 데이터 수신
            data = client_socket.recv(1024)
            if not data:
                break

            # 수신된 메시지 출력
            logging.info(f"수신된 메시지: {data}")

            # 메시지 해제
            header_format = '>HBBBB30s4sB17sI'
            header_size = struct.calcsize(header_format)
            header_data = data[:header_size]
            body_data = data[header_size:]

            unpacked_header = struct.unpack(header_format, header_data)
            nBodyMessageLength = unpacked_header[0]
            nMsgCode = unpacked_header[1]
            byteSvca = unpacked_header[2]
            byteDvca = unpacked_header[3]
            nAsId = unpacked_header[4]
            strSessionId = unpacked_header[5].decode().strip('\0')
            strSvcId = unpacked_header[6].decode().strip('\0')
            nResult = unpacked_header[7]
            strWtime = unpacked_header[8].decode().strip('\0')
            nDummy = unpacked_header[9]

            logging.info(f"헤더 정보: nBodyMessageLength={nBodyMessageLength}, nMsgCode={nMsgCode}, byteSvca={hex(byteSvca)}, byteDvca={hex(byteDvca)}, nAsId={nAsId}, strSessionId={strSessionId}, strSvcId={strSvcId}, nResult={nResult}, strWtime={strWtime}, nDummy={nDummy}")


            byteResponseMessage = None
            if nMsgCode == 3 :      # access message case
                byteResponseMessage = funcMakeAccessMessage(header_data)
            elif nMsgCode == 5 :    # hb message case
                byteResponseMessage = funcMakeHearBeatMessage(header_data)
            elif nMsgCode == 1 :    # query message case
                byteResponseMessage = funcMakeQueryMessage(header_data)
            if (byteResponseMessage) :
                logging.info(f"tcp response send:{byteResponseMessage}")
                client_socket.sendall(byteResponseMessage)
                # test
                #for i in range(10):
                #    time.sleep(0.001)
                #    client_socket.sendall(byteResponseMessage)

    except Exception as e:
        logging.error(f"클라이언트 처리 중 오류 발생: {e}")
    finally:
        client_socket.close()

def start_server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((server_ip, server_port))
    server_socket.listen(5)
    logging.info(f"서버가 {server_ip}:{server_port}에서 시작되었습니다.")

    try:
        while True:
            client_socket, addr = server_socket.accept()
            logging.info(f"클라이언트 연결됨: {addr}")
            client_handler = threading.Thread(target=handle_client, args=(client_socket,))
            client_handler.start()
    except Exception as e:
        logging.error(f"서버 실행 중 오류 발생: {e}")
    finally:
        server_socket.close()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    start_server()
