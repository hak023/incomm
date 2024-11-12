import datetime
import itertools
import json
import logging
import random
import select
import socket
import struct
import threading
import time
import config
import sip_svc_tcp_server
import main

listClientInfo = []
cycleListClientInfo = None
lockClientInfo = threading.Lock()
lockSocket = threading.Lock()

def funcStart():
    global cycleListClientInfo
    global listClientInfo
    config_dict = config.fnGetConfig()

    if config_dict :
        insupc_ip1 = config_dict['insupc_ip1']
        insupc_ip2 = config_dict['insupc_ip2']
        insupc_port1 = config_dict['insupc_port1']
        insupc_port2 = config_dict['insupc_port2']

        cycleListClientInfo = itertools.cycle(listClientInfo)

        # client 동작을 위한 스레드를 생성한다.
        handle_thread1 = threading.Thread(target=client_manager, args=(insupc_ip1, insupc_port1, ))
        handle_thread1.start() 

        # 2번은 현재 테스트상 하지 않는다. 따라서 주석 처리.
        # handle_thread2 = threading.Thread(target=client_manager, args=(insupc_ip2, insupc_port2, ))
        # handle_thread2.start()
    else :
        logging.error("Config Load Error...!!! config_dict null.")
    return

# insupc와 통신할 유니크한 sequenceid를 만들자. 문자열로 return하여 사용할 것임.
def funcMakeUniqueSequenceId() :
    # 현재 시간 정보 가져오기
    now = datetime.datetime.now()

    # 현재 시간의 각 요소 추출
    year = now.year
    month = now.month
    day = now.day
    hour = now.hour
    minute = now.minute
    second = now.second
    millisecond = now.microsecond // 1000

    # 변수 aaa 생성
    nSequenceId = year * 10000000000 + month * 100000000 + day * 1000000 + hour * 10000 + minute * 100 + second + millisecond / 1000
    return str(nSequenceId)

# tcp send 함수가 필요하다.
def funcSendTcp(client_socket, byteHeartBeatMessage) :
    with lockSocket:
        client_socket.sendall(byteHeartBeatMessage)

# access 메시지를 byte로 만들어서 리턴하자.
def funcMakeAccessMessage(nSystemId) :
    ################## header ####################
    # nBodyMessageLength 2byte
    nBodyMessageLength = 8
    # nMsgCode 1byte / 1=query_req, 2=query_rsp, 3=logon_req, 4=logon_rsp, 5=hb_req, 6=hb_rsp
    nMsgCode = 3
    # byteSvca 1byte / 0xf0 / 고정값
    byteSvca = 0xf0
    # byteDvca = 1byte / 0xb1 / 고정값
    byteDvca = 0xb1
    # nAsId = 1byte / 0~255 랜덤 인자값.
    nAsId = nSystemId
    # strSessionId 30byte / 모르겠다... 일단 고정값 쓰자.
    # strSessionId = "9158069199017"
    strSessionId = funcMakeUniqueSequenceId()
    # strSvcId = 4byte 고정값 쓰자.
    strSvcId = "TEST"
    # nResult = 0 1byte 0 고정값 사용한다. 요청은 항상 0으로.
    nResult = 0
    # strWtime = 0. 17 byte 고정값 사용한다. 요청은 항상 0으로.
    strWtime = "0" 
    # DUMMY 4byte. 0 고정값.
    nDummy = 0

    byteHeaderMessage = struct.pack('>HBBBB30s4sB17sI', nBodyMessageLength, nMsgCode, byteSvca, byteDvca, 
                                    nAsId, strSessionId.encode(), strSvcId.encode(), nResult, strWtime.encode(), nDummy)

    ################## parameter ####################
    # nParamCount = 1 / 1byte / param갯수. 1개.
    nParamCount = 1
    # nParamType = 1 / 1byte / 1=logon_info
    nParamType = 7
    # nParamLength = 4 / 2byte 아래 data가 4byte이므로.
    nParamLength = 4
    # nParamData = svca 헤더와 연관있어보이지만 일단 고정값 쓰자. 각 1byte씩. 총 4byte
    byteLogonInfoData1 = 0xf0
    byteLogonInfoData2 = 0x01
    byteLogonInfoData3 = 0x01
    byteLogonInfoData4 = 0x03

    byteBodyMessage = struct.pack('>BBHBBBB', nParamCount, nParamType, nParamLength, byteLogonInfoData1, byteLogonInfoData2, byteLogonInfoData3, byteLogonInfoData4)
    byteAccessMessage = byteHeaderMessage + byteBodyMessage
    return byteAccessMessage

# heartbeat 메시지를 byte로 만들어서 리턴하자.
def funcMakeHearBeatMessage(nSystemId) :
    ################## header ####################
    # nBodyMessageLength 2byte
    nBodyMessageLength = 0
    # nMsgCode 1byte / 1=query_req, 2=query_rsp, 3=logon_req, 4=logon_rsp, 5=hb_req, 6=hb_rsp
    nMsgCode = 5
    # byteSvca 1byte / 0xf0 / 고정값
    byteSvca = 0xf0
    # byteDvca = 1byte / 0xb1 / 고정값
    byteDvca = 0xb1
    # nAsId = 1byte / 0~255 랜덤 인자값.
    nAsId = nSystemId
    # strSessionId 30byte / 모르겠다... 일단 고정값 쓰자.
    # strSessionId = "9158069199017"
    strSessionId = funcMakeUniqueSequenceId()
    # strSvcId = 4byte 고정값 쓰자.
    strSvcId = "TEST"
    # nResult = 0 1byte 0 고정값 사용한다. 요청은 항상 0으로.
    nResult = 0
    # strWtime = 0. 17 byte 고정값 사용한다. 요청은 항상 0으로.
    strWtime = "0" 
    # DUMMY 4byte. 0 고정값.
    nDummy = 0

    byteHeaderMessage = struct.pack('>HBBBB30s4sB17sI', nBodyMessageLength, nMsgCode, byteSvca, byteDvca, 
                                    nAsId, strSessionId.encode(), strSvcId.encode(), nResult, strWtime.encode(), nDummy)
    
    return byteHeaderMessage
    
# DB query 메시지를 byte로 만들어서 리턴하자.
def funcMakeQueryMessage(nSystemId, strMdnNumber, nTrasactionId) :
    ################## header ####################
    # nBodyMessageLength 2byte
    nBodyMessageLength = 8
    # nMsgCode 1byte / 1=query_req, 2=query_rsp, 3=logon_req, 4=logon_rsp, 5=hb_req, 6=hb_rsp
    nMsgCode = 1
    # byteSvca 1byte / 0xf0 / 고정값
    byteSvca = 0xf0
    # byteDvca = 1byte / 0xb1 / 고정값
    byteDvca = 0xb1
    # nAsId = 1byte / 0~255 랜덤 인자값.
    if (nSystemId > 255): # byte제한인 255에 맞추자. 현재 port를 systemid를 대신해서 쓰므로 임시코드. 나중에 systemid를 제대로 관리하면 없어져야함.
        nAsId = nSystemId % 255
    else:
        nAsId = nSystemId
    # strSessionId 30byte / 모르겠다... 일단 고정값 쓰자.
    # sipsvc로부터 받은 transactionid를 전달하자.
    strSessionId = nTrasactionId
    # strSvcId = 4byte 고정값 쓰자.
    strSvcId = "TEST"
    # nResult = 0 1byte 0 고정값 사용한다. 요청은 항상 0으로.
    nResult = 0
    # strWtime = 0. 17 byte 고정값 사용한다. 요청은 항상 0으로.
    strWtime = "0" 
    # DUMMY 4byte. 0 고정값.
    nDummy = 0

    byteHeaderMessage = struct.pack('>HBBBB30s4sB17sI', nBodyMessageLength, nMsgCode, byteSvca, byteDvca, 
                                    nAsId, strSessionId.encode(), strSvcId.encode(), nResult, strWtime.encode(), nDummy)

    ################## parameter ####################
    # nParamCount = 1 / 1byte / param갯수. 2개.
    nParamCount = 2

    # nParamType = 1 / 1byte / 1=logon_info, 2=operation_name, 3=sql_input(mdn)
    nParam1Type = 2
    # nParamLength = 2byte 
    nParam1Length = 0
    # 쿼리를 위한 API 문자열을 사용하자.
    strParam1Value = "mcidPstnGetInfoV2"
    nParam1Length = len(strParam1Value) # strApiValue의 length값.

    # nParamType = 1 / 1byte / 1=logon_info, 2=operation_name, 3=sql_input(mdn)
    nParam2Type = 3
    # nParamLength = 2byte 
    nParam2Length = 0
    # 쿼리를 위한 API 문자열을 사용하자.
    #strParam2Value = "111112222"
    strParam2Value = strMdnNumber
    nParam2Length = len(strParam2Value) # strApiValue의 length값.

    # 패킹
    byteBodyMessage = struct.pack(f'!BBH{nParam1Length}sBH{nParam2Length}s',
                                nParamCount,
                                nParam1Type, nParam1Length, strParam1Value.encode(),
                                nParam2Type, nParam2Length, strParam2Value.encode())
    byteQueryMessage = byteHeaderMessage + byteBodyMessage
    logging.info(f"[send] incomm -> insupc Header[nBodyMessageLength({nBodyMessageLength}), nMsgCode({nMsgCode}), byteSvca({byteSvca}), byteDvca({byteDvca}), nAsId({nAsId}), strSessionId({strSessionId}), strSvcId({strSvcId}), nResult({nResult}), strWtime({strWtime}), nDummy({nDummy})], Body[TYPE({nParam1Type}:{strParam1Value}, MDN({nParam1Type}:{strParam1Value})]")
    return byteQueryMessage

# 수신된 메시지의 Header를 분석하여 type과 result를 리턴하자.
def funcDecodeHeaderMessage(byteRecvMessage, client_socket) :
    # 데이터 해제
    unpacked_header = struct.unpack('>HBBBB30s4sB17sI', byteRecvMessage[:62])
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
    if (nMsgCode == 4):
        logging.info(f"[{client_socket.getsockname()[0]}:{client_socket.getsockname()[1]}<-{client_socket.getpeername()[0]}:{client_socket.getpeername()[1]}] Receive Access Message")
    elif (nMsgCode == 6):
        logging.info(f"[{client_socket.getsockname()[0]}:{client_socket.getsockname()[1]}<-{client_socket.getpeername()[0]}:{client_socket.getpeername()[1]}] Receive HeartBeat Message")
    elif (nMsgCode == 2):
        logging.info(f"""[{client_socket.getsockname()[0]}:{client_socket.getsockname()[1]}<-{client_socket.getpeername()[0]}:{client_socket.getpeername()[1]}] Receive Query Message : nBodyMessageLength: {nBodyMessageLength}, nMsgCode: {nMsgCode}, byteSvca: {hex(byteSvca)}, byteDvca: {hex(byteDvca)}, nAsId: {nAsId}, strSessionId: {strSessionId}, strSvcId: {strSvcId}, nResult: {nResult}, strWtime: {strWtime}, nDummy: {nDummy}""")
    else:
        logging.info(f"""[{client_socket.getsockname()[0]}:{client_socket.getsockname()[1]}<-{client_socket.getpeername()[0]}:{client_socket.getpeername()[1]}] Receive Unknown Message : nBodyMessageLength: {nBodyMessageLength}, nMsgCode: {nMsgCode}, byteSvca: {hex(byteSvca)}, byteDvca: {hex(byteDvca)}, nAsId: {nAsId}, strSessionId: {strSessionId}, strSvcId: {strSvcId}, nResult: {nResult}, strWtime: {strWtime}, nDummy: {nDummy}""")


    return nMsgCode, nResult, nAsId

# insupc에 query한 후 response를 수신한 data의 body를 decode하기 위한 함수. data는 전송받은 그대로 넣으면 header 62byte를 제외하고 parsing한다.
def funcDecodeQueryMessage(data):
    # 아래 두개를 return하도록 예제만듬.
    #1) = '1':1
    #2) = '025671033':9

    # <<<<<<<<<<<<<현재 시뮬레이터가 받은 메시지 그대로 return하게 되어있어서(body없이) 시뮬레이터 환경에서는 주석처리함. 이후 insupc랑 붙일때는 해제하자.
    # pcap기준 data의 83번째 byte 부터 body가 시작되는데
    # 99번째에 length 1과 100번째에 value 1이 옴. 따라서.
    # unpacked_header = struct.unpack('>HBBBB30s4sB17sI', data[61:]) << 이게 body의 시작이고 
    # 계산해보면 이후 32byte를 뛰어넘어야함. 따라서 93부터.
    #unpacked_mdn_length = struct.unpack('>B', data[93:])
    #mdn_length = unpacked_mdn_length[0]
    #unpacked_mdn_number = struct.unpack(f'>{mdn_length}s', data[95:]) # 이거 95번째일수도 있음. length가 2byte아니었나? 확인 필요.
    #str_mdn_number = unpacked_mdn_number[0]
    str_mdn_number = "025671033"

    #nResult = 1 # 91번째일걸? 확인 아직 안했으므로 고정값. 그리고 일단 전체 result결과를 표시하므로 필요없음.

    #byteBodyMessage = struct.pack(f'!BBH{nParam1Length}sBH{nParam2Length}s',
    return str_mdn_number

# json string 메시지를 argument로 받아서 query 결과를 response 메시지를 만들자. 
def funcMakeQueryResponseMessage(strTransactionId, nSeq, nResult, data):
    logging.info(f"recv query response : {data}")
    strMdnNumber = funcDecodeQueryMessage(data)

    # json형식으로 encoding 하자.
    strResponseMessage = json.dumps({
        "cmd": "excute",
        "seq": nSeq,
        "service": "INSUP", 
        "reqNo": strTransactionId,
        "rspBody": {
            "result": nResult,
            "apiName": "mcidGetProfile",
            "outputParams": [str(nResult),strMdnNumber]
        }
    }, indent=4)
    return strResponseMessage

# insupc 에 질의할때 loadbalanace를 위한 함수.
# 호출할때마다 연결되어있는 socket을 리턴한다. 가용가능한 socket이 없으면 None 을 리턴함.
def getClientSockets():
    global listClientInfo
    global cycleListClientInfo
    
    for dictClientInfo in listClientInfo :
        dictClientInfo = next(cycleListClientInfo)
        if dictClientInfo["connected"] == True :
            return dictClientInfo["socket"]
    return None

def recv_manager(client_socket, epoll):
    try:
        events = epoll.poll(1)
        for fileno, event in events:
            if fileno == client_socket.fileno():
                if event & select.EPOLLIN:
                    recv_data(client_socket)
            else:
                # 예외 처리 등 필요한 작업 수행
                # 발생하는 케이스가 있나? 일단 로그만.
                logging.info(f"recv_data socket error {str(e)} [{client_socket.getpeername()[0]}:{client_socket.getpeername()[1]}] / server [{client_socket.getsockname()[0]}:{client_socket.getsockname()[1]}]")
    except Exception as e: 
        with lockSocket:
            # 소켓이 닫혔을 경우 epoll에서 제거
            epoll.unregister(client_socket.fileno())
            client_socket.close()
        logging.info(f"recv_data socket error {str(e)} [{client_socket.getpeername()[0]}:{client_socket.getpeername()[1]}] / server [{client_socket.getsockname()[0]}:{client_socket.getsockname()[1]}]")

# 데이터 수신 함수
def recv_data(client_socket):
    try:
        data = client_socket.recv(1024)
        if data:
            # 즉시 처리할 수 있는 메시지를 처리하자.
            nMsgCode, nResult, nAsId = funcDecodeHeaderMessage(data, client_socket)
            if (nMsgCode == 4 or nMsgCode == 6) : # ping or access
                #logging.info("ping 왔어요~ access message 왔어요. 로그추가 필요.")
                # 아무것도 안하면 될듯? return
                return
            elif nMsgCode == 2 : # query response
                #logging.info("query response:", data.decode()) << 일케 쓰면 안된다. err 났음. 일단 주석처리.

                # 정석은 워커로 올려서 처리해야 하지만 
                # 시간을 줄이기 위해 바로처리하자.
                conn = sip_svc_tcp_server.funcGetConnection(nAsId)
                if conn:
                    # 아래 메시지 받은걸 넣도록 하자.
                    strResponseMessage = funcMakeQueryResponseMessage(nAsId, nAsId, nResult, data)
                    nMessageSize = len(strResponseMessage)
                    # (00000130) {json} << 이런식으로 만들어지도록 함.
                    strTotalMessage = "(" + str(nMessageSize).zfill(8) + ")" + strResponseMessage
                    strLocalInfo, strPeerInfo = sip_svc_tcp_server.funcGetConnectionInfo(conn)
                    logging.info(f"[{strPeerInfo}<-{strLocalInfo}] Length:{nMessageSize} Message:{strTotalMessage}")
                    conn.send(strTotalMessage.encode())
                else:
                    strpeer, strlocal = sip_svc_tcp_server.funcGetConnectionInfo(conn)
                    logging.error(f"not found sipsvc connection. so message drop. {strpeer}-{strlocal}")
                return 
            else :
                logging.error("Unknown Message Received:", data.decode())
        else:
            with lockSocket:
                # 소켓이 닫혔을 경우 epoll에서 제거
                select.epoll.unregister(client_socket.fileno())
                client_socket.close()
    except Exception as e:
        logging.info(f"recv_data socket error {str(e)} ")

def client_manager(insupc_ip, insupc_port):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.setblocking(0)

    epoll = select.epoll()
    epoll.register(client_socket.fileno(), select.EPOLLOUT)

    bConnected = False

    dictClientInfo = {
        "socket": client_socket,
        "connected": bConnected
    }
    global listClientInfo
    with lockClientInfo:
        listClientInfo.append(dictClientInfo)

    # 별도의 스레드에서 recv 동작을 수행. 필요없을것같아서 일단 주석처리함.
    #recv_thread = threading.Thread(target=recv_manager, args=(client_socket,))
    #recv_thread.start()

    # heartbeat 전송을 위한 timer를 설정하자.
    last_heartbeat_time = time.time()

    # systemID를 정하자. 귀찮으니 랜덤값으로.
    nSystemId = random.randint(0, 255)

    while True:
        try:
            # 연결되어 있지 않은 상태라면 connect 시도
            if not bConnected:
                try :
                    client_socket.connect((insupc_ip, insupc_port))
                except BlockingIOError:
                    #logging.error(f"BlockingIOError occur")
                    pass

                events = epoll.poll(1)  # 1초 동안 대기합니다.

                for fileno, event in events:
                    if fileno == client_socket.fileno() and event & select.EPOLLOUT:
                        # 연결이 완료되었습니다.
                        bConnected = True
                        epoll.modify(client_socket.fileno(), select.EPOLLIN)
                        with lockClientInfo:
                            dictClientInfo["connected"] = bConnected
                        logging.info(f"insupc client socket connect: client [{client_socket.getpeername()[0]}:{client_socket.getpeername()[1]}] / server [{client_socket.getsockname()[0]}:{client_socket.getsockname()[1]}]")
                        # tcp connect 이후 access 메시지를 만들어서 전송.
                        byteAccessMessage = funcMakeAccessMessage(nSystemId)
                        funcSendTcp(client_socket, byteAccessMessage)
                        logging.info(f"[{client_socket.getsockname()[0]}:{client_socket.getsockname()[1]}->{client_socket.getpeername()[0]}:{client_socket.getpeername()[1]}] Send AccessMessage")
            else :
                # 10초 주기로 한번씩 heartbeat
                if time.time() - last_heartbeat_time >= 10 : 
                    # 시간을 갱신하고
                    last_heartbeat_time = time.time()
                    # heartbeat 메시지를 보내자.
                    byteHeartBeatMessage = funcMakeHearBeatMessage(nSystemId)
                    funcSendTcp(client_socket, byteHeartBeatMessage)
                    logging.info(f"[{client_socket.getsockname()[0]}:{client_socket.getsockname()[1]}->{client_socket.getpeername()[0]}:{client_socket.getpeername()[1]}] Send HeartBeat Message")
                logging.info(f"current time:{time.time()} / last time: {last_heartbeat_time}")
                
                recv_manager(client_socket, epoll)
                # 0.001초 interval << 필요없어보인다.
                # time.sleep(0.001)
        except Exception as e:
            logging.info(f"socket error {e} {insupc_ip}:{insupc_port}")
            bConnected = False
            with lockClientInfo:
                dictClientInfo["connected"] = bConnected
            with lockSocket:
                client_socket.close()
            logging.info(f"insupc client socket close: {insupc_ip}:{insupc_port}")
            time.sleep(0.1)

    return

def funcTestStart():
    global cycleListClientInfo
    global listClientInfo
    config_dict = config.fnGetConfig()

    if config_dict :
        insupc_ip1 = config_dict['insupc_ip1']
        #insupc_ip2 = config_dict['insupc_ip2']
        insupc_port1 = config_dict['insupc_port1']
        #insupc_port2 = config_dict['insupc_port2']

        cycleListClientInfo = itertools.cycle(listClientInfo)

        # client 동작을 위한 스레드를 생성한다.
        handle_thread1 = threading.Thread(target=client_manager, args=(insupc_ip1, insupc_port1, ))
        handle_thread1.start() 
        #handle_thread2 = threading.Thread(target=client_manager, args=(insupc_ip2, insupc_port2, ))
        #handle_thread2.start()
    else :
        logging.error("Config Load Error...!!! config_dict null.")
    return

if __name__ == '__main__':
    import os
    config.fnLoadConfig()
    dictConfig = config.fnGetConfig()
    log_name="insup_tcp_client"
    log_path = "./"
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
    
    funcTestStart()
    while True:
        time.sleep(1)
        # test.
        #worker.fnQueuePut(b"put -> main test message")
