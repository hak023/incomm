import datetime
import errno
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
def funcSendTcp(client_socket, byteMessage) :
    with lockSocket:
        try:
            strPeerInfo = client_socket.getpeername()
            strLocalInfo = client_socket.getsockname()
            logging.info(f"send tcp log. {strLocalInfo} ->{strPeerInfo} {byteMessage}")
            client_socket.sendall(byteMessage)
            return True  # 성공적으로 전송됨
        except Exception as e:
            logging.error(f"Failed to send TCP message: {str(e)}")
            return False  # 전송 실패

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
    nBodyMessageLength = 0
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

    # 아래서 하자. body size length 계산해야됨.
    #byteHeaderMessage = struct.pack('>HBBBB30s4sB17sI', nBodyMessageLength, nMsgCode, byteSvca, byteDvca, 
    #                                nAsId, strSessionId.encode(), strSvcId.encode(), nResult, strWtime.encode(), nDummy)

    ################## parameter ####################
    # nParamCount = 1 / 1byte / param갯수. 2개.
    nParamCount = 2

    # nParamType = 1 / 1byte / 1=logon_info, 2=operation_name, 3=sql_input(mdn)
    nParam1Type = 2
    # nParamLength = 2byte 
    nParam1Length = 0
    # pcap 확인해보니 알수없는 1byte에 18이 적혀있더라...
    nUnknownValue = 18 
    # 쿼리를 위한 API 문자열을 사용하자.
    strParam1Value = "mcidPstnGetInfoV2"
    nParam1Length = len(strParam1Value) # strApiValue의 length값.

    # nParamType = 1 / 1byte / 1=logon_info, 2=operation_name, 3=sql_input(mdn)
    nParam2Type = 3
    # nParamLength = 2byte 
    nParam2Length = 0
    #pcap 보고 규격맞춰줌... 4byte
    nUnknownParam2Value1 = 12
    nUnknownParam2Value2 = 1
    nUnknownParam2Value3 = 0
    nUnknownParam2Value4 = 9

    # 쿼리를 위한 API 문자열을 사용하자.
    #strParam2Value = "111112222"
    strParam2Value = strMdnNumber
    nParam2Length = len(strParam2Value) # strApiValue의 length값.

    # 패킹
    byteBodyMessage = struct.pack(f'!BBHB{nParam1Length}sBBBBBB{nParam2Length}s',
                                nParamCount,
                                nParam1Type, nUnknownValue, nParam1Length, strParam1Value.encode(),
                                nParam2Type, nUnknownParam2Value3, nUnknownParam2Value1, nUnknownParam2Value2, 
                                nUnknownParam2Value3, nParam2Length, strParam2Value.encode())
    
    nBodyMessageLength = len(byteBodyMessage)
    byteHeaderMessage = struct.pack('>HBBBB30s4sB17sI', nBodyMessageLength, nMsgCode, byteSvca, byteDvca, 
                                nAsId, strSessionId.encode(), strSvcId.encode(), nResult, strWtime.encode(), nDummy)
    byteQueryMessage = byteHeaderMessage + byteBodyMessage
    #logging.info(f"[send] incomm -> insupc Header[nBodyMessageLength({nBodyMessageLength}), nMsgCode({nMsgCode}), byteSvca({byteSvca}), byteDvca({byteDvca}), nAsId({nAsId}), strSessionId({strSessionId}), strSvcId({strSvcId}), nResult({nResult}), strWtime({strWtime}), nDummy({nDummy})], Body[TYPE({nParam1Type}:{strParam1Value}, MDN({nParam1Type}:{strParam1Value})]")
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


    return nMsgCode, nResult, nAsId, nBodyMessageLength

# insupc에 query한 후 response를 수신한 data의 body를 decode하기 위한 함수. data는 전송받은 그로 넣으면 header 62byte를 제외하고 parsing한다.
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

    #241120 by hak. 아래 일단 임시 주석검. 임시 return data 세팅.
    '''
    # 33 번째 byte에서 2byte로 표현된 mdn length를 unpack
    unpacked_mdn_length = struct.unpack('>H', data[31:32])
    mdn_length = unpacked_mdn_length[0]
    # mdn_length를 로그로 남기자.
    logging.info(f"decoding mdn length : {mdn_length}")

    # 35번째 byte부터 mdn_length만큼 unpack하여 전화번호 추출
    unpacked_mdn_number = struct.unpack(f'>{mdn_length}s', data[33:33+mdn_length])
    
    str_mdn_number = unpacked_mdn_number[0].decode()

    logging.info(f"decoding mdn number : {str_mdn_number}")
    '''
    str_mdn_number = "025671033"

    return str_mdn_number

# json string 메시지를 argument로 받아서 query 결과를 response 메시지를 만들자. 
def funcMakeQueryResponseMessage(strTransactionId, nSeq, nResult, data):
    # data가 있으면 strMdnNumber를 추출하자.
    strMdnNumber = None
    if data:
        logging.info(f"insupc query result({nResult}), response ({data})")
        strMdnNumber = funcDecodeQueryMessage(data)
    # data가 없으면 strMdnNumber는 ""이다.
    else:
        strMdnNumber = "0"   

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
# 호출할때마다 연결되어있는 socket을 리턴한다. 가용가능 socket이 없으면 None 을 리턴함.
def getClientSockets():
    global listClientInfo
    global cycleListClientInfo
    
    if not listClientInfo:  # 리스트가 비어있으면
        logging.error("listClientInfo is empty")
        return None
        
    try:
        # 연결된 소켓 찾기
        for client in listClientInfo:
            if client.get("connected", False):
                return client.get("socket")
                
        logging.error("No connected sockets found")
        return None
        
    except StopIteration:
        logging.error("StopIteration in getClientSockets")
        return None
    except Exception as e:
        logging.error(f"Error in getClientSockets: {str(e)}")
        return None

def recv_manager(client_socket):
    try:
        # 1. 먼저 header 62 bytes를 받는다
        client_socket.settimeout(1.0)  # 1초 타임아웃 설정
        header_data = client_socket.recv(62)
        if not header_data or len(header_data) != 62:
            logging.error(f"Failed to receive complete header data. Received {len(header_data) if header_data else 0} bytes")
            return False
        #else:
        #    logging.info(f"Received header data: {header_data}, length: {len(header_data)}")

        try:
            # 2. header 정보를 해석하고 body length도 받아온다
            nMsgCode, nResult, nAsId, nBodyMessageLength = funcDecodeHeaderMessage(header_data, client_socket)
        except struct.error as e:
            logging.error(f"Failed to decode header: {str(e)}")
            return False
        except Exception as e:
            logging.error(f"Exception Failed to decode header: {str(e)}")
            return False

        # 3. body message가 있으면 추가 데이터를 수신함.
        body_data = None
        if nBodyMessageLength > 0:
            try:
                body_data = b'' # 초기화
                remaining = nBodyMessageLength # 남은 데이터 길이
                while remaining > 0: # 데이터가 남아있으면 계속 수신함.
                    chunk = client_socket.recv(remaining)
                    if not chunk:
                        logging.error("Connection closed while receiving body data")
                        return False 
                    body_data += chunk
                    remaining -= len(chunk) # 남은 데이터 길이 갱신

            except socket.error as e:
                logging.error(f"Socket error while receiving body: {str(e)}")
                return False

        # 4. ping 또는 access 응답은 처리하지 않음
        if (nMsgCode == 4 or nMsgCode == 6):
            # body data length가 얼마인지만 로그로 남기자.
            logging.info(f"access or heartbeat body data length: {nBodyMessageLength}")
            return True
        
        # nMsgCode == 9 인 경우는 (뭔지 모름.) 처리하지 않음.
        if nMsgCode == 9:
            logging.info(f"unknown message code: {nMsgCode}")
            return True

        # 5. query response 처리
        elif nMsgCode == 2:
            # log를 남기자. process query response start.
            #logging.info(f"process query response start.")  
            conn = sip_svc_tcp_server.funcGetConnection(nAsId)
            if conn:
                try:
                    strResponseMessage = funcMakeQueryResponseMessage(nAsId, nAsId, nResult, body_data)
                    nMessageSize = len(strResponseMessage)
                    strTotalMessage = "(" + str(nMessageSize).zfill(8) + ")" + strResponseMessage
                    strLocalInfo, strPeerInfo = sip_svc_tcp_server.funcGetConnectionInfo(conn)
                    logging.info(f"[{strPeerInfo}<-{strLocalInfo}] Length:{nMessageSize} Message:{strTotalMessage}")
                    conn.send(strTotalMessage.encode())
                except Exception as e:
                    logging.error(f"Error processing query response: {str(e)}")
            else:
                logging.error(f"Connection not found for AsId: {nAsId}")
        else:
            logging.error(f"Unknown Message Code: {nMsgCode}")

    except socket.timeout:
        logging.info("No data received within timeout period")
        return True  # 타임아웃은 정상적인 상황으로 처리
    except Exception as e:
        logging.error(f"recv_data socket error: {str(e)}")
        return False  # 소켓을 직접 닫지 않고 False만 반환

    return True

def client_manager(insupc_ip, insupc_port):
    global listClientInfo
    while True:  # 외부 루프
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.setblocking(True)  # Blocking 모드로 설정
            
            bConnected = False
            dictClientInfo = {
                "socket": client_socket,
                "connected": bConnected
            }
            
            with lockClientInfo:
                listClientInfo = [info for info in listClientInfo if info["socket"] != client_socket]
                listClientInfo.append(dictClientInfo)

            last_heartbeat_time = time.time()
            nSystemId = random.randint(0, 255)

            while True:  # 내부 루프
                if not bConnected:
                    try:
                        client_socket.connect((insupc_ip, insupc_port))
                        bConnected = True
                        with lockClientInfo:
                            dictClientInfo["connected"] = bConnected
                            # cycleListClientInfo 재초기화
                            global cycleListClientInfo
                            cycleListClientInfo = itertools.cycle(listClientInfo)
                        logging.info(f"Connected to {insupc_ip}:{insupc_port}")
                        
                        time.sleep(0.1)
                        byteAccessMessage = funcMakeAccessMessage(nSystemId)
                        funcSendTcp(client_socket, byteAccessMessage)
                    except Exception as e:
                        logging.error(f"Connection failed: {str(e)}")
                        time.sleep(1)  # 연결 실패시 대기
                        continue
                else:
                    # heartbeat 체크
                    current_time = time.time()
                    if current_time - last_heartbeat_time >= 10:
                        last_heartbeat_time = current_time
                        try:
                            byteHeartBeatMessage = funcMakeHearBeatMessage(nSystemId)
                            bResult = funcSendTcp(client_socket, byteHeartBeatMessage)    
                            if bResult  == False:
                                logging.error(f"send heartbeat error: {bResult}")

                            # send 결과를 남기자.
                            logging.info(f"[{client_socket.getsockname()[0]}:{client_socket.getsockname()[1]}->{client_socket.getpeername()[0]}:{client_socket.getpeername()[1]}] Send Heartbeat Message")
                        except Exception as e:
                            logging.error(f"Failed to send heartbeat: {str(e)}")
                            
                    # return 결과는 확인할 필요 없다. false가 발생한다면 다시 while문으로 돌아가서 모든 처리를 점검하도록 프로그램 되어있음.
                    if not recv_manager(client_socket):
                        logging.error("Connection lost - attempting reconnect")
                        # exception 발생하도록 하자.
                        raise Exception("Connection lost - attempting reconnect")
                        continue

                time.sleep(0.0001)  # CPU 사용률 감소
        except Exception as e:
            logging.error(f"Connection error socket close(): {str(e)} for {insupc_ip}:{insupc_port}")
            try:
                
                bConnected = False
                with lockClientInfo:
                    dictClientInfo["connected"] = False
                    listClientInfo = [info for info in listClientInfo if info["socket"] != client_socket]
                    listClientInfo.remove(dictClientInfo)
                with lockSocket:
                    client_socket.close()
            except:
                pass
            
            time.sleep(1)  # 재연결 시도 전 대기
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
