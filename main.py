import configparser
import datetime
import logging
import os
import config
import worker
import time
import sip_svc_tcp_server
import insupc_tcp_client

"""
global cdr_logging
# cdr log를 위한 instance를 return한다. cdr_logging.info("log") 이렇게 사용하면 됨.
def getCdrLogInstance():
    # CDR을 위한 로그 생성
    return cdr_logging
"""

def main():
    config.fnLoadConfig()
    dictConfig = config.fnGetConfig()
    log_name = dictConfig['log_name']
    log_path = dictConfig['log_path']

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

    """
    # CDR 로그생성.
    global cdr_logging
    cdr_log_path = os.path.join(log_path, "cdr" + '.' + str(current_date))
    cdr_logging = logging.getLogger("cdr_logging")
    cdr_log_handler = logging.FileHandler(cdr_log_path)
    cdr_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    cdr_log_handler.setFormatter(cdr_formatter)
    cdr_logging.addHandler(cdr_log_handler)
    getCdrLogInstance(log_path, current_date)
    cdr_logging.info("Process Start %s", log_name)
    """

    # worker setting
    worker_number = dictConfig['worker_number']
    worker.fnStart(int(worker_number))

    # insupc tcp client setting.
    insupc_tcp_client.funcStart()

    # 내부 처리가 가능한 상태가 된 후에 호 인입이 되는 sip_svc 부분을 0.1초 기다렸다 로딩함.
    time.sleep(0.1)

    # sip_svc_tcp_server setting
    tcpserver_ip = dictConfig['tcpserver_ip']
    tcpserver_port = dictConfig['tcpserver_port']
    tcpserver_maxnum = dictConfig['tcpserver_maxnum']
    sip_svc_tcp_server.fnStart(tcpserver_ip, tcpserver_port, tcpserver_maxnum)


    
    while True:
        time.sleep(1)
        # test.
        #worker.fnQueuePut(b"put -> main test message")

    return

if __name__ == '__main__':
    main()
