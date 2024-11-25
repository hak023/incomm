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

def setup_logging(log_path, log_name, max_size_mb=300):
    current_date = datetime.date.today()
    log_count = 1
    
    def get_log_file_path():
        return os.path.join(log_path, f"{log_name}.{current_date}.{log_count}")
    
    def check_log_size(file_path):
        if os.path.exists(file_path):
            size_mb = os.path.getsize(file_path) / (1024 * 1024)  # Convert to MB
            return size_mb > max_size_mb
        return False
    
    def get_next_log_file():
        nonlocal log_count
        while check_log_size(get_log_file_path()):
            log_count += 1
        return get_log_file_path()
    
    class DailyRotatingHandler(logging.FileHandler):
        def __init__(self, filename, mode='a', encoding=None):
            self.base_filename = filename
            super().__init__(filename, mode, encoding)
            
        def emit(self, record):
            try:
                if self.check_new_day() or check_log_size(self.baseFilename):
                    self.baseFilename = get_next_log_file()
                    if self.stream:
                        self.stream.close()
                    self.stream = self._open()
                logging.FileHandler.emit(self, record)
            except Exception:
                self.handleError(record)
                
        def check_new_day(self):
            nonlocal current_date
            today = datetime.date.today()
            if today != current_date:
                current_date = today
                global log_count
                log_count = 1
                return True
            return False
    
    if not os.path.exists(log_path):
        os.makedirs(log_path)
        
    log_file = get_next_log_file()
    handler = DailyRotatingHandler(log_file)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # 기존 핸들러 제거
    for hdlr in logger.handlers[:]:
        logger.removeHandler(hdlr)
    
    logger.addHandler(handler)
    return logger

def main():
    config.fnLoadConfig()
    dictConfig = config.fnGetConfig()
    log_name = dictConfig['log_name']
    log_path = dictConfig['log_path']
    
    # 로깅 설정
    logger = setup_logging(log_path, log_name)
    logger.info("Process Start %s", log_name)
    
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
