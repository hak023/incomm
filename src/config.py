import configparser

config_dict = {}

def fnLoadConfig():
    global config_dict

    # config 파일 읽기
    config = configparser.ConfigParser()
    config.read('config.ini')

    # main 섹션에서 값 가져오기
    worker_number = config.getint('main', 'worker_number')
    log_path = config.get('main', 'log_path')
    log_name = config.get('main', 'log_name')

    # insupc 섹션에서 값 가져오기
    insupc_ip1 = config.get('insupc1', 'ip')
    insupc_port1 = config.getint('insupc1', 'port')
    insupc_ip2 = config.get('insupc2', 'ip')
    insupc_port2 = config.getint('insupc2', 'port')

    # tcpserver 섹션에서 값 가져오기
    tcpserver_ip = config.get('tcpserver', 'ip')
    tcpserver_port = config.getint('tcpserver', 'port')
    tcpserver_maxnum = config.getint('tcpserver', 'maxnum')
    
    # dict 자료형에 worker_number와 log_path를 저장하자.
    config_dict = {
        'worker_number': worker_number,
        'log_path': log_path,
        'log_name': log_name,
        'insupc_ip1': insupc_ip1,
        'insupc_port1': insupc_port1,
        'insupc_ip2': insupc_ip2,
        'insupc_port2': insupc_port2,
        'tcpserver_ip': tcpserver_ip,
        'tcpserver_maxnum': tcpserver_maxnum,
        'tcpserver_port': tcpserver_port
    }
    
    return 

def fnGetConfig():
    global config_dict
    return config_dict

def test():
    print("first : " + str(config_dict))
    fnLoadConfig()
    print("second : " + str(config_dict))
    dict = fnGetConfig()

    # config 중 worker_number만 print하려면 예제.
    print("worker_number : " + str(config_dict['worker_number']))

    return

if __name__ == '__main__':
    test()
    