import itertools
import json
import logging
from queue import Queue
import queue
import threading

from insupc_tcp_client import funcMakeQueryMessage, funcSendTcp, getClientSockets
import sip_svc_tcp_server

# 전역변수 worker queue.
#queue = Queue()
queues = []
threads = []
queue_cycle = None

# Lock 객체 생성
lock = threading.Lock()

def Worker():
    logging.info("worker make.")
    return

# 프로그램 시작시 main에서 호출할 function.
def fnStart(nQthreadNumber): 
    global queues
    global threads
    global queue_cycle

    for i in range(nQthreadNumber):
        # 큐 생성
        queue = Queue()
        queues.append(queue)

        # 스레드 생성 및 시작
        thread = threading.Thread(target=process_queue, args=(queue, i,))
        thread.start()
        # 스레드 관리 목록에 넣어두자.
        threads.append(thread)

    logging.info(f"Load Queue Thread Count : {len(queues)}")  # 수정: count -> len(queues)

    # queues 리스트를 순환하는 순환자 생성
    queue_cycle = itertools.cycle(queues)

    # # 모든 큐 작업이 완료될 때까지 대기
    # for queue in queues:
    #     queue.join()

    # # 모든 스레드 종료 대기
    # for thread in threads:
    #     thread.join()
    
    return

# queue를 순차적으로 사용하도록 하는 function. queue에 put할 때 사용된다.
def fnGetNextQueue():
    global queue_cycle
    with lock:  # lock을 사용하여 크리티컬 섹션 보호
        # 다음 순환 요소를 가져옴
        queue = next(queue_cycle)
    return queue

# worker queue를 이용할때 외부에서 호출하게 되는 함수. queue put 을 담당한다. 
def fnQueuePut(strMessage):
    logging.info(f"[Put -> WorkerQ] {strMessage}")
    # fnGetNextQueue 함수에서 순차적으로 queue를 리턴하여 선정하자.
    queue = fnGetNextQueue()
    # 큐에 테스트용 데이터 집어넣기
    queue.put(strMessage)
    queue.join()
    
    return

# thread 돌때 처리하는 함수. while 돌면서 계속 queue get 처리해서 fnWorkerProcess를 호출한다.
def process_queue(queue, nQueueThreadNumber):
    while True:
        if queue is None:
            print("queue is none")
            break
            
        strMessage = queue.get()
        fnWorkerProcess(strMessage, nQueueThreadNumber)
        # 큐에서의 작업 완료 여부를 알 수 있게 알려주자. 무한대기에 빠지지 않게.
        queue.task_done()
    return

# worker queue에서 처리해야할 내용을 이 함수에서 처리한다.
def fnWorkerProcess(strMessage, nQueueThreadNumber):
    strLogPrefix = "[WorkerThread-" + str(nQueueThreadNumber) + "]"
    logging.info(f"{strLogPrefix} [WorkerQ -> Get] {strMessage}")

    dictQueueMessage = json.loads(strMessage)
    dictClientInfo = dictQueueMessage["client_info"]
    # 이렇게 하면 실제로 변환이 안된다. recvmessage가 json형태기 때문에.
    # dictRecvMessage = dictQueueMessage["recv_message"]
    # 따라서 아래처럼 한번 더 json 을 loads함.
    dictRecvMessage = json.loads(dictQueueMessage["recv_message"])
    
    strClientIp = dictClientInfo["client_ip"]       # incomm에 접속하고 있는 client_ip, 즉 sipsvc
    nClientPort = dictClientInfo["client_port"]     # incomm에 접속하고 있는 client_port, 즉 sipsvc

    # dictRecvMessage 메시지를 parsing하자.
    nSequenceNumber = dictRecvMessage["seq"]
    nTrasactionId = dictRecvMessage["reqNo"]
    dictReqBody = dictRecvMessage["reqBody"]
    #strMdnNumber = dictReqBody["mdn"] ## 이거 협의 안되었음. 용님과 협의해서 입력하자.
    listInputParams = dictReqBody["inputParams"]
    strMdnNumber = listInputParams[0] # 첫번째 값이 mdn이다. 연동문서로 협의함.
    
    
    # 받은 메시지를 기반으로 insupc를 조회해야 한다.
    # dictRecvMessage 에서 mdn을 추출하자.
    byteQueryMessage = funcMakeQueryMessage(nClientPort, strMdnNumber, nTrasactionId)  # asid를 부여해야하나 임시로 nClientPort를 사용함. 추후 asid 관리해야함. 앞단 서버가 n개가 될 경우.
    # 메시지를 만들자
    client_socket = getClientSockets()
    if (client_socket) :
        funcSendTcp(client_socket, byteQueryMessage)
    else :
        logging.error(f"error. not found client_socket by workerprocess. ")

    return
 
# 테스트가 필요할때 쓰는 function.
def test():
    fnQueuePut("hello")
    logging.info("test. put queue data -> hello.")
    
    return

# main 으로 실행될때. 즉, 단위테스트할때 사용하자.
if __name__ == '__main__':
    fnStart(5)
    test()