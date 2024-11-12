import threading
import queue
import time

# 스레드에서 실행될 함수를 정의합니다.
def worker(q):
    while True:
        item = q.get()
        if item is None:
            break
        # 데이터 처리
        print(f'작업 처리: {item}')
        q.task_done()

# 큐를 생성합니다.
q = queue.Queue()

# 스레드를 생성하고 시작합니다.
num_worker_threads = 2
threads = []
for i in range(num_worker_threads):
    t = threading.Thread(target=worker, args=(q,))
    t.start()
    threads.append(t)

# 큐에 데이터를 넣습니다.
for item in range(20):
    q.put(item)

# 모든 작업이 끝날 때까지 기다립니다.
q.join()

# 스레드를 종료합니다.
for i in range(num_worker_threads):
    q.put(None)
for t in threads:
    t.join()
