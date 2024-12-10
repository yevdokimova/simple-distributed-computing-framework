import socket
import threading
import json
import time
from typing import List, Dict, Optional
from common.serialization import SerializationUtil
from common.task import Task, JoinTask, DistinctTask, GroupCountTask, GroupSumTask, OrderByTask
from common.result import Result
from common.dataframe import DataFrame
from common.logger import setup_logger

HEARTBEAT_INTERVAL = 5  # seconds
HEARTBEAT_TIMEOUT = 15  # seconds

logger = setup_logger('MasterServer')

class WorkerHandler:
    def __init__(self, worker_id: str, conn: socket.socket, addr):
        self.worker_id = worker_id
        self.conn = conn
        self.addr = addr
        self.last_heartbeat = time.time()
        self.is_busy = False
        self.lock = threading.Lock()

    def send_task(self, task: Task):
        try:
            serialized_task = SerializationUtil.serialize_task(task)
            message = {
                "type": "task",
                "task": json.loads(serialized_task)
            }
            self.conn.sendall((json.dumps(message) + "\n").encode())
            self.is_busy = True
            logger.info(f"Sent Task {task.task_id} to Worker {self.worker_id}")
        except Exception as e:
            logger.error(f"Failed to send Task to Worker {self.worker_id}: {e}")
            self.conn.close()

    def update_heartbeat(self):
        self.last_heartbeat = time.time()

class MasterServer:
    def __init__(self, host='0.0.0.0', port=5000):
        self.host = host
        self.port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.workers: List[WorkerHandler] = []
        self.workers_lock = threading.Lock()
        self.task_queue: List[Task] = []
        self.task_queue_lock = threading.Lock()
        self.round_robin_index = 0
        self.round_robin_lock = threading.Lock()
        self.client_sockets: Dict[str, socket.socket] = {}
        self.results: Dict[str, Result] = {}
        self.results_lock = threading.Lock()
        self.heartbeat_thread = threading.Thread(target=self.monitor_heartbeats, daemon=True)
        self.dispatcher_thread = threading.Thread(target=self.dispatch_tasks, daemon=True)

    def start(self):
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen()
        logger.info(f"Master Server started on {self.host}:{self.port}")

        self.heartbeat_thread.start()
        self.dispatcher_thread.start()

        while True:
            conn, addr = self.server_socket.accept()
            threading.Thread(target=self.handle_connection, args=(conn, addr), daemon=True).start()

    def handle_connection(self, conn: socket.socket, addr):
        with conn:
            buffer = ""
            while True:
                try:
                    data = conn.recv(1024).decode()
                    if not data:
                        logger.info(f"Connection closed by {addr}")
                        break
                    buffer += data
                    while '\n' in buffer:
                        line, buffer = buffer.split('\n', 1)
                        self.process_message(line, conn)
                except Exception as e:
                    logger.error(f"Error handling connection from {addr}: {e}")
                    break

    def process_message(self, message: str, conn: socket.socket):
        try:
            msg = json.loads(message)
            msg_type = msg.get("type")
            if msg_type == "register_worker":
                worker_id = msg.get("worker_id")
                with self.workers_lock:
                    worker = WorkerHandler(worker_id, conn, conn.getpeername())
                    self.workers.append(worker)
                logger.info(f"Registered Worker {worker_id} from {conn.getpeername()}")
            elif msg_type == "heartbeat":
                worker_id = msg.get("worker_id")
                with self.workers_lock:
                    for worker in self.workers:
                        if worker.worker_id == worker_id:
                            worker.update_heartbeat()
                            logger.debug(f"Heartbeat received from Worker {worker_id}")
                            break
            elif msg_type == "submit_task":
                task_data = msg.get("task")
                task = SerializationUtil.deserialize_task(json.dumps(task_data))
                with self.task_queue_lock:
                    self.task_queue.append(task)
                client_socket = conn
                self.client_sockets[task.task_id] = client_socket
                logger.info(f"Received Task {task.task_id} of type {task.operation} from Client")
            elif msg_type == "result":
                task_id = msg.get("task_id")
                success = msg.get("success")
                if success:
                    data_frame = DataFrame.from_json(json.dumps(msg.get("data_frame")))
                    result = Result(task_id, data_frame, True)
                    logger.info(f"Received successful Result for Task {task_id} from Worker")
                else:
                    error_message = msg.get("error_message")
                    result = Result(task_id, None, False, error_message)
                    logger.warning(f"Task {task_id} failed with error: {error_message}")

                with self.results_lock:
                    self.results[task_id] = result

                client_socket = self.client_sockets.get(task_id)
                if client_socket:
                    response = {
                        "task_id": task_id,
                        "success": success,
                        "data_frame": data_frame.rows if success else None,
                        "error_message": msg.get("error_message") if not success else None
                    }
                    try:
                        client_socket.sendall((json.dumps(response) + "\n").encode())
                        logger.info(f"Sent Result for Task {task_id} to Client")
                    except Exception as e:
                        logger.error(f"Failed to send Result to Client for Task {task_id}: {e}")
                    finally:
                        del self.client_sockets[task_id]
            else:
                logger.warning(f"Unknown message type: {msg_type}")
        except json.JSONDecodeError:
            logger.error("Failed to decode JSON message")
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def dispatch_tasks(self):
        while True:
            with self.task_queue_lock:
                if not self.task_queue:
                    time.sleep(1)
                    continue
                task = self.task_queue.pop(0)
            worker = self.get_next_available_worker()
            if worker:
                worker.send_task(task)
                serialized_task = SerializationUtil.serialize_task(task)
                logger.debug(f"Sending Task {task.task_id} to Worker {worker.worker_id}: {serialized_task}")
            else:
                with self.task_queue_lock:
                    self.task_queue.append(task)
                time.sleep(1)

    def get_next_available_worker(self) -> Optional[WorkerHandler]:
        with self.workers_lock:
            if not self.workers:
                return None
            with self.round_robin_lock:
                for _ in range(len(self.workers)):
                    worker = self.workers[self.round_robin_index % len(self.workers)]
                    self.round_robin_index += 1
                    if not worker.is_busy:
                        return worker
        return None

    def monitor_heartbeats(self):
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            current_time = time.time()
            with self.workers_lock:
                for worker in self.workers[:]:
                    if current_time - worker.last_heartbeat > HEARTBEAT_TIMEOUT:
                        logger.warning(f"Worker {worker.worker_id} timed out. Removing from active Workers.")
                        worker.conn.close()
                        self.workers.remove(worker)

def main():
    master = MasterServer(host='0.0.0.0', port=5000)
    master.start()

if __name__ == "__main__":
    main()
