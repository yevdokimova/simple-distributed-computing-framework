import socket
import threading
import json
import time
from common.serialization import SerializationUtil
from common.task import Task, JoinTask, DistinctTask, GroupCountTask, GroupSumTask, OrderByTask
from common.result import Result
from common.dataframe import DataFrame
from common.logger import setup_logger
import uuid

HEARTBEAT_INTERVAL = 5  # seconds

logger = setup_logger('WorkerServer')

class WorkerServer:
    def __init__(self, master_host='master', master_port=5000):
        self.master_host = master_host
        self.master_port = master_port
        self.worker_id = str(uuid.uuid4())
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.lock = threading.Lock()
        self.running = True

    def connect_to_master(self):
        attempts = 0
        while attempts < 5 and self.running:
            try:
                self.sock.connect((self.master_host, self.master_port))
                logger.info(f"Connected to Master at {self.master_host}:{self.master_port}")
                self.register_with_master()
                threading.Thread(target=self.send_heartbeats, daemon=True).start()
                self.listen_for_tasks()
                break
            except Exception as e:
                attempts += 1
                logger.error(f"Failed to connect to Master (Attempt {attempts}/5): {e}")
                time.sleep(5)
        else:
            logger.critical("Could not connect to Master after 5 attempts. Exiting.")
            self.stop()

    def register_with_master(self):
        message = {
            "type": "register_worker",
            "worker_id": self.worker_id
        }
        self.send_message(message)
        logger.info(f"Registered with Master as Worker ID: {self.worker_id}")

    def send_heartbeats(self):
        while self.running:
            heartbeat = {
                "type": "heartbeat",
                "worker_id": self.worker_id
            }
            self.send_message(heartbeat)
            time.sleep(HEARTBEAT_INTERVAL)

    def listen_for_tasks(self):
        buffer = ""
        while self.running:
            try:
                data = self.sock.recv(1024).decode()
                if not data:
                    logger.info("Master closed the connection.")
                    break
                buffer += data
                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    self.process_message(line)
            except Exception as e:
                logger.error(f"Error receiving data: {e}")
                break
        self.sock.close()

    def process_message(self, message: str):
        try:
            msg = json.loads(message)
            msg_type = msg.get("type")
            if msg_type == "task":
                task_data = msg.get("task")
                task = SerializationUtil.deserialize_task(json.dumps(task_data))
                logger.debug(f"Received Task {task.task_id} of type {task.operation}")
                threading.Thread(target=self.execute_task, args=(task,), daemon=True).start()
            else:
                logger.warning(f"Unknown message type: {msg_type}")
        except json.JSONDecodeError:
            logger.error("Failed to decode JSON message")
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def execute_task(self, task: Task):
        logger.info(f"Executing Task {task.task_id} of type {task.operation}")
        result = task.execute()
        response = {
            "type": "result",
            "task_id": result.task_id,
            "success": result.success,
        }
        if result.success:
            response["data_frame"] = {
                "rows": result.data_frame.rows
            }
        else:
            response["error_message"] = result.error_message
        self.send_message(response)
        logger.info(f"Task {task.task_id} execution {'succeeded' if result.success else 'failed'}")

    def send_message(self, message: dict):
        try:
            serialized = json.dumps(message) + "\n"
            with self.lock:
                self.sock.sendall(serialized.encode())
        except Exception as e:
            logger.error(f"Failed to send message: {e}")

    def stop(self):
        self.running = False
        self.sock.close()

def main():
    master_host = 'master'
    master_port = 5000
    worker = WorkerServer(master_host, master_port)
    worker.connect_to_master()

if __name__ == "__main__":
    main()
