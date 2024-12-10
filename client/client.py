import socket
import threading
import json
import uuid
import time
from typing import Dict
from common.serialization import SerializationUtil
from common.task import Task, JoinTask, DistinctTask, GroupCountTask, GroupSumTask, OrderByTask
from common.result import Result
from common.dataframe import DataFrame
from common.logger import setup_logger

logger = setup_logger('Client')

class Client:
    def __init__(self, master_host='master', master_port=5000):
        self.master_host = master_host
        self.master_port = master_port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.lock = threading.Lock()
        self.running = True
        self.received_results = {}
        self.connected = threading.Event()

    def connect_to_master(self):
        while not self.connected.is_set() and self.running:
            try:
                self.sock.connect((self.master_host, self.master_port))
                logger.info(f"Connected to Master at {self.master_host}:{self.master_port}")
                self.connected.set()
                threading.Thread(target=self.listen_for_results, daemon=True).start()
            except Exception as e:
                logger.error(f"Failed to connect to Master: {e}")
                time.sleep(5)

    def submit_task(self, task: Task):
        if not self.connected.is_set():
            logger.error("Cannot submit task: Not connected to Master Server.")
            return
        message = {
            "type": "submit_task",
            "task": json.loads(SerializationUtil.serialize_task(task))
        }
        self.send_message(message)
        logger.info(f"Submitted Task {task.task_id} of type {task.operation}")

    def listen_for_results(self):
        buffer = ""
        while self.running and self.connected.is_set():
            try:
                data = self.sock.recv(1024).decode()
                if not data:
                    logger.info("Master closed the connection.")
                    self.connected.clear()
                    break
                buffer += data
                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    self.process_message(line)
            except Exception as e:
                logger.error(f"Error receiving data: {e}")
                self.connected.clear()
                break
        self.sock.close()

    def process_message(self, message: str):
        try:
            msg = json.loads(message)
            task_id = msg.get("task_id")
            success = msg.get("success")
            if success:
                data_frame = DataFrame(rows=msg.get("data_frame", []))
                result = Result(task_id, data_frame, True)
                logger.info(f"Result for Task {task_id}:")
                self.display_dataframe(data_frame)
            else:
                error_message = msg.get("error_message")
                result = Result(task_id, None, False, error_message)
                logger.warning(f"Task {task_id} failed with error: {error_message}")
        except json.JSONDecodeError:
            logger.error("Failed to decode JSON message from Master")
        except Exception as e:
            logger.error(f"Error processing message from Master: {e}")

    def send_message(self, message: dict):
        try:
            serialized = json.dumps(message) + "\n"
            with self.lock:
                self.sock.sendall(serialized.encode())
        except Exception as e:
            logger.error(f"Failed to send message: {e}")

    def display_dataframe(self, df: DataFrame):
        rows = df.rows
        if not rows:
            print("No data.")
            return
        columns = set()
        for row in rows:
            columns.update(row.keys())
        columns = sorted(columns)
        print("\n" + "\t".join(columns))
        for row in rows:
            print("\t".join([str(row.get(col, "")) for col in columns]))

    def close(self):
        self.running = False
        self.sock.close()

def create_dataframe():
    rows = []
    print("Enter DataFrame rows (format: column1=value1,column2=value2,...), end with empty line:")
    while True:
        line = input()
        if not line.strip():
            break
        row = {}
        for pair in line.split(','):
            if '=' in pair:
                key, value = pair.split('=', 1)
                row[key.strip()] = value.strip()
        rows.append(row)
    return DataFrame(rows)

def main():
    master_host = 'master'
    master_port = 5000
    client = Client(master_host, master_port)
    client.connect_to_master()

    try:
        while True:
            print("\nSelect Operation:")
            print("1. JOIN")
            print("2. DISTINCT")
            print("3. GROUP BY COUNT")
            print("4. GROUP BY SUM")
            print("5. ORDER BY")
            print("0. Exit")
            choice = input("Enter choice: ").strip()
            if choice == '0':
                break
            elif choice == '1':
                print("Enter DataFrame 1:")
                df1 = create_dataframe()
                print("Enter DataFrame 2:")
                df2 = create_dataframe()
                join_keys = input("Enter join key columns separated by commas: ").strip().split(',')
                join_keys = [key.strip() for key in join_keys]
                task = JoinTask(df1, df2, join_keys)
                client.submit_task(task)
            elif choice == '2':
                print("Enter DataFrame for DISTINCT:")
                df = create_dataframe()
                columns = input("Enter columns to apply DISTINCT on, separated by commas: ").strip().split(',')
                columns = [col.strip() for col in columns]
                task = DistinctTask(df, columns)
                client.submit_task(task)
            elif choice == '3':
                print("Enter DataFrame for GROUP BY COUNT:")
                df = create_dataframe()
                group_by_columns = input("Enter group by columns separated by commas: ").strip().split(',')
                group_by_columns = [col.strip() for col in group_by_columns]
                task = GroupCountTask(df, group_by_columns)
                client.submit_task(task)
            elif choice == '4':
                print("Enter DataFrame for GROUP BY SUM:")
                df = create_dataframe()
                group_by_columns = input("Enter group by columns separated by commas: ").strip().split(',')
                group_by_columns = [col.strip() for col in group_by_columns]
                sum_column = input("Enter the column to sum: ").strip()
                task = GroupSumTask(df, group_by_columns, sum_column)
                client.submit_task(task)
            elif choice == '5':
                print("Enter DataFrame for ORDER BY:")
                df = create_dataframe()
                order_by_columns = input("Enter columns to order by separated by commas: ").strip().split(',')
                order_by_columns = [col.strip() for col in order_by_columns]
                order = input("Enter 'asc' for ascending or 'desc' for descending order: ").strip().lower()
                ascending = True if order == 'asc' else False
                task = OrderByTask(df, order_by_columns, ascending)
                client.submit_task(task)
            else:
                print("Invalid choice. Please try again.")
    except KeyboardInterrupt:
        print("\nExiting Client.")
    finally:
        client.close()

if __name__ == "__main__":
    main()
