import json
from pathlib import Path
import pika
import logging
from workflow_parser import parse_workflow, Workflow, Task
from typing import Dict, Set
import ipfshttpclient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Scheduler:
    def __init__(self):
        self.workflow: Workflow = None
        self.pending_tasks: Dict[int, Task] = {}
        self.completed_tasks: set[int] = set()
        self.connection = None
        self.channel = None
        self.results: Dict[int, dict] = {}

    def load_workflow(self, workflow_file: str):
        try:
            with Path(workflow_file).open("r") as f:
                workflow_json = f.read()
            self.workflow = parse_workflow(workflow_json)
            logger.info(f"Loaded workflow: {self.workflow.workflow_id}")

            for task in self.workflow.tasks:
                self.pending_tasks[task.id] = task
        except Exception as e:
            logger.error(f"Failed to load workflow: {e}")
            raise

    def connect_rabbitmq(self):
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host="127.0.0.1", port=5672)
            )
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue="task_queue", durable=True)
            self.channel.queue_declare(queue="completion_queue", durable=True)
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise

    def queue_ready_tasks(self):
        for task_id, task in list(self.pending_tasks.items()):
            dependencies_met = True
            if task.depends_on:
                for dep_id in task.depends_on:
                    if dep_id not in self.completed_tasks:
                        dependencies_met = False
                        break
            if dependencies_met:
                self.channel.basic_publish(
                    exchange="",
                    routing_key="task_queue",
                    body=json.dumps(task.model_dump()),
                    properties=pika.BasicProperties(delivery_mode=2)
                )
                logger.info(f"Queued task: {task.id} - {task.name}")
                del self.pending_tasks[task_id]

    def handle_completion(self, ch, method, properties, body):
        try:
            result = json.loads(body)
            task_id = result["task_id"]
            status = result["status"]
            logger.info(f"Task {task_id} completed with status: {status}")

            if status == "completed":
                self.results[task_id] = {
                    "result_cid": result["result_cid"],
                    "metadata_cid": result["metadata_cid"]
                }
                with ipfshttpclient.connect() as client:
                    result_content = client.cat(result["result_cid"]).decode("utf-8")
                    metadata_content = json.loads(client.cat(result["metadata_cid"]).decode("utf-8"))
                    logger.info(f"Task {task_id} result content: {result_content}")
                    logger.info(f"Task {task_id} metadata: {metadata_content}")

                self.completed_tasks.add(task_id)
                self.queue_ready_tasks()
            else:
                logger.error(f"Task {task_id} failed: {result['error']}")
                self.connection.close()
                raise RuntimeError(f"Task {task_id} failed")

            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logger.error(f"Error handling completion: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def run(self, workflow_file: str):
        try:
            self.load_workflow(workflow_file)
            self.connect_rabbitmq()
            self.queue_ready_tasks()
            self.channel.basic_consume(queue="completion_queue", on_message_callback=self.handle_completion)
            logger.info("Scheduler started, waiting for task completions...")
            self.channel.start_consuming()
        except Exception as e:
            logger.error(f"Scheduler error: {e}")
            raise
        finally:
            if self.connection and not self.connection.is_closed:
                self.connection.close()

if __name__ == "__main__":
    scheduler = Scheduler()
    scheduler.run("workflow.json")