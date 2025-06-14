from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import subprocess
import json
from pathlib import Path
from typing import Optional
import pika
import threading
import logging
import ipfshttpclient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

class Task(BaseModel):
    id: int
    name: str
    command: str
    depends_on: Optional[list[int]] = None

class TaskResult(BaseModel):
    task_id: int
    status: str
    result_cid: Optional[str] = None
    metadata_cid: Optional[str] = None
    error: Optional[str] = None

def execute_task_internal(task: Task) -> TaskResult:
    try:
        result_dir = Path("results")
        result_dir.mkdir(exist_ok=True)
        script_dir = Path("scripts")
        script_dir.mkdir(exist_ok=True)

        result = subprocess.run(
            task.command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=30
        )

        status = "completed" if result.returncode == 0 else "failed"
        error = result.stderr if result.returncode != 0 else None
        result_cid = None
        metadata_cid = None

        if status == "completed":
            result_file = result_dir / f"task_{task.id}.json"
            output_file = result_dir / f"output.txt" if task.id == 1 else result_dir / f"processed.txt"
            result_data = {
                "task_id": task.id,
                "name": task.name,
                "status": status,
                "output": result.stdout,
                "result_path": str(output_file)
            }
            with result_file.open("w") as f:
                json.dump(result_data, f, indent=2)
            
            with ipfshttpclient.connect() as client:
                result_cid = client.add(str(output_file))["Hash"]
                metadata_cid = client.add(str(result_file))["Hash"]

        return TaskResult(
            task_id=task.id,
            status=status,
            result_cid = result_cid,
            metadata_cid = metadata_cid,
            error=error
        )

    except subprocess.TimeoutExpired:
        return TaskResult(task_id=task.id, status="failed", error="Task timed out")
    except Exception as e:
        return TaskResult(task_id=task.id, status="failed", error=str(e))

@app.post("/execute", response_model=TaskResult)
async def execute_task(task: Task):
    try:
        result = execute_task_internal(task)
        if result.status == "failed":
            raise HTTPException(status_code=500, detail=f"Task {task.id} failed: {result.error}")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Task {task.id} failed: {str(e)}")

def consume_tasks():
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host="127.0.0.1", port=5672)
        )
        channel = connection.channel()

        channel.queue_declare(queue="task_queue", durable=True)
        channel.queue_declare(queue="completion_queue", durable=True)

        def callback(ch, method, properties, body):
            task_data = json.loads(body)
            task = Task(**task_data)
            logger.info(f"Received task: {task.id} - {task.name}")

            result = execute_task_internal(task)
            logger.info(f"Task {task.id} result: {result.status}")

            channel.basic_publish(
                exchange="",
                routing_key="completion_queue",
                body=json.dumps(result.model_dump()),
                properties=pika.BasicProperties(delivery_mode=2)
            )

            ch.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue="task_queue", on_message_callback=callback)
        logger.info("Node started, waiting for tasks...")
        channel.start_consuming()

    except Exception as e:
        logger.error(f"Error in consumer: {e}")
    finally:
        if "connection" in locals():
            connection.close()

if __name__ == "__main__":
    consumer_thread = threading.Thread(target=consume_tasks, daemon=True)
    consumer_thread.start()

    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)