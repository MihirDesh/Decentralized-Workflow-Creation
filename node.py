from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import subprocess
import json
from pathlib import Path
from typing import Optional

app = FastAPI()

class Task(BaseModel):
    id: int
    name: str
    command: str
    depends_on: Optional[list[int]] = None

class TaskResult(BaseModel):
    task_id: int
    status: str
    result_path: Optional[str] = None
    error: Optional[str] = None

@app.post("/execute", response_model = TaskResult)
async def execute_task(task: Task):
    try:
        result = subprocess.run(
            task.command,
            shell = True,
            capture_output = True,
            text = True,
            timeout = 30
        )

        status = "completed" if result.returncode == 0 else "failed"
        error = result.stderr if result.returncode != 0 else None
        result_path = None

        if status == "completed":
            result_dir = Path("results")
            result_dir.mkdir(exist_ok = True)
            result_file = result_dir / f"task_{task.id}.json"
            result_data = {
                "task_id": task.id,
                "name": task.name,
                "status": status,
                "output": result.stdout,
                "result_path": str(result_dir / f"output.txt") 
            }
            with result_file.open("w") as f:
                json.dump(result_data, f, indent=2)
            result_path = str(result_file)

            return TaskResult(
                task_id = task.id,
                status = status,
                result_path = result_path,
                error = error
            )
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=500, detail=f"Task {task.id} timed out")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Task {task.id} failed: {str(e)}")
    
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)    