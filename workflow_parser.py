from pydantic import BaseModel, Field, field_validator
from typing import List, Optional
import json
from pathlib import Path

class Task(BaseModel):
    id: int = Field(..., gt = 0)
    name: str = Field(..., min_length = 1)
    command: str = Field(..., min_length = 1)
    depends_on: Optional[List[int]] = None

class Workflow(BaseModel):
    workflow_id: str = Field(..., min_length = 1)
    tasks: List[Task] = Field(..., min_length = 1)

    @field_validator("tasks")
    @classmethod
    def validate_tasks(cls, tasks):
        task_ids = [task.id for task in tasks]
        if len(task_ids) != len(set(task_ids)):
            raise ValueError("Duplicate task IDs found")
        
        for task in tasks:
            if task.depends_on:
                for dep_id in task.depends_on:
                    if dep_id not in task_ids:
                        raise ValueError(f"Task {task.id} depends on invalid task ID {dep_id}")
                    
        def has_cycle(task_id, visited, rec_stack):
            visited.add(task_id)
            rec_stack.add(task_id)
            for task in tasks:
                if task.id == task_id and task.depends_on:
                    for dep_id in task.depends_on:
                        if dep_id not in visited:
                            if has_cycle(dep_id, visited, rec_stack):
                                return True
                        elif dep_id in rec_stack:
                            return True
            rec_stack.remove(task_id)
            return False
        
        visited = set()
        rec_stack = set()
        for task in tasks:
            if task.id not in visited:
                if has_cycle(task.id, visited, rec_stack):
                    raise ValueError("Circular dependency detected")
        return tasks
    
def parse_workflow(json_input : str) -> Workflow:
    try:
        data = json.loads(json_input)
        return Workflow.model_validate(data)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON: {e}")
    except Exception as e:
        raise ValueError(f"Validation error: {e}")
    
if __name__ == "__main__":
    workflow_file = Path("workflow.json")
    try:
        with workflow_file.open("r") as f:
            workflow_json = f.read()
        workflow = parse_workflow(workflow_json)
        print("Parsed Workflow:")
        print(f"Workflow ID: {workflow.workflow_id}")
        for task in workflow.tasks:
            print(f"Task {task.id}: {task.name}, Command: {task.command}, Depends on: {task.depends_on}")
    except ValueError as e:
        print(f"Error: {e}")
    except FileNotFoundError:
        print(f"Error: {workflow_file} not found")