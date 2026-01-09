# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Dagger is a configuration-driven framework that transforms YAML definitions into Apache Airflow DAGs. It uses dataset lineage (matching inputs/outputs) to automatically build dependency graphs across workflows.

## Common Commands

### Development Setup
```bash
make install-dev      # Create venv, install package in editable mode with dev/test deps
source venv/bin/activate
```

### Testing
```bash
make test             # Run all tests with coverage (sets AIRFLOW_HOME automatically)

# Run a single test file
AIRFLOW_HOME=$(pwd)/tests/fixtures/config_finder/root/ ENV=local pytest -s tests/path/to/test_file.py

# Run a specific test
AIRFLOW_HOME=$(pwd)/tests/fixtures/config_finder/root/ ENV=local pytest -s tests/path/to/test_file.py::test_function_name
```

### Linting
```bash
make lint             # Run flake8 on dagger and tests directories
black dagger tests    # Format code
```

### Local Airflow Testing
```bash
make test-airflow     # Build and start Airflow in Docker (localhost:8080, user: dev_user, pass: dev_user)
make stop-airflow     # Stop Airflow containers
```

### CLI
```bash
dagger --help
dagger list-tasks     # Show available task types
dagger list-ios       # Show available IO types
dagger init-pipeline  # Create a new pipeline.yaml
dagger init-task --type=<task_type>  # Add a task configuration
dagger init-io --type=<io_type>      # Add an IO definition
dagger print-graph    # Visualize dependency graph
```

## Architecture

### Core Flow
1. **ConfigFinder** discovers pipeline directories (each with `pipeline.yaml` + task YAML files)
2. **ConfigProcessor** loads YAML configs with environment variable support
3. **TaskFactory/IOFactory** use reflection to instantiate task/IO objects from YAML
4. **TaskGraph** builds a 3-layer graph: Pipeline → Task → Dataset nodes
5. **DagCreator** traverses the graph and generates Airflow DAGs using **OperatorFactory**

### Key Directories
- `dagger/pipeline/tasks/` - Task type definitions (DbtTask, SparkTask, AthenaTransformTask, etc.)
- `dagger/pipeline/ios/` - IO type definitions (S3, Redshift, Athena, Databricks, etc.)
- `dagger/dag_creator/airflow/operator_creators/` - One creator per task type, translates tasks to Airflow operators
- `dagger/graph/` - Graph construction from task inputs/outputs
- `dagger/config_finder/` - YAML discovery and loading
- `tests/fixtures/config_finder/root/dags/` - Example DAG configurations for testing

### Adding a New Task Type
1. Create task definition in `dagger/pipeline/tasks/` (subclass of Task)
2. Create any needed IOs in `dagger/pipeline/ios/` (if new data sources)
3. Create operator creator in `dagger/dag_creator/airflow/operator_creators/`
4. Register in `dagger/dag_creator/airflow/operator_factory.py`

### Configuration Files
- `pipeline.yaml` - Pipeline metadata (owner, schedule, alerts, airflow_parameters)
- `[taskname].yaml` - Task configs (type, inputs, outputs, task-specific params)
- `dagger_config.yaml` - System config (Neo4j, Elasticsearch, Spark settings)

### Key Patterns
- **Factory Pattern**: TaskFactory/IOFactory auto-discover types via reflection
- **Strategy Pattern**: OperatorCreator subclasses handle task-specific operator creation
- **Dataset Aliasing**: IO `alias()` method enables automatic dependency detection across pipelines
