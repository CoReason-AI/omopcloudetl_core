# OMOP Cloud ETL Ecosystem Documentation

## Introduction

This document provides a comprehensive overview of the `omopcloudetl-core` software and its role within the larger OMOP Cloud ETL ecosystem. It details the architectural principles, the function of each component, and how the current implementation aligns with the provided specification and build guide.

## High-Level Architecture (HLD) and Ecosystem

The OMOP Cloud ETL ecosystem is a hyper-modular, cloud-native framework designed for petabyte-scale ETL processes targeting the OMOP Common Data Model. It employs a **Compiler/Submitter Architecture**, which fundamentally decouples workflow definition, SQL compilation, and execution orchestration. This separation addresses the scalability and maintainability bottlenecks often found in traditional, monolithic ETL tools.

The ecosystem is composed of several key Python packages:

*   **`omopcloudetl-core`**: The foundational library. It contains the core abstractions, data models (for configuration, workflows, and DML), the `WorkflowCompiler`, and the `SpecificationManager`. It defines *what* to do, but not *how* to execute it.
*   **`omopcloudetl`**: The Command Line Interface (CLI) that acts as the user-facing entry point and coordinator for the entire ecosystem.
*   **`omopcloudetl-omop-resources`**: An assets package that distributes standard, dialect-agnostic transformation logic in the form of YAML DML files.
*   **Orchestrator Packages (e.g., `omopcloudetl-orchestrator-local`)**: Pluggable components responsible for executing a compiled workflow plan. The local orchestrator, for example, uses a simple thread pool for development and small-scale loads.
*   **Provider Packages (e.g., `omopcloudetl-provider-databricks`)**: Pluggable components that provide the platform-specific logic for connecting to a database and compiling DML into dialect-specific SQL.

## Core Architectural Principles

The `omopcloudetl-core` implementation is mandated to follow several key architectural principles:

1.  **Declarative DML over Imperative SQL**: Instead of writing complex and often dialect-specific SQL scripts, users define transformation logic in structured, readable, and maintainable YAML DML files. `omopcloudetl-core` defines the Pydantic schema for this DML, ensuring validation and consistency.

2.  **Compiler/Orchestrator Separation**: The core package acts as a `WorkflowCompiler`. Its primary role is to take a user-defined workflow, validate its dependency graph (DAG), and compile it into an executable artifact called a `CompiledWorkflowPlan`. The core package contains **no execution logic**. The actual execution is delegated to a separate, pluggable `Orchestrator`.

3.  **Decoupled Orchestration**: The `BaseOrchestrator` abstract class defined in the core package allows for different execution engines to be used. This means the same workflow can be run on a local machine for testing and then scaled up on a cloud-native platform like Databricks Jobs or Argo Workflows simply by changing the configuration, without altering the core logic.

4.  **Zero Data Movement (Cloud-Native Ingress)**: A critical principle for cloud scalability. The framework is designed so that the client (the machine running the CLI) only moves instructions, never the actual data. Bulk loading operations are handled by the data platform itself, using URI-based commands (e.g., `COPY INTO 's3://...'`). This avoids routing petabytes of data through a client machine, which is a common performance bottleneck.

5.  **Automated Idempotency**: The `BaseSQLGenerator` abstraction requires that provider implementations automatically produce idempotent SQL (e.g., using `MERGE` or a transactional `DELETE`+`INSERT`). This makes ETL pipelines resilient and safely re-runnable without causing data duplication.

6.  **Dynamic CDM Specification Sourcing**: To ensure workflows are always aligned with the latest standards, the `SpecificationManager` dynamically sources OMOP CDM DDL definitions directly from the official OHDSI GitHub repository. It also supports caching to improve performance and can use local files for offline development.

7.  **Commercial-Grade Robustness**: The architecture mandates features like automatic connection retries (using `tenacity`) and structured observability through Pydantic models for `LoadMetrics` and `ExecutionMetrics`, ensuring the framework is reliable and transparent.

## Codebase Comparison to Specification

The current `omopcloudetl-core` codebase is a complete and faithful implementation of the 8-phase build specification provided. It successfully embodies all the architectural principles and component responsibilities outlined.

| Specification Phase | Component | Implemented In | Status & Notes |
| :--- | :--- | :--- | :--- |
| **Phase 0: Setup** | Project Structure & Dependencies | `pyproject.toml`, `src/omopcloudetl_core/` | **Complete**. All specified dependencies and the full directory structure are in place. |
| **Phase 1: Core Infra** | Custom Exceptions & Logging | `exceptions.py`, `logging.py` | **Complete**. A full hierarchy of custom exceptions and a centralized, colorized logger are implemented. |
| **Phase 2: Config** | Secrets Abstraction & Config Models | `abstractions/secrets.py`, `config/models.py`, `config/manager.py` | **Complete**. `ConfigManager` properly loads YAML, resolves secrets via the `DiscoveryManager`, and populates Pydantic models. |
| **Phase 3: Core Models** | Metrics & DML Schema | `models/metrics.py`, `models/dml.py` | **Complete**. The Pydantic models for observability (`LoadMetrics`) and the declarative DML schema (using discriminated unions) are fully defined. |
| **Phase 4: Abstractions**| `BaseConnection`, `BaseSQLGenerator`, `BaseOrchestrator` | `abstractions/` | **Complete**. All core abstract base classes are defined as per the specification, enforcing the key architectural patterns. |
| **Phase 5: CDM Sourcing**| `SpecificationManager` & Models | `specifications/` | **Complete**. The manager correctly fetches, parses, and caches OMOP CDM specifications from the OHDSI repository. |
| **Phase 6: Discovery** | `DiscoveryManager` | `discovery.py` | **Complete**. The manager uses `importlib.metadata` to dynamically discover and instantiate pluggable providers and orchestrators from other packages. |
| **Phase 7: Workflow** | Workflow Models & Compiled Plan | `models/workflow.py` | **Complete**. The models for both the user-facing `WorkflowConfig` and the compiler's output, `CompiledWorkflowPlan`, are fully implemented. |
| **Phase 8: Compilation**| `WorkflowCompiler` & `sql_tools` | `compilation/compiler.py`, `sql_tools.py` | **Complete**. The `WorkflowCompiler` successfully orchestrates all other components to translate a high-level workflow into a detailed, executable plan without containing any execution logic itself. |

In summary, the `omopcloudetl-core` package serves as the robust, well-architected foundation of the entire ecosystem. It successfully translates the high-level design principles into a concrete, testable, and extensible software artifact, setting the stage for the broader ecosystem of providers and orchestrators to build upon.
