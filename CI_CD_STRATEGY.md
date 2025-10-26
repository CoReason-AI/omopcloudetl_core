# CI/CD Strategy

This document outlines the CI/CD architecture for this project, including the Docker strategy and security measures.

## CI/CD Architecture

The CI/CD pipeline is built using GitHub Actions and is divided into two workflows: `ci.yml` and `docker.yml`.

-   **`ci.yml`**: This workflow is triggered on `push` and `pull_request` events to the `main` and `develop` branches. It consists of two jobs:
    1.  **`lint`**: This job runs the `pre-commit` suite to ensure all code adheres to the defined quality and style standards.
    2.  **`test`**: This job runs the `pytest` suite across a matrix of Python versions (3.10, 3.11, and 3.12) to ensure the code is working as expected. It depends on the `lint` job, so it will only run if the linting passes.

-   **`docker.yml`**: This workflow is triggered on `push` events to the `main` and `develop` branches. It builds, scans, and pushes a Docker image to the GitHub Container Registry.

## Docker Strategy

The `Dockerfile` is a multi-stage build to create a lean and secure production image.

-   **Stage 1 (Builder)**: This stage installs Poetry, exports the project dependencies to a `requirements.txt` file, and installs them. It also installs the application itself.
-   **Stage 2 (Runtime)**: This stage uses a slim Python base image, creates a non-root user, and copies the installed dependencies and application from the builder stage. This results in a smaller and more secure final image.

## Security Measures

-   **Principle of Least Privilege (PoLP)**: The GitHub Actions workflows are configured with the minimum required permissions.
-   **SHA Pinning**: All third-party GitHub Actions are pinned to their full commit SHA to prevent supply chain attacks.
-   **Vulnerability Scanning**: The `docker.yml` workflow includes a step to scan the Docker image for vulnerabilities using Trivy. The workflow will fail if any `CRITICAL` or `HIGH` severity vulnerabilities are found.
-   **Non-Root User**: The `Dockerfile` creates and runs the application as a non-root user to reduce the attack surface.
