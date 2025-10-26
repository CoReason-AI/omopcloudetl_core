# How to Use This Project with Visual Studio

This guide provides step-by-step instructions for setting up and working with this project in Visual Studio using a local clone of a GitHub repository.

## Prerequisites

-   **Git:** You must have Git installed on your system. You can download it from [git-scm.com](https://git-scm.com/).
-   **Visual Studio:** This guide assumes you have a recent version of Visual Studio installed with the **Python development** workload. You can download it from [visualstudio.microsoft.com](https://visualstudio.microsoft.com/).
-   **Python:** Ensure you have a supported version of Python installed (>=3.10). You can download it from [python.org](https://python.org/).

## Step 1: Clone the GitHub Repository Locally

First, you need to get a local copy of the project from your GitHub repository.

1.  **Open a Terminal or Command Prompt:** You can use Git Bash, PowerShell, or the command prompt.
2.  **Navigate to Your Projects Folder:** `cd` into the directory where you want to store the project.
3.  **Clone the Repository:** Run the `git clone` command with your repository's URL.

    ```sh
    git clone https://github.com/YOUR_USERNAME/YOUR_REPOSITORY.git
    ```

4.  **Navigate into the Project:**

    ```sh
    cd YOUR_REPOSITORY
    ```

## Step 2: Open the Project in Visual Studio

Visual Studio has excellent support for Python projects, even those without a `.sln` file.

1.  **Launch Visual Studio.**
2.  From the start screen, select **Open a local folder**.
3.  Navigate to the directory where you just cloned the repository and click **Select Folder**.
4.  Visual Studio will open the folder and the **Solution Explorer** will show the complete file structure.

## Step 3: Set Up the Python Environment

Visual Studio needs to know which Python interpreter to use. It can automatically create and manage a virtual environment for you.

1.  **Create a Virtual Environment:**
    -   In the **Solution Explorer**, right-click on the `Python Environments` node and select **Add Environment...**.
    -   In the **Add Environment** dialog, choose the **Virtual Environment** option.
    -   **Name:** Keep the default name (e.g., `.venv`).
    -   **Base interpreter:** Select a Python version that meets the project's requirements (>=3.10).
    -   **Requirements file:** Visual Studio should automatically detect the `pyproject.toml` file.
    -   Click **Create**.

2.  **Wait for Installation:** Visual Studio will create the virtual environment and automatically install all the dependencies listed in `pyproject.toml` (including the `dev` dependencies like `pytest`). You can monitor the progress in the **Output** window.

## Step 4: Run the Tests

Once the dependencies are installed, you can discover and run the tests directly within Visual Studio.

1.  **Open the Test Explorer:**
    -   Go to the top menu and select **Test > Test Explorer**.
2.  **Discover Tests:**
    -   The **Test Explorer** will automatically discover the tests in the `tests/` directory. You should see `test_add` listed.
3.  **Run the Tests:**
    -   Click the **Run All Tests In View** button (the green play icon) at the top of the **Test Explorer**.
    -   The tests will run, and the results will be displayed. You should see a green checkmark indicating that the test passed.

## Step 5: Working with Your Code

You can now edit the project files, and Visual Studio will provide full IntelliSense, debugging, and linting support.

-   **Editing:** Open any `.py` file (e.g., `src/my_python_project/main.py`) to start coding.
-   **Debugging:** Set breakpoints by clicking in the margin next to a line of code and start a debugging session by running your script with the debugger attached.
-   **Linting:** Visual Studio will use the project's `ruff` configuration from `pyproject.toml` to lint your code and show warnings or errors directly in the editor.

You have now successfully set up and tested the project in Visual Studio!
