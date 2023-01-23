# unizin-validation

## Overview

The unizin-validation project contains a program written in Python that attempts to ensure Canvas data was successfully
loaded each night into Unizin data sources, including the Unizin Data Warehouse (UDW) and the Unizin Data Platform (UDP).
The program does this by running SQL queries against the data sources and performing basic checks on the results to detect
irregularities. The queries and checks used are defined in `dbqueries.py`. CSV files with the query results are generated
as part of the workflow.

## Development

### Pre-requisities

The sections below provide instructions for configuring, installing, and using the application.
Depending on the environment you plan to run the application in, you may need to install one of the following:

* [Python 3.10](https://docs.python.org/3/)
* [Docker Desktop](https://www.docker.com/products/docker-desktop)

### Configuration

Configuration variables for the program, `validate.py`, are loaded using a JSON file, typically called `env.json`.
To create your version of this file, make a copy of the `env_sample.json` template from the project's `config` directory;
then, add the connection parameters for each data source in the proper nested JSON object.
To connect to these data sources, you will likely need to use a VPN or Ethernet connection with the necessary permissions.
You can also use the configuration file to set the [Python logging level](https://docs.python.org/3/library/logging.html)
(with `LOG_LEVEL`) and the path CSV files will be written to (with `OUT_DIR`).

For development, it is recommended that you use the default file name, `env.json`, and store it in the default directory,
`config` (or a volume mapped to `config`; see the **With Docker** section below). However, the program checks for an environment
variable called `ENV_FILE` before using these defaults, so the path and name expected by the program can be tweaked if desired.

### Installation & Usage

#### With `venv`

To install and run the validation program using a Python virtual environment, do the following:

1. Place the `env.json` file described in the **Configuration** section (above) in the `config` directory.

2. Create and activate a virtual environment.
    ```sh
    python3 -m venv venv
    source venv/bin/activate  # for Mac OS
    ```

3. Install the dependencies.
    ```sh
    pip install -r requirements.txt
    ```

4. Run the program.
    ```sh
    python validate.py
    ```

    Optionally, you can specify one of two pre-defined jobs -- `UDW` or `Unizin` -- as an additional option.
    The default job value is `UDW`. The `Unizin` job includes an additional query and check against the UDP Context Store.

CSV files containing the query results will be written to the value of the `OUT_DIR` configuration variable
(the default is the `data` directory).

You can also run the test suite by issuing the following command:
    ```
    python test.py
    ```


#### With Docker

The validation program can also be installed and run with Docker using Docker Compose. To do so, perform the following steps.
**Note**: these steps assume you have specified the value of `OUT_DIR` as the `data` directory and that the
configuration file will be found at the path `config/env.json`.

1. Create directories at `~/secrets/unizin-validation` and `~/data/unizin-validation` on your machine,
where `~` is your user's home directory.

2. Place the `env.json` file described in the **Configuration** section (above) in the `~/secrets/unizin-validation` directory.

3. Build a Docker image for the project.
    ```sh
    docker compose build
    ```

4. Run one of the job services
    ```sh
    # For the UDW job
    docker compose run udw
    # For the Unizin job
    docker compose run unizin
    ```

CSV files containing the query results will be written to the `~/data/unizin-validation` directory on your machine.

You can also run the test suite by issuing the following command:
    ```
    docker compose run test
    ```
