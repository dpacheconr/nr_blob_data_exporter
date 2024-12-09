## Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/dpacheconr/nr_blob_data_exporter
    cd nr_blob_data_exporter
    ```

2. Create a virtual environment:
    ```sh
    python3 -m venv venv
    ```

3. Activate the virtual environment:

    - On Windows:
        ```sh
        venv\Scripts\activate
        ```
    - On macOS/Linux:
        ```sh
        source venv/bin/activate
        ```

4. Install the required dependencies:
    ```sh
    pip3 install -r requirements.txt
    ```

## Running the Script

1. Ensure you have set the correct values for NEW_RELIC_API_KEY, NEW_ACCOUNT_ID, QUERY, log_attribute and blob_attribute

2. Run the script:
    ```sh
    python3 blob_exporter.py
    ```

## Exiting the Virtual Environment

To exit the virtual environment, simply run:
```sh
deactivate
```

The script will fetch data from the New Relic API, process it, and generate CSV files with the results.