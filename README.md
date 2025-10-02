# Lakehouse Demo Project

## Overview
This project demonstrates the capabilities of a Lakehouse architecture using Databricks. It showcases how to analyze and visualize data efficiently.

## Getting Started
To get started with the project, follow these steps:

1. Install pipx and hatch:
    ```sh
    python -m pip install --user pipx
    python -m pipx ensurepath
    pipx install hatch
    ```

## Setup dashboard
You can use the Databricks UI to modify the dashboard, but any modifications made through the UI will not be applied to the bundle `.lvdash.json` file unless you explicitly update it.

To update the local bundle `.lvdash.json` file, run:

```sh
databricks bundle generate dashboard --resource nyc_taxi_trip_analysis --force
```

To continuously poll and retrieve the updated `.lvdash.json` file when it changes, run:

```sh
databricks bundle generate dashboard --resource nyc_taxi_trip_analysis --force --watch
```


## Relevant Docs:

1. Databricks free limitations:
https://docs.databricks.com/aws/en/getting-started/free-edition-limitations

2. sync app 
databricks sync --watch . /Workspace/Users/rahl@kapacity.dk/booking-app

