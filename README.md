# README - Ray and PySpark Example

[Dean Wampler](mailto:dean@anyscale.com), May 2020

This code was used for a Spark + AI Summit 2020 demo.

## Setup

You'll need Python 3.6 or newer and Java 8. Then `pip install` the following:

```
pip install ray[serve]
pip install pyspark
pip install jupyterlab
```

## Running the Demo

### Running Ray

This example runs Ray as a one-node cluster on your laptop.

In a terminal window, run the following command to start the Ray "head" service:

```shell
ray start --head
```

### Running Jupyter Lab

Start Jupyter Lab as follows:

```shell
jupyter lab
```

A browser window will open with the UI.

Double click `Spark-RayUDF.ipynb` to open the notebook and work through the self-contained example.

### Ray Serve Implementation

There is a second implementation (for completeness), where the UDF makes HTTP requests to a separate service, implemented with [Ray Serve](https://docs.ray.io/en/master/rayserve/overview.html). This service could be implemented in any technology; Ray is not explicitly used in the PySpark UDF for this example.



## For More about Ray

* https://ray.io: The entry point for all things Ray
* Tutorials: https://anyscale.com/academy
* GitHub repo: https://github.com/ray-project/ray
* Need help?
    * Ray Slack: https://ray-distributed.slack.com
    * [ray-dev](https://groups.google.com/forum/?nomobile=true#!forum/ray-dev) Google group
* Check out our forthcoming events online: https://anyscale.com/events
