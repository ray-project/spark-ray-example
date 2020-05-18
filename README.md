# README - Ray and PySpark Example

[Dean Wampler](mailto:dean@anyscale.com), May 2020

This repo contains Dean's demo for his talk at Spark + AI Summit 2020.

## Setup

### Java 8

You will need Java 8 for Spark.

Install Java 8 from [here](https://www.java.com/en/download/). Once installed, verify the version:

```shell
$ java -version
java version "1.8.0_221"
Java(TM) SE Runtime Environment (build 1.8.0_221-b11)
Java HotSpot(TM) 64-Bit Server VM (build 25.221-b11, mixed mode)
```

(The patch version `221` may be different.)

### Python 3.6 or 3.7

Python 3.6 or 3.7 is required. PySpark doesn't yet support 3.8.

#### Anaconda

We recommend using [Anaconda](https://www.anaconda.com/). Installation instructions are [here](https://www.anaconda.com/distribution/).

Then run the following commands to set up the demo environment, including Python 3.7, PySpark, and Ray:

```shell
$ conda env create -f environment.yml
$ conda activate spark-ray
```

#### Pip

If you prefer to use `pip` instead, first ensure you have a supported version of Python installed:

```shell
$ python --version
Python 3.7.7
```

If needed, Python installation instructions are [here](https://www.python.org/downloads/).

Installation instructions for `pip` are [here](https://pip.pypa.io/en/stable/installing/).

Now run the following command to complete the setup.

```shell
$ pip install -r requirements.txt
```

## Running the Demo

First, start Jupyter Lab as follows:

```shell
$ jupyter lab
```

A browser window will open with the UI.

Double click [Spark-RayUDF.ipynb](Spark-RayUDF.ipynb) to open the notebook and work through the self-contained example.

## Ray Serve Implementation

There is a second implementation (for completeness), where the UDF makes HTTP requests to a separate service, implemented with [Ray Serve](https://docs.ray.io/en/master/rayserve/overview.html). In other words, Ray _isn't_ embedded in the PySpark UDF for this example. The service invoked could be implemented in any technology.

To Run this variant, first evaluate all the cells in the [ray-serve/DataGovernanceServer.ipynb](ray-serve/DataGovernanceServer.ipynb) notebook. This runs the server for Data Governance requests.

Then work through the [ray-serve/Spark-RayServiceUDF.ipynb](ray-serve/Spark-RayServiceUDF.ipynb) notebook.

## For More about Ray

* https://ray.io: The entry point for all things Ray
* Tutorials: https://anyscale.com/academy
* GitHub repo: https://github.com/ray-project/ray
* Need help?
    * Ray Slack: https://ray-distributed.slack.com
    * [ray-dev](https://groups.google.com/forum/?nomobile=true#!forum/ray-dev) Google group
* Check out these forthcoming events online: https://anyscale.com/events
