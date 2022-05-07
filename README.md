# spark-sight: Spark performance at a glance

<br/>

[<img src="images/chrome_366lyjRAot.gif">](https://medium.com/@alfredo.fomitchenko/spark-sight-spark-performance-at-a-glance-c2316d2a251b)

<br/>

[<img src="images/chrome_7fwtosJQ0I.gif">](https://medium.com/@alfredo.fomitchenko/spark-sight-spark-performance-at-a-glance-c2316d2a251b)

## What is it?
**spark-sight** is a less detailed, more intuitive representation 
of what is going on inside your Spark application in terms of performance:

* CPU time spent doing the “actual work”
* CPU time spent doing shuffle reading and writing
* CPU time spent doing serialization and deserialization
* (coming) Memory usage per executor
* (coming) Memory spill intensity per stage

**spark-sight** is not meant to replace the Spark UI altogether,
rather it provides a bird’s-eye view of the stages
allowing you to identify at a glance 
which portions of the execution may need improvement.

## Main features
The Plotly figure consists of two charts with synced x-axis:
* top chart: **efficiency** in terms of CPU cores available for tasks

![](images/chrome_Z4O7K79Pgk.gif)

* bottom chart: stages **timeline**

![](images/chrome_pL0UHBeNIo.gif)

## Where to get it

```shell
pip install spark-sight
```

## Dependencies

* [**Pandas**: Powerful Python data analysis toolkit](https://github.com/pandas-dev/pandas) 
  for support with Spark event log ingestion and processing 
* [**Plotly**: The interactive graphing library for Python](https://github.com/plotly/plotly.py) 
  for the awesome interactive UI

## Usage

```shell
spark-sight --help
```

```
                      _             _       _     _
 ___ _ __   __ _ _ __| | __     ___(_) __ _| |__ | |_
/ __| '_ \ / _` | '__| |/ /____/ __| |/ _` | '_ \| __|
\__ \ |_) | (_| | |  |   <_____\__ \ | (_| | | | | |_
|___/ .__/ \__,_|_|  |_|\_\    |___/_|\__, |_| |_|\__|
    |_|                               |___/

usage: spark-sight [-h] [--path path] [--cpus cpus] [--deploy_mode [deploy_mode]]

Spark performance at a glance.

optional arguments:
  -h, --help            show this help message and exit
  --path path           Local path to the Spark event log
  --cpus cpus           Total CPU cores of the cluster
  --deploy_mode [deploy_mode]
                        Deploy mode the Spark application was submitted with. Defaults to cluster deploy mode
```

### Unix

```shell
spark-sight \
    --path "/path/to/spark-application-12345" \
    --cpus 32 \
    --deploy_mode "cluster_mode"
```

A new browser tab will be opened.

### Windows PowerShell

```shell
spark-sight `
    --path "C:\path\to\spark-application-12345" `
    --cpus 32 `
    --deploy_mode "cluster_mode"
```

A new browser tab will be opened.

## Read more

[<img src="images/logo_medium.png" width="300">](https://medium.com/@alfredo.fomitchenko/spark-sight-spark-performance-at-a-glance-c2316d2a251b)
