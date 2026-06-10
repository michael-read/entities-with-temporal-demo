# **Entity Demo with Temporal Workflows**

## Introduction

This repository, along with its corresponding [blog post here](https://www.tensor7.ai/post/creating-entities-with-temporal-workflows) demonstrates how to build, test, and run Entities using Temporal Workflows with the Java SDK.


## The Demo

### System Architecture

This demo showcases a streaming data pipeline built with a Pulsar message broker, all running in Docker containers that pass events into the User Entity Workflow.

#### How It Works

The system is organized into three main components that work together to process purchase data:

#### 1. Producer  
Generates random purchase events for 2,000 unique user IDs and publishes them to a Pulsar topic.

#### 2. Consumer

Reads purchase events from the Pulsar topic and forwards them to the workflow system.

#### 3. User Entity Workflow / Worker

Temporal receives purchase events and routes each one to the appropriate User Entity, which tracks and aggregates total spending per user.

### The Data Flow

```
Producer → Pulsar Topic → Consumer → User Entity Workflow → User Entities  
(creates events)    (message broker)    (reads events)    (aggregates data)  
```

This pipeline demonstrates how data moves through the system—from generation to consumption and finally to entity-based processing.

## Running The Demo

This demo has been developed and runs successfully on the following laptop specifications:  

### Hardware Information:  
- Hardware Model:                              System76 Oryx Pro  
- Memory:                                      64.0 GiB  
- Processor:                                   11th Gen Intel® Core™ i7-11800H × 16  
- Graphics:                                    Intel Corporation TigerLake-H GT1 \[UHD Graphics\]  
- Graphics 1:                                  NVIDIA GeForce RTX™ 3070 Laptop GPU  
- Disk Capacity:                               1.0 TB

### Software Information:  
- Firmware Version:                            2023-09-08\_42bf7a6  
- OS Name:                                     Ubuntu 24.04.4 LTS  
- OS Build:                                    (null)  
- OS Type:                                     64-bit  
- GNOME Version:                               46  
- Windowing System:                            X11  
- Kernel Version:                              Linux 6.14.0-37-generic

> **_Note:_**  This demo should take less than 20GB of memory to run.

### Prerequisites

* Installed Java JDK (v24 recommend [Eclipse Adoptium](https://adoptium.net/temurin/release))
* Installed Apache Maven  (v3.9.15 \+)
* Installed Docker
* Installed Docker Compose
* The **producer** and **consumer** subprojects use Akka Actors and Akka Streams. To build these projects, you’ll need to configure Maven to download the required dependencies from the Akka Maven repository (https://repo.akka.io/maven/). Access to this repository requires an access token. If you don't have one, you'll need to [register for a free token](https://account.akka.io/token) before building these projects. Once registered, Akka provides instructions for configuring Maven’s **settings.xml** file to seamlessly integrate the Akka repository. **Licensing note:** Akka is initially released under the Business Source License (BSL), which automatically converts to the Apache 2.0 license after three years. This demo uses Akka versions released on April 27, 2023, which are now open source under Apache 2.0.
* Clone the Temporal Server Samples to a convenient place from [here](https://github.com/temporalio/samples-server).
* Clone this repository to a convenient place.

If you’d like to take a look at Grafana observability for the Temporal Server in later steps, then you’ll need to add the environment variable:

```
- PROMETHEUS_ENDPOINT=0.0.0.0:8000  
```

at approximately line 88 of the **temporal** service in the **/samples-server/compose/docker-compose.yml** file before continuing.

### Steps to Run:

1. Build and containerize this project to your local Docker repository:
```bash
    cd entity-demo-java
    mvn compile package
```
This should result in creating the following images:
```
IMAGE                                     DISK USAGE
temporal-entity-demo/consumer:0.0.1       503MB        
temporal-entity-demo/consumer:latest      503MB        
temporal-entity-demo/producer:0.0.1       464MB        
temporal-entity-demo/producer:latest      464MB        
temporal-entity-demo/user-entity:0.0.1    436MB        
temporal-entity-demo/user-entity:latest   436MB        
```
You can verify the above images using the "docker images" command.

2. Open a terminal window, and start the supporting Temporal Server:
```bash
    cd /samples-server/compose
    docker compose up
```
> **_Note:_** Wait until it comes to a slow crawl before the next step.
3. Open another terminal window, and switch to the directory of this [README.md](http://README.md).
```bash
    cd entity-demo-java
    docker compose up
```

> **_Note:_**: You should wait for at least 30 seconds before the next step.

4. Pulsar should be up and running. The init script having created the tenant, topic, and three partitions. Next, open another terminal window, in the same directory, and issue the following to start the streaming pipeline:

```bash
    docker compose scale producer=1 consumer=3
```

### Look at Observability

#### Poke around Grafana

1. Open Grafana by opening a browser and entering the address [http://localhost:3000](http://localhost:3000)
2. Enter username/password (admin/admin)
3. Enter new username/password(s)  (I usually just enter admin/admin again)
4. Once logged in, close the ad banner, and click on “Dashboards” on the left side.
5. Under Dashboards, click on the Pulsar folder, and then click on the “Pulsar \- Topic” link.

The “Pulsar \- Topic” dashboard will give you insights to how the broker is performing. The most interesting metric is probably the “Local backlog”.

If you enabled the PROMETHEUS\_ENDPOINT, then you should have metrics for both the Temporal workers and the Temporal server from the Dashboards/Temporal folder.

#### Poke around the Temporal UI

Open the Temporal UI by opening a browser and entering the address [http://localhost:8080](http://localhost:8080).

The default Temporal UI display is running **Workflows**. There should eventually be 2000 workflows running concurrently. If you want to see the state of any given user entity, click on the specific workflow to open it, and click on **Queries**, and finally **\[Run Query\]**.

#### Check out Prometheus scraping

Open the Prometheus scraping targets display by opening a browser and entering the address http://localhost:9090/targets.

Some targets may be down because we have not used all the potentially configured instances contained in the observability/prometheus.yml file.

### Thoughts on Pipeline Scalability

#### Producer

The producer is the fastest component in our pipeline because it generates events and publishes them directly to the Pulsar topic. By default, it is rate-limited to 75 events per second.

To increase production output, you can add more producer instances. For example, to double the output to 150 events per second:

```bash
$ docker compose scale producer=2
```

To change the event generation rate, you can scale down to zero, modify the code, rebuild the image, and then scale back to one instance.

#### Consumer

The consumer operates in Pulsar's Failover consumption mode, which limits active consumers to one per partition. With three partitions, you can have at most three active consumers. Additional consumers remain idle and only activate when an active consumer fails.

**Consumer distribution examples:**

| Consumers | Partition Distribution |
|-----------|----------------------|
| 3 | Each consumer handles one partition |
| 2 | One consumer handles two partitions; the other handles one |
| 1 | The single consumer handles all three partitions |

To increase consumption capacity, you must add more Pulsar partitions.

#### Entity Workers

The entity workers aggregate data from the user entity workflow. This demo starts with five instances, which provides a reasonable baseline. You can scale up or down as needed.

To add another instance:

```bash
$ docker compose scale entity=6
```


### How to shut down.

There should be two terminal windows open. You can enter the “d” character to detach, and then enter:  
```bash 
docker compose down  
```