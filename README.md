[![build status](https://gitlab.tubit.tu-berlin.de/anton.rudacov/DBPRO-IoT-Framework/badges/dev/build.svg)](https://gitlab.tubit.tu-berlin.de/anton.rudacov/DBPRO-IoT-Framework/commits/dev)  


# DBPRO IoT Framework

Development of an IoT framework for stream based analysis of sensor data. DBPRO, TU Berlin, summer term 2018.

(Original project home: [https://gitlab.tubit.tu-berlin.de/anton.rudacov/DBPRO-IoT-Framework](https://gitlab.tubit.tu-berlin.de/anton.rudacov/DBPRO-IoT-Framework))
 
  
## Features

This application is an example of basic Flink-Kafka-InfluxDB workflow.

The program performs following steps:
* generates a stream of timestamps and attaches data to it (captured before from a sensor).
* sinks stream data to InfluxDB and to a Kafka topic
* consumes data from Kafka topic (in another thread) and sinks it to InfluxDB as well
* splits infinite stream in windows and calculates "Dynamic Time Warping" distances in order to recognise the input
* adds annotations containing recognised character to Grafana via HTTP request 

 
## How to use

Just run the *main()* method in **jobs/App.java**

In order to actually get any observable output you have to run the necessary instances (Kafka, InfluxDB, Grafana) and setup their addresses in corresponding java classes.

 
## Built With

* [Kafka](https://kafka.apache.org/) - distributed streaming platform
* [Apache Flink](https://ci.apache.org/projects/flink/flink-docs-stable/) - data processing engine
* [InfluxDB](https://www.influxdata.com/) - time-series data storage
* [Grafana](https://grafana.com/) - time series analytics platform
* [Maven](https://maven.apache.org/) - Build automation tool and dependency management
* [GitLab](https://about.gitlab.com/) - Git repository manager, wiki, issue tracking and CI/CD
* [Docker](https://www.docker.com/) - Testing and CI environment (container platform)
 
## Authors

* **Anton Rudacov** - [@antonrud](https://github.com/antonrud)
* **Janine Schulte** - [@janineschulte](https://gitlab.tubit.tu-berlin.de/janineschulte)
* **Sven Hellweg** - [@hellweg.sven](https://gitlab.tubit.tu-berlin.de/hellweg.sven)
 
 
## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details


## Acknowledgements

Project structure and some implementations are inspired by [this awesome demonstration](https://youtu.be/fstKKxvY23c) by [Jamie Grier](https://github.com/jgrier).

Dynamic Time Warping algorithm is created by [Cheol-Woo Jung](mailto:cjung@gatech.edu) (browse [source](http://trac.research.cc.gatech.edu/GART/browser/GART/weka/edu/gatech/gart/ml/weka/DTW.java?rev=9)).
