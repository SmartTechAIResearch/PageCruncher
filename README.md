#PageCruncher
Spark workload using CommonCrawl input files to execute and profile common spark operations.

##Compile
CD into the PageCruncher folder and run

    sbt compile assemly

sbt can be installed using apt-get on Ubuntu or via a simular package manager for another OS.

If everything goes well a jar file will be available at /tmp/target/scala-2.1.0/.

##Usage
The workload works out of the box if the CommonCrawl and NER Classifiers are present in hdfs:///user/hdfs/CommonCrawl.

Optional args:
1. Path to CommonCrawl files (e.g. file:///&lt;path&gt; when running locally)
2. Repartition factor

---
