# PageCruncher
Spark workload using CommonCrawl input files to execute and profile common spark operations.

## Compile
CD into the PageCruncher folder and run

    sbt [clean] compile assembly

sbt can be installed using apt-get on Ubuntu or via a similar package manager for another OS.
For detailed instructions, see http://www.scala-sbt.org/release/docs/Installing-sbt-on-Linux.html .

Scala can be downloaded from their website, http://www.scala-lang.org/download/ . When using a Debian family OS, you
can simply download a deb package and install it with `dpkg -i scala-2.11.8.deb`. Note that you can specify a specific
Scala version in the **build.sbt** file. While building the app, that Scala version will be used. It is recommended to 
use the same version as Spark does. You can also change other dependencies in the same file.

If everything goes well a jar file will be available at /tmp/target/scala-2.11/ (depending on the specified Scala 
version). This path can be changed in the **build.sbt** file ( `target := file("/tmp/target/")` ).

If you don't want to compile the application yourself, you can always download the latest release.
https://github.com/sizingservers/PageCruncher/releases

## Usage

### Needed resources
The classifiers can be found under `/PageCruncher/lib/classifiers`. CommonCrawl archives can be downloaded from 
http://commoncrawl.org/the-data/get-started/. This version is tested with the archives from May 2015 and September 2016.
Normally it should work with older / newer archives as well.

You can put these files on a local file system, for example EXT4, or you can use the Hadoop file system (HDFS).

### Directory structure
It is common to run a Spark workload in a clustered environment. You can choose for **non-HDFS storage** or 
**HDFS storage**. Note, when you run the workload in a cluster environment, all nodes in the cluster need access to the
storage.


#### Local file systems
Default HDFS will be used, see below for more information. If you want to use another file system, you should create
some directories. For example the base path could be `/media/`, the CommonCrawl files can be places under 
`/media/CommonCrawl` and the classifiers under `/media/Classifiers`. While processing the data, it is needed to write
some intermediate results to disk, this path is also needed to run the workload properly. For example: `/media/Checkpoint`.

You can change these paths in the **SparkApp.scala** file or you can pass them as arguments (see below). 

#### HDFS
If you want to use HDFS, you can follow our standard directory structure or use your own paths. Just as with other file 
systems, you can change the paths in **SparkApp.scala** or pass them as arguments. 

The pathPrefix, is actually the URL to your HDFS master, for example: hdfs://spark-master:9000. If you want to follow our
path structur, you can place the CommonCrawl archives in `/user/hdfs/CommonCrawl/`, the classifiers in 
`/user/hdfs/Classifiers` and checkpoints can be written to `/user/hdfs/Checkpoint`.

Depending on how you are running the workload, you will need to put the jar file on your HDFS cluster (all nodes need
to access the file). 

 
#### Optional arguments. 
1. pathPrefix, starting directory for the app (e.g. file:///&lt;path&gt; or hdfs:///&lt;hdfs-master:port&gt;)
2. Repartition factor (cores, e.g. 48)
3. CommonCrawl path (e.g. user/hdfs/CommonCrawl)
4. Classifiers path (e.g. user/hdfs/Classifiers)
5. Checkpoint path (e.g. user/hdfs/Checkpoint)

#### Running the workload
Depending on where Spark is installed and so on, starting the app could look like the following rule 
(Standalone cluster manager):

    # $SPARK_HOME is an environment variable, this can be added in your ~/.bashrc file (e.g. export SPARK_HOME=/usr/local/spark)
    $SPARK_HOME/bin/spark-submit --class be.sizingservers.pagecruncher.SparkApp --master local[48] --deploy-mode client /tmp/target/scala-2.11/PageCruncher-assembly-1.1.1.jar file:///media 48 CommonCrawl/ Classifiers/ Checkpoint/
    
    $SPARK_HOME/bin/spark-submit --class be.sizingservers.pagecruncher.SparkApp --master spark://spark-master:6066 --deploy-mode cluster --supervise hdfs://spark-master:9000/user/hdfs/PageCruncher-assembly-1.1.1.jar hdfs://spark-master:9000/ 56
    

## Performance

Default Spark memory and CPU settings are rather low performance minded. There are several ways to decide how many cores
 and memory can be used. You can pass some arguments to spark-submit, or you can tune some settings in the config files. 

Have a look at the files in the `$SPARK_HOME/conf` directory. Your `spark-defaults.conf` could look like the following: 

    spark.eventLog.enabled          true
    spark.eventLog.dir              hdfs://spark-master:9000/user/hdfs/History
    spark.history.fs.logDirectory   hdfs://spark-master:9000/user/hdfs/History
    spark.local.dir                 /mnt/ssd1/tmp,/mnt/ssd2/tmp
    spark.driver.memory             14g
    spark.driver.cores              2
    spark.executor.memory           28g

More information can be found at http://spark.apache.org/docs/latest/configuration.html 

---