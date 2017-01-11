package be.sizingservers.pagecruncher

import java.io.{BufferedInputStream, InputStreamReader}
import java.net.URI
import java.util.Calendar

import de.l3s.boilerpipe.extractors.ArticleExtractor
import edu.stanford.nlp.ling.CoreAnnotations
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}
import org.apache.hadoop.io.Text
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.archive.io.ArchiveReader
import org.commoncrawl.warc.WARCFileInputFormat

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created by wannes on 7/6/15.
  */
case class Entity(label: String, etype: String)

object SparkApp {

  val sleepTime = 3000
  // Gives you some time to read important console output
  // Set to 0 if you don't need this information.
  val timestamp: Long = System.currentTimeMillis / 1000

  var pathPrefix = "hdfs://spark-master:9000/"
  // HDFS or local FS -> starting point where to find files
  var numCores = 8
  // Repartition factor: factor 8 will result in 8 tasks
  var commoncrawlPath = "user/hdfs/CommonCrawl/"
  // Relative path to CommonCrawl files (see pathPrefix)
  var classifiersPath = "user/hdfs/Classifiers/"
  // Relative path to the Classifiers (see pathPrefix)
  var checkpointPath = "user/hdfs/Checkpoint/" // Relative path to the Checkpoint dir (see pathPrefix)

  def getPathPrefix(): String = pathPrefix

  def getClassifiersPath(): String = classifiersPath

  // Main program
  def main(args: Array[String]): Unit = {
    try
      // Processing arguments
      if (args.length >= 1) pathPrefix = args(0) + "/"
      if (args.length >= 2) numCores = Integer.parseInt(args(1))
      if (args.length >= 3) commoncrawlPath = args(2)
      if (args.length >= 4) classifiersPath = args(3)
      if (args.length >= 5) checkpointPath = args(4)

      // Print processed arguments
      printf("\u001B[32m\n---Path: %s, numCores %d---\n\n\u001B[0m", pathPrefix, numCores)
      println("pathPrefix: " + pathPrefix)
      println("numCores: " + numCores)
      println("commoncrawlPath: " + commoncrawlPath)
      println("classifiersPath: " + classifiersPath)
      println("checkpointPath: " + checkpointPath)
      Thread.sleep(sleepTime)


      // Spark configuration
      val sparkConf = new SparkConf()
      sparkConf.setAppName("PageCruncher Benchmark")

      // Spark entry point = spark context
      val sc = new SparkContext(sparkConf)

      // Location where to save (to local FS / HDFS)
      sc.setCheckpointDir(pathPrefix + checkpointPath)

      // Generic configuration
      val conf = new Configuration()

      // FS: wich one (will be determined based on the pathPrefix)
      var fs: FileSystem = null
      if (pathPrefix.startsWith("hdfs")) {
        // To avoid strange exceptions, the HDFS path has to be an URI
        fs = FileSystem.get(new URI(pathPrefix), conf)
      } else {
        // To avoid strange exception, set the specific defaultFS for Spark
        conf.set("fs.defaultFS", pathPrefix + commoncrawlPath)
        fs = FileSystem.get(conf)
      }

      val crawlFiles = mutable.MutableList[String]()
      ClassifierStore.hadoopConf = conf

      // Find the CommonCrawl files
      // The second boolean parameter here sets the recursion to true
      val fileStatusListIterator: RemoteIterator[LocatedFileStatus] = fs.listFiles(new Path(pathPrefix + commoncrawlPath), false)
      while (fileStatusListIterator.hasNext) {
        val fileStatus: LocatedFileStatus = fileStatusListIterator.next
        if (fileStatus.getPath.getName.startsWith("CC")) {
          crawlFiles += fileStatus.getPath.toString
        }
      }

      // newAPIHadoopFile(String path, scala.reflect.ClassTag<K> km, scala.reflect.ClassTag<V> vm, scala.reflect.ClassTag<F> fm)
      // Get an RDD for a Hadoop file with an arbitrary new API InputFormat.
      val ent = crawlFiles.map(sc.newAPIHadoopFile(_, classOf[WARCFileInputFormat[Text, ArchiveReader]], classOf[Text], classOf[ArchiveReader]))
        .map(extractEntities)
        .toSeq // Sequential instead of parallel

      println("\n\u001B[32m" + "---Group / Sort---" + "\u001B[0m\n")
      Thread.sleep(sleepTime)

      // Union -> unique elements from sc and ent
      val res = sc.union(ent)
        .groupBy(_._1) // groupby first element
        /*.flatMap {
          x =>
            val sitecount = mutable.HashMap[String, Long]()
            for (site <- x._2) {
              if (sitecount.contains(site._2)) {
                val lng:Long = sitecount.get(site._2).get + 1
                sitecount.put(site._2, lng)
              } else {
                sitecount.put(site._2, 1)
              }
            }
            sitecount.map{ case (k,v) => ((k, (x._1._1, x._1._2)), v)}.toSeq
        }*/
        .flatMap {
        case (k, x) =>
          x.map(_._2).groupBy(identity).mapValues(_.size).map { c => ((c._1, k), c._2) }
      }
        .sortBy(_._2, ascending = false)

      println("\n\u001B[32m" + "---Initial Processing Done---" + "\u001B[0m\n")
      Thread.sleep(sleepTime)

      res
        .saveAsTextFile(pathPrefix + "count-per-site" + timestamp.toString)

      println("\n\u001B[32m" + "---Site Count Saved---" + "\u001B[0m\n")
      Thread.sleep(sleepTime)

      val si = res.keys.map(_._1).distinct().map(x => (x.hashCode, x))
      val ei = res.keys.map(_._2).distinct().map(x => (x.hashCode, x))

      println("\n\u001B[32m" + "---Indexes Created---" + "\u001B[0m\n")
      Thread.sleep(sleepTime)

      //val sei = si.join(ei)

      //println("Lookup table joined")

      val ratings = res
        .map { x =>
          Rating(x._1._1.hashCode, x._1._2.hashCode, x._2)
        }

      println("\n\u001B[32m" + "---Created Ratings---" + "\u001B[0m\n")
      Thread.sleep(sleepTime)

      val model = ALS.train(ratings, 10, 20, 1)

      println("\n\u001B[32m" + "---Trained Model---" + "\u001B[0m\n")
      Thread.sleep(sleepTime)

      val usersProducts = ratings.map { case Rating(user, product, rate) =>
        (user, product)
      }
      val predictions =
        model.predict(usersProducts).map { case Rating(user, product, rate) =>
          ((user, product), rate)
        }
      val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }.join(predictions)
      val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
        val err = (r1 - r2)
        err * err
      }.mean()

      println("\n\u001B[32m" + "---Mean Squared Error = " + MSE + "---" + "\u001B[0m\n")
      Thread.sleep(sleepTime)

      //model.save(sc, "hdfs:///user/hdfs/model-train")

      predictions.map {
        case ((site, entity), count) =>
          (site, (entity, count))

      }
        .join(si)
        .map {
          case (sidx, ((eidx, count), site)) =>
            (eidx, (site, count))
        }
        .join(ei)
        .map {
          case (eidx, ((site, count), (entlbl, enttype))) =>
            ((entlbl, enttype), site, count)
        }
        .sortBy(_._3)
        .saveAsTextFile(pathPrefix + "predictions" + timestamp.toString)
      /*.flatMap(_._2)
      .map{ case (k, t) =>
      (k + ":" +t , 1)
    }
      .reduceByKey(_ + _)
      .map(x => x._2 -> x._1)
      .sortByKey(true)*/

      /*val mapper: ObjectMapper = new ObjectMapper() with ScalaObjectMapper
      mapper.registerModule(DefaultScalaModule)
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

      var os = fs.create(new Path("hdfs:///user/hdfs/count-per-site"))
      mapper.writeValue(os, ents)
      os.close()*/

      /*val globalCount = ents
        .groupBy(_._1)
        .map {
        x =>
          var count = 0L
          for (site <- x._2) {
            count+=1
          }
          (x._1, count)
      }
        .sortBy(_._2, ascending=false)
        .saveAsTextFile("hdfs:///user/hdfs/count-global")*/

      println("\n\u001B[32m" + "---Done---" + "\u001B[0m\n")
      Thread.sleep(sleepTime)

      // Stop app automatically when finished
      sc.stop()
    catch {
      case ex: Exception => ex.printStackTrace()
    }
  }

  def extractEntities(rdd: RDD[(Text, ArchiveReader)]) = {
    val extract = rdd.flatMap {
      case (text, reader) => reader.iterator().toSeq
    }
      .map(CARProducer.build)
      .filter(ar => {
        ar.header("WARC-Type").equals("response")
      })
      .map(ar => {
        var bodyText: String = null
        try {
          val resp = ApacheHTTPFactory.createResponse(ar.body)

          if (resp.containsHeader("Content-Type") && resp.getFirstHeader("Content-Type").getValue.startsWith("text/html")) {
            try {
              val bi = new BufferedInputStream(resp.getEntity.getContent)
              bodyText = ArticleExtractor.getInstance().getText(new InputStreamReader(bi))
            } catch {
              case ex: Exception =>
            }
          }
        } catch {
          case ex: Exception =>

        }

        (ar.header("WARC-Target-URI").toString, bodyText)
      }
      )
      .filter {
        case (uri, bodyText) => bodyText != null
        case _ => false
      }
      //    Repartition:
      //    Reshuffle the data in the RDD randomly to create either more or fewer partitions and balance it across them.
      //    This always shuffles all data over the network.
      .repartition(numCores)
      .flatMap { case (uri: String, bodyText: String) => {

        assert(bodyText != null)
        assert(ClassifierStore.get_TL() != null)

        val classifiers = ClassifierStore.get_TL().classify(bodyText)
        val entities = mutable.MutableList[((String, String), String)]()

        for (sentence <- classifiers) {
          for (word <- sentence) {
            if (word.get(classOf[CoreAnnotations.AnswerAnnotation]) != "O") {
              entities += (((word.word(), word.get(classOf[CoreAnnotations.AnswerAnnotation])), new URI(uri).getHost))
            }
          }
        }

        entities

      }
      }

    extract.checkpoint()
    extract
  }

  def genMapper[A, B](f: A => B): A => B = {
    val locker = com.twitter.chill.MeatLocker(f)
    x => locker.get.apply(x)
  }
}
