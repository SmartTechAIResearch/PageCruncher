package be.sizingservers.pagecruncher

import java.io.{BufferedInputStream, InputStreamReader}
import java.net.URI

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
case class Entity(label:String, etype:String)

object SparkApp {

  var pathPrefix = "hdfs:///user/hdfs/CommonCrawl/"
  var numCores = 2

  def getPathPrefix():String = pathPrefix

  def main(args: Array[String]): Unit = {
    try {
      if (args.length > 1) pathPrefix = args(0) + "/"
      if (args.length == 2) numCores = Integer.parseInt(args(1))

      printf("Path: %s, numCores %d\n", pathPrefix, numCores)

      val sparkConf = new SparkConf()
        .setAppName("PageCruncher")
      val sc = new SparkContext(sparkConf)
      if (pathPrefix.startsWith("hdfs")) {
        sc.setCheckpointDir("hdfs:///user/hdfs/checkpoint")
      } else {
        sc.setCheckpointDir(pathPrefix + "checkpoint")
      }

      val conf = new Configuration()
      val fs: FileSystem = FileSystem.get(conf)
      val crawlFiles = mutable.MutableList[String]()
      ClassifierStore.hadoopConf = conf

      //the second boolean parameter here sets the recursion to true
      val fileStatusListIterator: RemoteIterator[LocatedFileStatus] = fs.listFiles(new Path(pathPrefix), false)
      while (fileStatusListIterator.hasNext) {
        val fileStatus: LocatedFileStatus = fileStatusListIterator.next
        if (fileStatus.getPath.getName.startsWith("CC")) {
          crawlFiles += fileStatus.getPath.toString
        }
      }

      val ent = crawlFiles.map(sc.newAPIHadoopFile(_, classOf[WARCFileInputFormat[Text, ArchiveReader]], classOf[Text], classOf[ArchiveReader]))
        .map(extractEntities)
        .toSeq

      println("Group/sort")

      val res = sc.union(ent)
        .groupBy(_._1)
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
          x.map(_._2).groupBy(identity).mapValues(_.size).map{c => ((c._1, k), c._2)}
      }
        .sortBy(_._2, ascending=false)

      println("Initial processing done")

      res
        .saveAsTextFile(pathPrefix + "count-per-site")

      print("Site count saved")

      val si = res.keys.map(_._1).distinct().map(x => (x.hashCode, x))
      val ei = res.keys.map(_._2).distinct().map(x => (x.hashCode, x))

      println("Indexes created")

      //val sei = si.join(ei)

      //println("Lookup table joined")

      val ratings = res
        .map{x =>
        Rating(x._1._1.hashCode, x._1._2.hashCode, x._2)
      }

      println("Created ratings")

      val model = ALS.train(ratings, 10, 20, 1)

      println("Trained model")

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
      println("Mean Squared Error = " + MSE)

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
      .saveAsTextFile(pathPrefix + "predictions")
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


      println("done")
    } catch {
      case ex: Exception => ex.printStackTrace()
    }
  }

  def extractEntities(rdd:RDD[(Text, ArchiveReader)]) = {
    val extract = rdd.flatMap{
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
        case ex:Exception =>

      }

      (ar.header("WARC-Target-URI").toString, bodyText)
    }
      )
      .filter {
      case (uri, bodyText) => bodyText != null
      case _ => false
    }
      .repartition(numCores)
      .flatMap{ case (uri:String, bodyText:String) => {

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

    }}

    extract.checkpoint()
    extract
  }

  def genMapper[A, B](f: A => B): A => B = {
    val locker = com.twitter.chill.MeatLocker(f)
    x => locker.get.apply(x)
  }
}
