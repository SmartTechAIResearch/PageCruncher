package be.sizingservers.pagecruncher

import java.io.InputStreamReader

import de.l3s.boilerpipe.extractors.ArticleExtractor
import edu.stanford.nlp.ie.AbstractSequenceClassifier
import edu.stanford.nlp.ie.crf.CRFClassifier
import edu.stanford.nlp.io.StringOutputStream
import edu.stanford.nlp.ling.{CoreAnnotations, CoreLabel}
import org.archive.io.ArchiveRecord
import org.archive.io.warc.{WARCReader, WARCReaderFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Created by wannes on 7/1/15.
 */
object CruchApp {
  def main(args: Array[String]): Unit = {
    try {
      //val sparkConf = new SparkConf().setAppName("PageCruncher") //.setMaster("local[2]")
      //val sc = new SparkContext(sparkConf)


      val serializedClassifier = "/home/wannes/Dropbox/PageCruncher/lib/classifiers/english.muc.7class.distsim.crf.ser.gz"
      val classifier:AbstractSequenceClassifier[CoreLabel] = CRFClassifier.getClassifier(serializedClassifier)


      //val hbaseConf = HBaseConfiguration.create()
      //hbaseConf.addResource(new Path("/etc/hbase/conf/core-site.xml"))
      //hbaseConf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))

      val reader: WARCReader = WARCReaderFactory.get("/home/wannes/Downloads/CC-MAIN-20150501060904-00075-ip-10-235-10-82.ec2.internal.warc.gz")
      //val rdd = sc.makeRDD(reader.toSeq)

      /*rdd.foreach({
        ar =>
          if (ar.hasContentHeaders && ar.getHeader.getHeaderValue("WARC-Type").equals("response")) {
            println(ar.getHeader.getHeaderValue("WARC-Target-URI"))
          }
      })*/

      val responses:mutable.MutableList[(String, List[(String, String)])] = mutable.MutableList()

      for (split: ArchiveRecord <- reader.iterator()) {
        if (split.hasContentHeaders && split.getHeader.getHeaderValue("WARC-Type").equals("response")) {
          println(split.getHeader.getHeaderValue("WARC-Target-URI"))
          val entities = mutable.MutableList[(String, String)]()
          val os = new StringOutputStream()
          split.dump(os)
          val str = os.toString

          val resp = ApacheHTTPFactory.createResponse(str)
          if (resp.containsHeader("Content-Type") && resp.getFirstHeader("Content-Type").getValue.startsWith("text/html")) {
            try {
              val bodyText: String = ArticleExtractor.getInstance().getText(new InputStreamReader(resp.getEntity.getContent))
              val classifiers = classifier.classify(bodyText)

              for (sentence <- classifiers) {
                for (word <- sentence) {
                  if (word.get(classOf[CoreAnnotations.AnswerAnnotation]) != "O") {
                    System.out.print(word.word() + '/' + word.get(classOf[CoreAnnotations.AnswerAnnotation]) + ' ')
                    entities += ((word.word(), word.get(classOf[CoreAnnotations.AnswerAnnotation])))
                  }

                }
              }

              println("poop")
              responses += ((split.getHeader.getHeaderValue("WARC-Target-URI").toString, entities.toList))
            } catch {
              case ex:Exception =>
            }
          }
        }
      }

    } catch {
      case ex: Exception => ex.printStackTrace()
    }
  }
}
