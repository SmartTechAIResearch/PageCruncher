package be.sizingservers.pagecruncher

import edu.stanford.nlp.io.StringOutputStream
import org.archive.io.ArchiveRecord

import scala.collection.JavaConversions._

object CARProducer {
  def build(ar: ArchiveRecord): ContainedArchiveRecord = {
    val os = new StringOutputStream()
    ar.dump(os)
    val str = os.toString
    ContainedArchiveRecord(ar.getHeader.getHeaderFields.toMap, str)
  }
}

case class ContainedArchiveRecord(header: Map[String, AnyRef], body: String) {

}
