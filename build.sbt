name := "PageCruncher"

version := "1.2.1"

scalaVersion := "2.11.12"

target := file("/tmp/target/")

libraryDependencies += "org.netpreserve.commons" % "webarchive-commons" % "1.1.8" exclude("org.apache.hadoop", "hadoop-core")

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.2.1" % "provided"

libraryDependencies += "org.apache.httpcomponents" % "httpcore" % "4.4.8"

libraryDependencies += "com.syncthemall" % "boilerpipe" % "1.2.2"

libraryDependencies += "xerces" % "xercesImpl" % "2.11.0"

libraryDependencies += "net.sourceforge.nekohtml" % "nekohtml" % "1.9.22"

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}