name := "PageCruncher"

scalaVersion := "2.10.5"

target := file("/tmp/target/")

libraryDependencies += "org.netpreserve.commons" % "webarchive-commons" % "1.1.5" exclude("org.apache.hadoop", "hadoop-core")

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.0-cdh5.4.4" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.3.0-cdh5.4.4" % "provided"

libraryDependencies += "org.apache.httpcomponents" % "httpcore" % "4.4.1"

libraryDependencies += "de.l3s.boilerpipe" % "boilerpipe" % "1.2.0"

libraryDependencies += "xerces" % "xercesImpl" % "2.11.0"

libraryDependencies += "net.sourceforge.nekohtml" % "nekohtml" % "1.9.17"

//libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.3.3"

//libraryDependencies += "com.twitter" %% "chill" % "0.6.0"

resolvers += "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

resolvers += "Boilerpipe" at "http://boilerpipe.googlecode.com/svn/repo/"

net.virtualvoid.sbt.graph.Plugin.graphSettings

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
