name := "PageCruncher"
version := "1.1.1"

scalaVersion := "2.11.7"

target := file("/tmp/target/")

libraryDependencies += "org.netpreserve.commons" % "webarchive-commons" % "1.1.7" exclude("org.apache.hadoop", "hadoop-core")

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.1.0" % "provided"

libraryDependencies += "org.apache.httpcomponents" % "httpcore" % "4.4.5"

libraryDependencies += "com.syncthemall" % "boilerpipe" % "1.2.2"

libraryDependencies += "xerces" % "xercesImpl" % "2.11.0"

libraryDependencies += "net.sourceforge.nekohtml" % "nekohtml" % "1.9.21"

//libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.3.3"

//libraryDependencies += "com.twitter" %% "chill" % "0.6.0"

//resolvers += "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

resolvers += "Boilerpipe" at "http://boilerpipe.googlecode.com/svn/repo/"

net.virtualvoid.sbt.graph.Plugin.graphSettings

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}