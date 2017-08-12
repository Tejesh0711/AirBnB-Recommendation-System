
name := """airbnb"""

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  cache,
  ws,
  "org.scalatestplus.play" %% "scalatestplus-play" % "1.5.1" % Test

)

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"





libraryDependencies ++= Seq(
  "com.adrianhurt" %% "play-bootstrap" % "1.1-P25-B3"
)

//Dependencies for slick,play and mySQL
libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.34"
libraryDependencies += "com.typesafe.play" %% "play-slick" % "2.0.0"
libraryDependencies += "com.typesafe.play" %% "play-slick-evolutions" % "2.0.0"
// https://mvnrepository.com/artifact/org.apache.commons/commons-lang3
libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.0"
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.4.17"
// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.0.0"


// https://mvnrepository.com/artifact/org.apache.hbase/hbase
libraryDependencies += "org.apache.hbase" % "hbase" % "1.2.4"

// https://mvnrepository.com/artifact/org.apache.hbase/hbase-shaded-client
libraryDependencies += "org.apache.hbase" % "hbase-shaded-client" % "1.2.4"

// https://mvnrepository.com/artifact/org.apache.hbase/hbase-client
libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.2.4"

// https://mvnrepository.com/artifact/org.apache.hbase/hbase-server
libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.2.4" excludeAll  ExclusionRule(organization = "org.mortbay.jetty")

// https://mvnrepository.com/artifact/org.apache.hbase/hbase-common
libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.2.4"


libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.6.0" excludeAll (ExclusionRule(organization = "org.eclipse.jetty"))

libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.6.0"


//// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-core
//libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "1.2.1"

//spark
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.1.0"

//dependency override for jackson
dependencyOverrides ++= Set("com.fasterxml.jackson.core" % "jackson-databind" % "2.6.1")

//fixing java.lang.ClassNotFoundException: de.unkrig.jdisasm.Disassembler
libraryDependencies += "org.codehaus.janino" % "janino" % "3.0.7"


