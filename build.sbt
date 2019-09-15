name := "ofdreports"
version := "0.1"
scalaVersion := "2.12.8"

lazy val sparkVersion = "2.4.2"
lazy val spark = "org.apache.spark"

libraryDependencies += spark %% "spark-core" % sparkVersion % "provided"
libraryDependencies += spark %% "spark-sql"  % sparkVersion % "provided"
libraryDependencies += spark %% "spark-hive" % sparkVersion % "provided"

libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.12.1"
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.12.1"

// assembly plugin:
//resolvers += Resolver.bintrayIvyRepo("com.eed3si9n", "sbt-plugins")
resolvers += Resolver.url("bintray-sbt-plugins", url("https://dl.bintray.com/eed3si9n/sbt-plugins/"))(Resolver.ivyStylePatterns)
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.10" % "provided")

// https://stackoverflow.com/questions/19584686/java-lang-noclassdeffounderror-while-running-jar-from-scala
retrieveManaged := true

