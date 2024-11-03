val scala3Version = "3.5.2"

lazy val root = project
  .in(file("."))
  .settings(
    name := "pekko-blob-storage",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.24.1",
    libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.24.1",

    libraryDependencies += "org.scalameta" %% "munit" % "1.0.2" % Test
  )
