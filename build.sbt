val scala3Version = "3.5.2"
val pekkoVersion = "1.1.2"
val scalaTestVersion = "3.2.14"

lazy val root = project
  .in(file("."))
  .settings(
    name := "pekko-blob-storage",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.24.1",
    libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.24.1",
    libraryDependencies += "org.apache.logging.log4j" % "log4j-slf4j18-impl" % "2.18.0",


    libraryDependencies += "org.apache.pekko" %% "pekko-actor-typed" % pekkoVersion,
    libraryDependencies += "org.apache.pekko" %% "pekko-stream" % pekkoVersion,
    libraryDependencies += "org.apache.pekko" %% "pekko-actor-testkit-typed" % pekkoVersion % Test,
    libraryDependencies += "org.apache.pekko" %% "pekko-persistence-typed" % pekkoVersion,
    libraryDependencies += "org.apache.pekko" %% "pekko-persistence-testkit" % pekkoVersion % Test,

    libraryDependencies += "org.scalameta" %% "munit" % "1.0.2" % Test,
    libraryDependencies += "org.scalatest" %% "scalatest" % scalaTestVersion,
  )
