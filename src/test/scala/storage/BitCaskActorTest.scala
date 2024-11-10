package com.github.sdreynolds.blob.storage

import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.scalatest.wordspec.AnyWordSpecLike
import java.nio.file.Files

class BitCaskActorSuite extends ScalaTestWithActorTestKit with AnyWordSpecLike {
  "BitCaskActor" must {
    "Write and read what is stored" in {
      val expectedKey = "actorKey".getBytes()
      val expectedValue = "actorValue".getBytes()

      // @TODO: how to clean up temporary files
      val directory = Files.createTempDirectory("actorTesting")
      val storage = testKit.spawn(BitCask(directory, "testCast"))

      val storageProbe = testKit.createTestProbe[BitCask.Response]()

      storage ! BitCask.Write(expectedKey, expectedValue)
      storage ! BitCask.Read(expectedKey, storageProbe.ref)

      storageProbe.expectMessage(BitCask.Value(Bytes(expectedValue)))

    }

    "Write and delete what is stored" in {
      val expectedKey = "actorKey".getBytes()
      val expectedValue = "actorValue".getBytes()

      // @TODO: how to clean up temporary files
      val directory = Files.createTempDirectory("actorTesting")
      val storage = testKit.spawn(BitCask(directory, "testCast"))

      val storageProbe = testKit.createTestProbe[BitCask.Response]()

      storage ! BitCask.Write(expectedKey, expectedValue)
      storage ! BitCask.Delete(expectedKey)
      storage ! BitCask.Read(expectedKey, storageProbe.ref)

      storageProbe.expectMessage(BitCask.NoValue)

    }

    "Handle delete request for a key that doesn't exist" in {
      val expectedKey = "actorKey".getBytes()

      // @TODO: how to clean up temporary files
      val directory = Files.createTempDirectory("actorTesting")
      val storage = testKit.spawn(BitCask(directory, "testCast"))

      storage ! BitCask.Delete(expectedKey)
    }
  }

  "Write, then compact and then read what is stored" in {
    val expectedKey = "actorKey".getBytes()
    val expectedValue = "actorValue".getBytes()

    // @TODO: how to clean up temporary files
    val directory = Files.createTempDirectory("actorTesting")
    val storage = testKit.spawn(BitCask(directory, "testCast"))

    val storageProbe = testKit.createTestProbe[BitCask.Response]()

    storage ! BitCask.Write(expectedKey, expectedValue)

    storage ! BitCask.SyncCompaction
    storage ! BitCask.Read(expectedKey, storageProbe.ref)

    storageProbe.expectMessage(BitCask.Value(Bytes(expectedValue)))

  }
}
