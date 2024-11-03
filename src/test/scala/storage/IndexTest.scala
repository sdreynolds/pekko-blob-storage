package com.github.sdreynolds.blob.storage

import java.nio.file.Files
import scala.util.Using

class IndexTest extends munit.FunSuite {
  test("Write and read a record back") {
    Using.resource(BitCask.Index.createIndexFromDirectory(Files.createTempDirectory("dbfiles"))) { index =>
      val updatedIndex = index
        .write("indexTestingKey".getBytes, "indexValue".getBytes)
        .rollNewWriteFile()
      assertEquals(updatedIndex.read("indexTestingKey".getBytes).get.toSeq, "indexValue".getBytes.toSeq)
    }
  }

  test("Read Key that doesn't exist") {
    Using.resource(BitCask.Index.createIndexFromDirectory(Files.createTempDirectory("dbfiles"))) { index =>
      assertEquals(index.read("Doesn't Exist".getBytes), None)
    }
  }

  test("Delete a key already commited") {
    Using.resource(BitCask.Index.createIndexFromDirectory(Files.createTempDirectory("dbfiles"))) { index =>
      val keyBytes = "indexTestingKey".getBytes
      val afterWrite = index.write(keyBytes, "indexValue".getBytes)
      assertEquals(afterWrite.read(keyBytes).get.toSeq, "indexValue".getBytes.toSeq)

      val afterDelete = afterWrite.delete(keyBytes)
      assertEquals(afterDelete.read(keyBytes), None)
    }
  }
}
