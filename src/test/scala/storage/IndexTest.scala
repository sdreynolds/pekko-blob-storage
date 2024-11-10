package com.github.sdreynolds.blob.storage

import java.nio.file.Files
import scala.util.Using

class IndexTest extends munit.FunSuite {
  test("Write and read a record back") {
    Using.resource(BitCask.Index.createIndexFromDirectory(Files.createTempDirectory("dbfiles"))) { index =>
      Using.resource(index.write("indexTestingKey".getBytes, "indexValue".getBytes)) { updatedIndex =>
        assertEquals(updatedIndex.read("indexTestingKey".getBytes).get.toSeq, "indexValue".getBytes.toSeq)
      }
    }
  }

  test("Read Key that doesn't exist") {
    Using.resource(BitCask.Index.createIndexFromDirectory(Files.createTempDirectory("dbfiles"))) { index =>
      assertEquals(index.read("Doesn't Exist".getBytes), None)
    }
  }

  test("Delete a key already commited") {
    val keyBytes = "indexTestingKey".getBytes
    Using.resource(BitCask.Index.createIndexFromDirectory(Files.createTempDirectory("dbfiles"))) { index =>
      Using.resource(index.write(keyBytes, "indexValue".getBytes)) { afterWrite =>
        assertEquals(afterWrite.read(keyBytes).get.toSeq, "indexValue".getBytes.toSeq)
        Using.resource(afterWrite.delete(keyBytes)) { afterDelete =>
          assertEquals(afterDelete.read(keyBytes), None)
        }
      }
    }
  }

  test("Can still read data from a compact index after a new file is rolled") {
    Using.resource(BitCask.Index.createIndexFromDirectory(Files.createTempDirectory("dbfiles"))) { index =>
      val keyBytes = "indexTestingKey".getBytes

      // Need to roll the file to move it to a read only file
      Using.resource(index
        .write(keyBytes, "indexValue".getBytes)
        .rollNewWriteFile()
        .compactIndex()) {compacted =>
        assertEquals(compacted.read(keyBytes).get.toSeq, "indexValue".getBytes.toSeq)
      }
    }
  }

  test("Data in the current write cursor is not present in the compacted index") {
    Using.resource(BitCask.Index.createIndexFromDirectory(Files.createTempDirectory("dbfiles"))) { index =>
      val keyBytes = "indexTestingKey".getBytes

      // Do not roll the index, keeping the current write cursor active
      Using.resource(index.write(keyBytes, "indexValue".getBytes).compactIndex()) { compacted =>

        assertEquals(compacted.read(keyBytes), None)
      }
    }
  }
}
