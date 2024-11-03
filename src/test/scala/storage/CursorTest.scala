package com.github.sdreynolds.blob.storage

import java.nio.file.Files
// Files.createTempFile()
import scala.util.Using
import java.io.RandomAccessFile

class CursorTestSuite extends munit.FunSuite {
  test("Read an Empty file") {
    Using.resource(BitCaskCursor.createCursorForFile(Files.createTempFile("store", "bitcask").toFile())) { cursor =>
      assertEquals(cursor.read(0, 15).getRecord, None)
    }
  }

  test("Write a record to a new file") {

    Using.resource(BitCaskCursor.createCursorForFile(Files.createTempFile("store", "bitcask").toFile())) { cursor =>
      val response = cursor.write("RecordKey".getBytes(), "RecordValue".getBytes()).makeReadOnly()
      val read = response.read(0, 14 + "RecordKey".getBytes.length + "RecordValue".getBytes.length)
      assert(read.getRecord.isDefined)

      val readResult = read.getRecord.get
      assertEquals(readResult.record.get.toSeq, "RecordValue".getBytes().toSeq)
    }
  }

  test("1000 Writes and reads") {
    Using.resource(new RandomAccessFile(Files.createTempFile("store", ".bitcask").toFile(), "rw")) {tempFile =>
      var cursor = BitCaskCursor.createCursorForRandomFile(tempFile)
      for (i <- 0 until 1000) {
        val keyName = s"Awesome$i"
        val value = s"someValue$i"
        cursor = cursor.write(keyName.getBytes(), value.getBytes())
      }

      // @TODO: read the data back
    }
  }
}
