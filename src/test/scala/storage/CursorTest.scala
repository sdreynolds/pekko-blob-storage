package com.github.sdreynolds.blob.storage

import java.nio.file.Files
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
    val tempFile = Files.createTempFile("store", ".bitcask").toFile()
    var cursor: Option[WritableBitCaskCursor] = None
    try {
      cursor = Some(BitCaskCursor.createCursorForFile(tempFile))
      for (i <- 0 until 1000) {
        val keyName = s"Awesome$i"
        val value = s"someValue$i"
        cursor = cursor.map(_.write(keyName.getBytes(), value.getBytes()))
      }

      val index = cursor.get.loadFile()
      assertEquals(index.get(Bytes("Awesome0".getBytes())).get.position, 0L)
      assertEquals(index.get(Bytes("Awesome0".getBytes())).get.length, 32)

      val lastKey = Bytes("Awesome999".getBytes())
      assertEquals(index.get(lastKey).get.length, 36)
      val databaseRead = cursor.get.read(index.get(lastKey).get.position, index.get(lastKey).get.length)

      assertEquals(databaseRead.getRecord.get.record.get.toSeq, "someValue999".getBytes().toSeq)
    } finally {
      cursor.map(_.close())
    }
  }
}
