package com.github.sdreynolds.blob.storage

import java.nio.file.Files
import scala.util.Using
import munit._

class CompactionTest extends FunSuite {

  val cursor = new Fixture[WritableBitCaskCursor]("cursors") {
    var writableCursor: WritableBitCaskCursor = null

    def apply() = writableCursor
    override def beforeEach(context: BeforeEach): Unit = {
      writableCursor =
        BitCaskCursor.createCursorForFile(Files.createTempFile("store", "bitcask").toFile())
    }
    override def afterEach(context: AfterEach): Unit = {
      writableCursor.deleteFile()
    }
  }

  override def munitFixtures = List(cursor)


  test("Given empty Seq of Locations, return empty cursor") {
    Using.resource(BitCaskCursor.compactLocations(Seq(), Files.createTempFile("store", "bitcask").toFile())) {
      readOnlyCursor => {
        assertEquals(readOnlyCursor.read(0, 15).getRecord, None)
      }
    }
  }

  test("Compact single file into a new file") {
    val keyBytes = "RecordKey".getBytes()
    val valueBytes = "RecordValue".getBytes()
    val timestamp = 1731693736

    val readOnly = cursor().writeAtTimestamp(keyBytes, valueBytes, timestamp).makeReadOnly()

    val length = keyBytes.length + valueBytes.length + BitCaskCursor.HEADER_SIZE
    val location = ValueLocation(0, length, timestamp, Some(readOnly))
    val compactedCursor = BitCaskCursor.compactLocations(Seq(location),
      Files.createTempFile("store", "bitcask").toFile())

    try {
      assert(compactedCursor.read(0, length).getRecord.isDefined)
    } finally {
      compactedCursor.deleteFile()
    }
  }

  test("After a value has been updated, the compacted file will start at position 0 with latest value") {
    val keyBytes = "RecordKey".getBytes()
    val valueBytes = "RecordValue".getBytes()
    val timestamp = 1731693736

    val readOnly = cursor().writeAtTimestamp(keyBytes, "originalBytes".getBytes(), timestamp)
      .writeAtTimestamp(keyBytes, valueBytes, timestamp + 2)
      .makeReadOnly()

    // Because we are compacting the new value and overriding the old value, move position forward
    // past the first record.
    val position = keyBytes.length + "originalBytes".getBytes().length + BitCaskCursor.HEADER_SIZE

    val length = keyBytes.length + valueBytes.length + BitCaskCursor.HEADER_SIZE
    val location = ValueLocation(position, length, timestamp, Some(readOnly))
    val compactedCursor = BitCaskCursor.compactLocations(Seq(location),
      Files.createTempFile("store", "bitcask").toFile())

    try {
      assert(compactedCursor.read(0, length).getRecord.isDefined)
    } finally {
      compactedCursor.deleteFile()
    }
  }
}
