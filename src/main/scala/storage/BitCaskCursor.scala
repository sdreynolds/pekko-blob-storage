package com.github.sdreynolds.blob.storage

import java.io.File
import java.io.RandomAccessFile
import scala.collection.mutable.HashMap
import java.util.zip.CRC32
import java.io.IOException
import org.apache.logging.log4j.LogManager
import java.util.Arrays

object BitCaskCursor {
  final val TOMBSTONE = Array(0xFF.toByte, 0xFF.toByte, 0xFF.toByte, 0xFF.toByte)
  final val TOMBSTONE_SIZE = TOMBSTONE.length
  final val HEADER_SIZE = 14

  def createCursorForFile(file: File): WritableBitCaskCursor = {
    new WritableBitCaskCursor(new RandomAccessFile(file, "rw"), file, 0, None, None)
  }

  def readUInt32(a: Byte, b: Byte, c: Byte, d: Byte) = {
    (a & 0xFF) << 24 | (b & 0xFF) << 16 | (c & 0xFF) << 8 | (d & 0xFF) << 0
  }

  def readUInt16(a: Byte, b: Byte) = (a & 0xFF) << 8 | (b & 0xFF) << 0

  def writeInt32(value: Int, buffer: Array[Byte], start: Int) = {
    buffer.update(start, (value >>> 24).toByte)
    buffer.update(start + 1, (value >>> 16).toByte)
    buffer.update(start + 2, (value >>> 8).toByte)
    buffer.update(start + 3, value.byteValue)
  }

  def writeInt32(value: Long, buffer: Array[Byte], start: Int) = {
    buffer.update(start, (value >>> 24).toByte)
    buffer.update(start + 1, (value >>> 16).toByte)
    buffer.update(start + 2, (value >>> 8).toByte)
    buffer.update(start + 3, value.toByte)
  }

  def writeInt16(value: Int, buffer: Array[Byte], start: Int) = {
    buffer.update(start, (value >>> 8).toByte)
    buffer.update(start + 1, value.toByte)
  }
}

case class BitCaskWriteResult(offset: Long, recordSize: Int, timestamp: Int)
case class BitCaskReadResult(record: Option[Array[Byte]])

class ReadOnlyBitCaskCursor(fileHandle: RandomAccessFile, file: File, lastRead: Option[BitCaskReadResult])
    extends BitCaskCursor(fileHandle, file, lastRead) {

  def read(position: Long, recordTotalLength: Int): BitCaskCursor = {
    if position + recordTotalLength > fileHandle.length() then
      this
    else
      val newRead = readInternal(position, recordTotalLength)
      new ReadOnlyBitCaskCursor(fileHandle, file, Some(BitCaskReadResult(newRead)))
  }
}

class WritableBitCaskCursor(fileHandle: RandomAccessFile,
  file: File,
  writePosition: Long,
  lastWrite: Option[BitCaskWriteResult],
  lastRead: Option[BitCaskReadResult]) extends BitCaskCursor(fileHandle, file, lastRead) {

  def getWriteOffset = lastWrite.map(r => r.offset)
  def getWriteSize = lastWrite.map(r => r.recordSize)

  // Catch IOException and close the file and exit hard!
  def read(position: Long, recordTotalLength: Int): BitCaskCursor = {
    if position + recordTotalLength > fileHandle.length() then return this

    val newRead = readInternal(position, recordTotalLength)
    new WritableBitCaskCursor(fileHandle, file, writePosition, None,
      Some(BitCaskReadResult(newRead)))
  }

  def delete(key: Array[Byte]): WritableBitCaskCursor = write(key, BitCaskCursor.TOMBSTONE)

  def writeAtTimestamp(key: Array[Byte], value: Array[Byte], timestamp: Int): WritableBitCaskCursor = {
    val keySize = key.length
    val valueSize = value.length
    logger.debug("writing sizes of {} and {}", keySize, valueSize)

    // @TODO: Check the size of the buffers to constraint to ints

    val length = keySize + valueSize + BitCaskCursor.HEADER_SIZE// 4 + 4 + 2 + 4 bytes

    val internalBuffer = new Array[Byte](length)

    BitCaskCursor.writeInt32(timestamp, internalBuffer, 4)
    BitCaskCursor.writeInt16(keySize, internalBuffer, 8)
    BitCaskCursor.writeInt32(valueSize, internalBuffer, 10)
    key.copyToArray(internalBuffer, BitCaskCursor.HEADER_SIZE)
    value.copyToArray(internalBuffer, BitCaskCursor.HEADER_SIZE + keySize)

    val crc = new CRC32()
    crc.update(internalBuffer, 4, length - 4)
    val crcValue = crc.getValue

    BitCaskCursor.writeInt32(crcValue, internalBuffer, 0)

    if lastRead.isDefined then
      fileHandle.seek(writePosition)

    fileHandle.write(internalBuffer)

    new WritableBitCaskCursor(fileHandle, file, writePosition + length,
      Some(BitCaskWriteResult(writePosition, length, timestamp)),
      None)
  }

  def write(key: Array[Byte], value: Array[Byte]): WritableBitCaskCursor = {
    val timestamp = (System.currentTimeMillis / 1000).toInt
    writeAtTimestamp(key, value, timestamp)
  }

  def makeReadOnly(): ReadOnlyBitCaskCursor = new ReadOnlyBitCaskCursor(fileHandle, file, lastRead)

}

abstract class BitCaskCursor(
  fileHandle: RandomAccessFile,
  file: File,
  lastRead: Option[BitCaskReadResult])
    extends AutoCloseable {

  protected val logger = LogManager.getLogger(classOf[BitCaskCursor])

  def getRecord = lastRead

  def close() = fileHandle.close()

  def deleteFile(): Boolean = {
    close()
    file.delete()
  }

  def read(position: Long, recordTotalLength: Int): BitCaskCursor

  protected def readInternal(position: Long, recordTotalLength: Int): Option[Array[Byte]] = {
    // creating an array buffer for this read is wasteful, we should pass a
    // buffer around
    val internalBuffer = new Array[Byte](recordTotalLength)

    fileHandle.seek(position)

    val bytesRead = fileHandle.read(internalBuffer, 0, recordTotalLength)

    val expectedCrc = BitCaskCursor.readUInt32(internalBuffer(0), internalBuffer(1), internalBuffer(2), internalBuffer(3))
    val crc = new CRC32
    // Read the rest of the buffer and put it in the CRC
    crc.update(internalBuffer, 4, recordTotalLength - 4)

    if crc.getValue.toInt != expectedCrc then
      throw new IOException("CRC for record did not match CRC on disk")

    val timestamp = BitCaskCursor.readUInt32(internalBuffer(4), internalBuffer(5), internalBuffer(6), internalBuffer(7))
    val keySize = BitCaskCursor.readUInt16(internalBuffer(8), internalBuffer(9))
    val valueSize = BitCaskCursor.readUInt32(internalBuffer(10), internalBuffer(11), internalBuffer(12), internalBuffer(13))

    val value = new Array[Byte](valueSize)
    Array.copy(internalBuffer, BitCaskCursor.HEADER_SIZE + keySize, value, 0, valueSize)

    // Check for deletion
    if valueSize == BitCaskCursor.TOMBSTONE_SIZE && Arrays.equals(value, BitCaskCursor.TOMBSTONE) then
      logger.debug("Value at {} has been deleted", position)
      None
    else
      Some(value)
  }

  def loadFile(): HashMap[Bytes, ValueLocation] = {
    val index: HashMap[Bytes, ValueLocation] = new HashMap()
    fileHandle.seek(0)

    val internalBuffer = new Array[Byte](BitCaskCursor.HEADER_SIZE)

    // valueBuffer is kept internally and never handed off
    val valueBuffer = new Array[Byte](255)

    while (fileHandle.getFilePointer() < fileHandle.length()) {

      val position = fileHandle.getFilePointer()

      val headerReadResult = fileHandle.read(internalBuffer, 0, BitCaskCursor.HEADER_SIZE)

      if headerReadResult < 0 then
        throw new RuntimeException("Failed to read the header")

      val expectedCrc = BitCaskCursor.readUInt32(internalBuffer(0), internalBuffer(1), internalBuffer(2), internalBuffer(3))
      val timestamp = BitCaskCursor.readUInt32(internalBuffer(4), internalBuffer(5), internalBuffer(6), internalBuffer(7))

      val keySize = BitCaskCursor.readUInt16(internalBuffer(8), internalBuffer(9))
      val keyBuffer = new Array[Byte](keySize)
      val keyReadResult = fileHandle.read(keyBuffer, 0, keySize)

      val valueSize = BitCaskCursor.readUInt32(internalBuffer(10), internalBuffer(11), internalBuffer(12), internalBuffer(13))


      val location = ValueLocation(position, valueSize + keySize + BitCaskCursor.HEADER_SIZE, timestamp, Some(this))

      val valueReadResult = fileHandle.read(valueBuffer, 0, valueSize)

      val crc = new CRC32
      // Timestamp plus key and value sizes
      crc.update(internalBuffer, 4, 10)
      // Key bytes
      crc.update(keyBuffer, 0, keySize)
      // value bytes
      crc.update(valueBuffer, 0, valueSize)

      if crc.getValue.toInt != expectedCrc then
        throw new IOException("CRC for record did not match CRC on disk")

      index += (Bytes(keyBuffer) -> location)
    }

    // At the end of this the cursor is at the end of the file where writes happen.
    // therefore, no internal state has changed and the cursor can be used for writing
    index
  }
}
