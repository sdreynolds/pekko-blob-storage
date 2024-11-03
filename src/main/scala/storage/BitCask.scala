package com.github.sdreynolds.blob.storage

import java.io.File
import java.io.RandomAccessFile
import java.util.zip.CRC32
import java.io.IOException
import java.util.RandomAccess
import org.apache.logging.log4j.LogManager;

object BitCaskCursor {
  final val TOMBSTONE = Array(0xFF.toByte, 0xFF.toByte, 0xFF.toByte, 0xFF.toByte)
  final val TOMBSTONE_SIZE = 0xFFFFFFFF
  final val HEADER_SIZE = 14

  def createCursorForFile(file: File): WritableBitCaskCursor = {
    new WritableBitCaskCursor(new RandomAccessFile(file, "rw"), 0, None, None)
  }

  def createCursorForRandomFile(file: RandomAccessFile): WritableBitCaskCursor = {
    new WritableBitCaskCursor(file, 0, None, None)
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

case class BitCaskWriteResult(offset: Int, recordSize: Int, timestamp: Int)
case class BitCaskReadResult(record: Option[Array[Byte]])

class ReadOnlyBitCaskCursor(fileHandle: RandomAccessFile, lastRead: Option[BitCaskReadResult])
    extends BitCaskCursor(fileHandle, lastRead) {
  def read(position: Int, recordTotalLength: Int): BitCaskCursor = {
    if position + recordTotalLength > fileHandle.length() then return this

    val newRead = readInternal(position, recordTotalLength)
    new ReadOnlyBitCaskCursor(fileHandle, Some(BitCaskReadResult(newRead)))
  }
}

class WritableBitCaskCursor(fileHandle: RandomAccessFile,
  writePosition: Int,
  lastWrite: Option[BitCaskWriteResult],
  lastRead: Option[BitCaskReadResult]) extends BitCaskCursor(fileHandle, lastRead) {

  def getWriteOffset = lastWrite.map(r => r.offset)
  def getWriteSize = lastWrite.map(r => r.recordSize)

  // Catch IOException and close the file and exit hard!
  def read(position: Int, recordTotalLength: Int): BitCaskCursor = {
    if position + recordTotalLength > fileHandle.length() then return this

    val newRead = readInternal(position, recordTotalLength)
    new WritableBitCaskCursor(fileHandle, writePosition, None,
      Some(BitCaskReadResult(newRead)))
  }

  def delete(key: Array[Byte]): BitCaskCursor = write(key, BitCaskCursor.TOMBSTONE)

  def write(key: Array[Byte], value: Array[Byte]): WritableBitCaskCursor = {
    val timestamp = (System.currentTimeMillis / 1000).toInt
    val keySize = key.length
    val valueSize = value.length
    logger.info("writing sizes of {} and {}", keySize, valueSize)

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

    logger.info("Writing the following crc value: {}", crcValue)

    BitCaskCursor.writeInt32(crcValue, internalBuffer, 0)

    if lastRead.isDefined then
      fileHandle.seek(writePosition)

    fileHandle.write(internalBuffer)

    logger.info("Cursor is {}", writePosition)
    logger.info("Wrote buffer of {}", internalBuffer)

    new WritableBitCaskCursor(fileHandle, writePosition + length,
      Some(BitCaskWriteResult(writePosition, length, timestamp)),
      None)
  }

  def makeReadOnly(): ReadOnlyBitCaskCursor = new ReadOnlyBitCaskCursor(fileHandle, lastRead)

}

abstract class BitCaskCursor(
  fileHandle: RandomAccessFile,
  lastRead: Option[BitCaskReadResult])
    extends AutoCloseable {

  protected val logger = LogManager.getLogger(classOf[BitCaskCursor])

  def getRecord = lastRead

  def close() = fileHandle.close()

  def read(position: Int, recordTotalLength: Int): BitCaskCursor

  protected def readInternal(position: Int, recordTotalLength: Int): Option[Array[Byte]] = {
    // creating an array buffer for this read is wasteful, we should pass a
    // buffer around
    val internalBuffer = new Array[Byte](recordTotalLength)

    fileHandle.seek(position)

    val bytesRead = fileHandle.read(internalBuffer, 0, recordTotalLength)
    logger.info("Read of buffer: {} -> {}", bytesRead, internalBuffer)

    val expectedCrc = BitCaskCursor.readUInt32(internalBuffer(0), internalBuffer(1), internalBuffer(2), internalBuffer(3))
    val crc = new CRC32
    // Read the rest of the buffer and put it in the CRC
    crc.update(internalBuffer, 4, recordTotalLength - 4)

    logger.info("Read CRC value is {}", crc.getValue.toInt)

    if crc.getValue.toInt != expectedCrc then
      throw new IOException("CRC for record did not match CRC on disk")

    val timestamp = BitCaskCursor.readUInt32(internalBuffer(4), internalBuffer(5), internalBuffer(6), internalBuffer(7))
    val keySize = BitCaskCursor.readUInt16(internalBuffer(8), internalBuffer(9))
    val valueSize = BitCaskCursor.readUInt32(internalBuffer(10), internalBuffer(11), internalBuffer(12), internalBuffer(13))

    valueSize match {
      case BitCaskCursor.TOMBSTONE_SIZE => None
      case _ => {
        logger.info("Value size is {}", valueSize)
        val value = new Array[Byte](valueSize)
        Array.copy(internalBuffer, BitCaskCursor.HEADER_SIZE+ keySize, value, 0, valueSize)
        Some(value)
      }
    }
  }
}
