package com.github.sdreynolds.blob.storage

import java.io.File
import java.io.RandomAccessFile
import java.util.zip.CRC32
import java.io.IOException
import java.util.RandomAccess
import org.apache.logging.log4j.LogManager;

import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.persistence.typed.PersistenceId
import org.apache.pekko.persistence.typed.scaladsl.Effect
import org.apache.pekko.persistence.typed.scaladsl.EventSourcedBehavior
import java.nio.file.Path

import scala.jdk.StreamConverters._
import java.nio.file.Files
import scala.collection.mutable.HashMap
import java.util.Arrays
import com.github.sdreynolds.blob.storage.BitCaskCursor.createCursorForFile

object BitCask {
  object Index {
    def createIndexFromDirectory(directory: Path) = {
      val bitcaskFiles = Files.walk(directory, 1).toScala(LazyList)
        .filter(_.endsWith(".bitcask"))
        .sorted

      val index = bitcaskFiles
        .map(path => BitCaskCursor.createCursorForFile(path.toFile()))
        .map(_.makeReadOnly())
        .map(_.loadFile())
        .foldLeft(HashMap[Bytes, ValueLocation]())(_ ++ _)

      val writableCursor = BitCaskCursor.createCursorForFile(directory.resolve(fileName()).toFile())
      new Index(index, writableCursor, directory)
    }

    def fileName(): String = s"bitcaskfilev1_${System.currentTimeMillis()}"
  }

  final class Index(index: HashMap[Bytes, ValueLocation], writableCursor: WritableBitCaskCursor, directory: Path) extends AutoCloseable {

    def close() = {
      index.values.flatMap(location => location.cursor).map(_.close())
      writableCursor.close()
    }

    def write(key: Array[Byte], value: Array[Byte]): Index = {
      val writeResult = writableCursor.write(key, value)
      index += (Bytes(key) -> ValueLocation(writeResult.getWriteOffset.get, writeResult.getWriteSize.get, None))
      new Index(index, writeResult, directory)
    }

    def delete(key: Array[Byte]): Index = {
      val deleteCursor = writableCursor.delete(key)
      index += (Bytes(key) -> ValueLocation(deleteCursor.getWriteOffset.get, deleteCursor.getWriteSize.get, None))
      new Index(index, deleteCursor, directory)
    }

    def read(key: Array[Byte]): Option[Array[Byte]] = {
      index.get(Bytes(key))
        .flatMap(location =>
          location.cursor.getOrElse(writableCursor)
            .read(location.position, location.length).getRecord
        )
        .flatMap(result => result.record)
    }

    def rollNewWriteFile(): Index = {
      val frozenCursor = writableCursor.makeReadOnly()
      val openedCursor = createCursorForFile(directory.resolve(Index.fileName()).toFile())

      index.addAll(
        index
          .filter((key: Bytes, location: ValueLocation) => location.cursor.isEmpty)
          .map((key: Bytes, location: ValueLocation) =>
            (key -> ValueLocation(location.position, location.length, Some(frozenCursor)))))
      new Index(index, openedCursor, directory)
    }

    def compactIndex(): Index = {
      val indicesToCompact = index.filter((key, location) => location.cursor.isDefined)

      val newIndex = Index(
        new HashMap(),
        createCursorForFile(directory.resolve(Index.fileName()).toFile()),
        directory
      )

      indicesToCompact.
        foldLeft(newIndex) {
          case (indexToBuild, (key, location)) => {
            read(key.bytes)
              .map(value => indexToBuild.write(key.bytes, value))
              .getOrElse(indexToBuild)
          }
        }

      indicesToCompact.foreach((key, location) => location.cursor.get.deleteFile())
      newIndex
    }
  }

  def apply(directory: Path, name: String): Behavior[Command] = {
    val index = Index.createIndexFromDirectory(directory)
    internalBehavior(index)
  }

  private def internalBehavior(index: Index): Behavior[Command] = {
    Behaviors.setup { context =>
      context.log.info("Starting BitCask Actor")

      Behaviors.receiveMessage {
        case Read(key, replyTo) => {
          val response = index.read(key).map(value => Value(Bytes(value))).getOrElse(NoValue)
          replyTo ! response
          Behaviors.same
        }
        case Write(key, value) => internalBehavior(index.write(key, value))
        case Delete(key) => internalBehavior(index.delete(key))

        case SyncCompaction => internalBehavior(index.rollNewWriteFile().compactIndex())
      }
    }
  }

  sealed trait Command
  case class Read(key: Array[Byte], replyTo: ActorRef[Response]) extends Command
  case class Write(key: Array[Byte], value: Array[Byte]) extends Command
  case class Delete(key: Array[Byte]) extends Command
  case object SyncCompaction extends Command

  sealed trait Response
  case class Value(value: Bytes) extends Response
  case object NoValue extends Response

}

case class ValueLocation(position: Long, length: Int, cursor: Option[BitCaskCursor])

object Bytes {
  def apply(bytes: Array[Byte]) = new Bytes(bytes)
}

class Bytes(val bytes: Array[Byte]) {
  override def equals(other: Any) = {
    if (!other.isInstanceOf[Bytes]) then
      false
    else
      Arrays.equals(bytes, other.asInstanceOf[Bytes].bytes);
  }

  override def hashCode = Arrays.hashCode(bytes)
}
