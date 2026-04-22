package DiscountRulesEngine

import scala.io.Source
import scala.util.Try
import java.io.{FileWriter, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import Domain.Transaction

object FileIO {

  // Function to read the file in chunks, process, and avoid memory crashes
  def processFileInChunks(filePath: String, chunkSize: Int = 10000)(processChunk: List[Transaction] => Unit): Try[Unit] = {
    Try {
      val source = Source.fromFile(filePath)
      val lines = source.getLines().drop(1).filter(_.trim.nonEmpty)

      lines.grouped(chunkSize).foreach { chunk =>
        val transactions = chunk.flatMap { line =>
          Try {
            val columns = line.split(",")
            Transaction(columns(0), columns(1), columns(2), columns(3).toInt, columns(4).toDouble, columns(5), columns(6))
          }.toOption
        }.toList

        processChunk(transactions)
      }

      source.close()
    }
  }

  //-------------------------------------------------------------------------------------//
  // Logging function
  def logEvent(level: String, message: String): Try[Unit] = {
    Try {
      val fileWriter = new FileWriter("src/main/resources/rules_engine.log", true)
      val printWriter = new PrintWriter(fileWriter)
      val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

      printWriter.println(s"$timestamp $level $message")
      printWriter.close()
    }
  }
}