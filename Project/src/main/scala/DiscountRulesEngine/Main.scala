package DiscountRulesEngine

import scala.util.{Try, Success, Failure}
import scala.collection.parallel.CollectionConverters._
import FileIO.{processFileInChunks, logEvent}
import DiscountRules.{rules, calculateFinalDiscount}

object Main {
  //---------------------------------------------------------------------------------------------//
  // Orchestrator
  def main(args: Array[String]): Unit = {
    logEvent("INFO", "Engine started for processing.")

    val dbProcess = for {
      conn <- DatabaseManager.getConnection()
      _ <- DatabaseManager.initializeTables(conn)
    } yield conn

    dbProcess match {
      case Success(conn) =>
        logEvent("INFO", "Database connection secured and tables initialized.")
        var totalProcessed = 0
        var totalSaved = 0

        val processResult = processFileInChunks("src/main/resources/TRX10M.csv", 10000) { chunk =>
          val transactionsWithDiscounts = chunk.par.map { t =>
            val finalDiscount = calculateFinalDiscount(t, rules)
            val finalPrice = t.unitPrice - (t.unitPrice * finalDiscount)

            if (finalDiscount > 0.0) {
              logEvent("INFO", s"Transaction processed: ${t.productName} got a discount of ${finalDiscount * 100}%.")
            }

            t.copy(discount = finalDiscount, finalPrice = finalPrice)
          }.toList

          val discountedTransactionsOnly = transactionsWithDiscounts.filter(_.discount > 0.0)

          DatabaseManager.saveTransactions(conn, discountedTransactionsOnly) match {
            case Success(_) =>
              totalProcessed += chunk.length
              totalSaved += discountedTransactionsOnly.length
            case Failure(e) =>
              logEvent("ERROR", s"Failed to save chunk to DB: ${e.getMessage}")
          }
        }

        processResult match {
          case Success(_) =>
            logEvent("INFO", s"COMPLETE! Total Processed: $totalProcessed | Total Saved: $totalSaved.")
          case Failure(e) =>
            logEvent("ERROR", s"File processing crashed: ${e.getMessage}")
        }

        conn.close()

      case Failure(exception) =>
        logEvent("ERROR", s"Initial Database Error: ${exception.getMessage}")
    }
  }
}