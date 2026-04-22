package DiscountRulesEngine

import scala.io.Source
import scala.util.{Try, Success, Failure}
import java.time.ZonedDateTime
import java.time.{ZonedDateTime, LocalDate}
import java.time.temporal.ChronoUnit
import java.io.{FileWriter, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.sql.{Connection, DriverManager, PreparedStatement}
import scala.collection.parallel.CollectionConverters._

object Main {
  case class Transaction(timestamp: String, productName: String, expiryDate: String, quantity: Int, unitPrice: Double, channel: String, paymentMethod: String, discount: Double = 0.0, finalPrice: Double = 0.0)

  // Rule is a tuple of 2 functions, first one returns Bool (rule fit or not) and second returns Double (discount)
  type Rule = (Transaction => Boolean, Transaction => Double)

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

  // ------------------------------------------------------------------------------------------//

  // The Rules..
  // Category rule qualification and calc
  def isCategoryQualified(t: Transaction): Boolean = categoryDiscount(t) > 0.0

  def categoryDiscount(t: Transaction): Double = {
    val category = t.productName.split("-")(0).trim

    if (category == "Cheese") 0.10
    else if (category == "Wine") 0.05
    else 0.0
  }

  // Quantity rule qualification and calc
  def isQuantityQualified(t: Transaction): Boolean = t.quantity >= 6

  def quantityDiscount(t: Transaction): Double = {
    val q = t.quantity

    if (q >= 6 && q <= 9) 0.05
    else if (q >= 10 && q <= 14) 0.07
    else if (q >= 15) 0.10
    else 0.0
  }

  // Specific day and month rule qualification and calc
  def isDateQualified(t: Transaction): Boolean = dateDiscount(t) > 0.0

  def dateDiscount(t: Transaction): Double = {
    val date = t.timestamp.split("T")(0)
    val datePart = date.split("-")
    val month = datePart(1)
    val day = datePart(2)

    if (month == "03" && day == "23") 0.5 else 0.0
  }

  // Expiry date rule qualification and calc
  // This is a general function I will use in expiryDiscount function
  def getDaysRemaining(t: Transaction): Long = {
    val txDateString = t.timestamp.split("T")(0)
    val txDate = LocalDate.parse(txDateString)
    val expDate = LocalDate.parse(t.expiryDate)
    // Get the diff between 2 dates
    ChronoUnit.DAYS.between(txDate, expDate)
  }

  def isExpiryQualified(t: Transaction): Boolean = expiryDiscount(t) > 0.0

  def expiryDiscount(t: Transaction): Double = {
    val daysRemaining = getDaysRemaining(t)
    if (daysRemaining > 0 && daysRemaining < 30) (30 - daysRemaining) * 0.01
    else
      0.0
  }

  def pMethodQualified(t: Transaction): Boolean = pMethodDiscount(t) > 0.0

  def pMethodDiscount(t: Transaction): Double = {
    if (t.paymentMethod == "Visa") 0.05
    else 0.0
  }

  def quanRoundedQualified(t: Transaction): Boolean = quanRoundedDiscount(t) > 0.0
  def quanRoundedDiscount(t: Transaction): Double = {
    ((t.quantity + 4) / 5 * 5.0) / 100
  }

  // Each rule is type Rule (Tuple of 2 functions)
  val rule1: Rule = (isCategoryQualified, categoryDiscount)
  val rule2: Rule = (isQuantityQualified, quantityDiscount)
  val rule3: Rule = (isDateQualified, dateDiscount)
  val rule4: Rule = (isExpiryQualified, expiryDiscount)
  val rule5: Rule = (pMethodQualified, pMethodDiscount)
  val rule6: Rule = (quanRoundedQualified, quanRoundedDiscount)

  // Put them into a list (list of Rules)
  val rules: List[Rule] = List(rule1, rule2, rule3, rule4, rule5, rule6)

  //---------------------------------------------------------------------------------------//

  // Calculate the discount of applied rules and final discount (get avg of top 2 discounts).
  def calculateFinalDiscount(t: Transaction, rules: List[Rule]): Double = {
    val applicableDiscounts = rules
      // In filter we focus on first function and do not look at the second, in map we map the filter on the second function so we only apply calculation rule on True qualifications
      .filter { case (qualifier, _) => qualifier(t) }
      .map { case (_, calculator) => calculator(t) }

    val topDiscounts = applicableDiscounts.sortWith(_ > _).take(2)

    if (topDiscounts.isEmpty) 0.0
    else topDiscounts.sum / topDiscounts.length
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

//---------------------------------------------------------------------------------------------//

// Database object connection
object DatabaseManager {

  val url = "jdbc:mysql://localhost:3306/iti-scala"
  val username = "root"
  // I have added the database pass as an environment variable in IntelliJ IDE
  val password = sys.env.getOrElse(
    "DB_PASS",
    throw new IllegalArgumentException("CRITICAL: DB_PASS environment variable is missing!")
  )

  // Connection
  def getConnection(): Try[Connection] = Try {
    DriverManager.getConnection(url, username, password)
  }

  def initializeTables(conn: Connection): Try[Unit] = Try {
    val statement = conn.createStatement()

    val createTransactionsTable =
      """
        CREATE TABLE IF NOT EXISTS transactions (
          id INT AUTO_INCREMENT PRIMARY KEY,
          timestamp VARCHAR(50),
          product_name VARCHAR(255),
          expiry_date VARCHAR(50),
          quantity INT,
          unit_price DOUBLE,
          channel VARCHAR(50),
          payment_method VARCHAR(50),
          discount DOUBLE,
          final_price DOUBLE
        )
      """

    statement.execute(createTransactionsTable)
    statement.close()
  }

  def saveTransactions(conn: Connection, transactions: List[Main.Transaction]): Try[Array[Int]] = Try {
    val sql =
      """
        INSERT INTO transactions
        (timestamp, product_name, expiry_date, quantity, unit_price, channel, payment_method, discount, final_price)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      """

    val pstmt: PreparedStatement = conn.prepareStatement(sql)

    // I used foreach as a pure function instead of loops
    transactions.foreach { t =>
      pstmt.setString(1, t.timestamp)
      pstmt.setString(2, t.productName)
      pstmt.setString(3, t.expiryDate)
      pstmt.setInt(4, t.quantity)
      pstmt.setDouble(5, t.unitPrice)
      pstmt.setString(6, t.channel)
      pstmt.setString(7, t.paymentMethod)
      pstmt.setDouble(8, t.discount)
      pstmt.setDouble(9, t.finalPrice)
      pstmt.addBatch()
    }

    val results = pstmt.executeBatch()
    pstmt.close()
    results
  }
}