package DiscountRulesEngine

import scala.util.Try
import java.sql.{Connection, DriverManager, PreparedStatement}
import Domain.Transaction

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

  def saveTransactions(conn: Connection, transactions: List[Transaction]): Try[Array[Int]] = Try {
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