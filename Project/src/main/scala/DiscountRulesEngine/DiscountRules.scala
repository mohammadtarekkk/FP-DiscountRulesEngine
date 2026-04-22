package DiscountRulesEngine

import java.time.LocalDate
import java.time.temporal.ChronoUnit
import Domain.{Transaction, Rule}

object DiscountRules {

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
}