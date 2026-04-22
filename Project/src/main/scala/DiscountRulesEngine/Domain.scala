package DiscountRulesEngine

object Domain {
  case class Transaction(timestamp: String, productName: String, expiryDate: String, quantity: Int, unitPrice: Double, channel: String, paymentMethod: String, discount: Double = 0.0, finalPrice: Double = 0.0)

  // Rule is a tuple of 2 functions, first one returns Bool (rule fit or not) and second returns Double (discount)
  type Rule = (Transaction => Boolean, Transaction => Double)
}