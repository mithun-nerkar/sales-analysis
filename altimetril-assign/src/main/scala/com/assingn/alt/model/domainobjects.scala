package com.assingn.alt.model

/**
 * @author Used for creating encoders for data serialization and deserilazation of datasets
 *
 */
class domainobjects {
  // created case class for all dataset objects for compile time safety and to save schema inference time
}
case class employeeMaster(
  employeeId:  String,
  firstName:   String,
  lastName:    String,
  email:       String,
  empLocation: String)

case class productMaster(
  productId:   String,
  productName: String,
  productDesc: String)

case class salesMaster(
  orderId:      String,
  employeeId:   String,
  productId:    String,
  saleDate:     String,
  customerName: String,
  soldUnits:    String)

case class twitterFeed(
  productName:    String,
  custName:       String,
  reviewComments: String)