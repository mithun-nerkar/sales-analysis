package com.assingn.transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.functions.desc

class calculatekpi {

  /**
   * @param sparkSessionObj
   * @return this kpi calcualtes the number of review comments made per product by the customer
   */
  def perProductReviewComments(sparkSessionObj: SparkSession): DataFrame = {

    val loadTwitterData = sparkSessionObj.read.parquet(configfactory.getConfigs().getString("writepath.twitterwritepath"))
    val reviewCommentPerProductDf = loadTwitterData.select(loadTwitterData("*")).
      groupBy("productName").agg(count("reviewComments")
        .alias("Review Comment Count")).orderBy(desc("Review Comment Count"))
    reviewCommentPerProductDf

  }

  /**
   * @param sparkSessionObj
   * @return this kpi returns the total units sold by each employee for sales dashboard
   */
  def employeeWiseSaleUnits(sparkSessionObj: SparkSession): DataFrame = {

    val salesDataLake = sparkSessionObj.read.parquet(configfactory.getConfigs().getString("writepath.saleswritepath"))
    val employeeDataLake = sparkSessionObj.read.parquet(configfactory.getConfigs().getString("writepath.empwritepath"))

    val employeeDataFilterCol = employeeDataLake.select(
      employeeDataLake.col("employeeId").alias("EmpId"),
      employeeDataLake.col("firstName"))

    val joinedEmpSalesDf = employeeDataFilterCol.join(
      salesDataLake,
      employeeDataFilterCol.col("EmpId") === salesDataLake.col("employeeId"), "inner").
      groupBy("employeeId", "firstName").agg(sum("soldUnits").cast(DataTypes.createDecimalType(32, 0)).alias("Units Sold by Employee")).
      orderBy(desc("Units Sold by Employee"))

    joinedEmpSalesDf

  }

  /**
   * @param sparkSessioObj
   * @return product wise units sold and top products sold
   */
  def productWiseSales(sparkSessioObj: SparkSession): DataFrame = {

    val productLakeData = sparkSessioObj.read.parquet(configfactory.getConfigs().getString("writepath.prodwritepath"))
    val salesDataLake = sparkSessioObj.read.parquet(configfactory.getConfigs().getString("writepath.saleswritepath"))

    val productSalesDf = productLakeData.select(
      productLakeData.col("productId").alias("prodId"),
      productLakeData.col("productName"))

    val productSalesJoin = productSalesDf.join(salesDataLake, productSalesDf.col("prodId") === salesDataLake.col("productId"), "inner")
      .groupBy("productId", "productName").agg(sum("soldUnits").cast(DataTypes.createDecimalType(32, 0)).alias("Sold_Units"))
      .orderBy(desc("Sold_Units"))
      

    productSalesJoin

  }

}