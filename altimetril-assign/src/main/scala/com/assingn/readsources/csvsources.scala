package com.assingn.readsources

import org.apache.spark.sql.SparkSession
import scala.util.Try
import org.apache.spark.sql.DataFrame
import com.assingn.alt.model.employeeMaster
import org.apache.log4j.Logger
import org.apache.spark.sql.Encoders
import scala.util.Success
import scala.util.Failure
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Dataset
import com.assingn.alt.model.productMaster
import org.apache.spark.sql.Dataset
import com.assingn.alt.model.salesMaster
import org.apache.spark.sql.Dataset
import com.assingn.alt.model.twitterFeed

/**
 * @author This is first step in assingnment where data is sourced from csv files can be databases,sdfp,amazon s3 etc
 *
 */
class csvsources {

  val sourceLogger = Logger.getLogger(classOf[csvsources])

  def loadEmployeeMaster(sparkSessionbj: SparkSession, sourcePath: String): Try[Dataset[employeeMaster]] = {

    try {

      val employeeEncoder = Encoders.product[employeeMaster]
      sourceLogger.info("loading the employee master files")
      val employeeMasterDf = sparkSessionbj.read.option("header", "true").csv(sourcePath).as(employeeEncoder)

      Success(employeeMasterDf)

    } catch {
      case exception: Exception =>
        exception.getMessage()
        sourceLogger.error("error reading employee master data" + exception.getMessage)
        Failure(exception)
    }

  }

  def loadProductMaster(sparkSessionObj: SparkSession, sourePath: String): Try[Dataset[productMaster]] = {

    try {

      val productEncoder = Encoders.product[productMaster]
      sourceLogger.info("loading the product master table")
      val productMasterDf = sparkSessionObj.read.option("header", "true").csv(sourePath).as(productEncoder)
      Success(productMasterDf)

    } catch {
      case exception: Exception =>
        sourceLogger.error("error reading product master data" + exception.getMessage)
        Failure(exception)
    }

  }

  def loadSalesMaster(sparkSessionObj: SparkSession, sourcePath: String): Try[Dataset[salesMaster]] = {

    try {

      val salesDataEncoder = Encoders.product[salesMaster]
      sourceLogger.info("reading the sales data")
      val salesDataDf = sparkSessionObj.read.option("header", "true").csv(sourcePath).as(salesDataEncoder)
      Success(salesDataDf)

    } catch {
      case exception: Exception =>
        sourceLogger.error("error reading sales data" + exception.getMessage)
        Failure(exception)
    }

  }

  def loadTwitterData(sparkSessionObj: SparkSession, sourcePath: String): Try[Dataset[twitterFeed]] = {
    try {

      val twitterDataEncoder = Encoders.product[twitterFeed]
      sourceLogger.info("reading twitter data from file")
      val twitterFeedDf = sparkSessionObj.read.option("header", "true").csv(sourcePath).as(twitterDataEncoder)
      Success(twitterFeedDf)

    } catch {
      case exception: Exception =>
        sourceLogger.error("error reading twitter data" + exception.getMessage)
        Failure(exception)
    }
  }

} 