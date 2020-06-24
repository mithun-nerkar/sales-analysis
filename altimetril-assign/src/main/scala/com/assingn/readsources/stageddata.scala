package com.assingn.readsources

import org.apache.spark.sql.SparkSession
import com.assingn.transform.configfactory
import org.apache.spark.sql.Dataset
import com.assingn.alt.model.employeeMaster
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Dataset
import com.assingn.alt.model.productMaster
import org.apache.spark.sql.Dataset
import com.assingn.alt.model.salesMaster
import org.apache.spark.sql.Dataset
import com.assingn.alt.model.twitterFeed

/**
 * @author This is second step in the assingment where data is written to lake in any format , we prefer parquet format to write data to lake
 *
 */
class stageddata {

  def stageEmpData(sparkSessioObj: SparkSession, empDataSet: Dataset[employeeMaster], writePath: String): Unit = {
    empDataSet.write.mode(SaveMode.Overwrite).parquet(writePath)

  }

  def stageProdData(sparkSessioObj: SparkSession, productDataset: Dataset[productMaster], writePath: String): Unit = {
    productDataset.write.mode(SaveMode.Overwrite).parquet(writePath)
  }

  def stageSalesData(sparkSessioObj: SparkSession, salesDataset: Dataset[salesMaster], writePath: String): Unit = {
    salesDataset.write.mode(SaveMode.Overwrite).parquet(writePath)
  }

  def stageTweetData(sparkSessioObj: SparkSession, tweetDataset: Dataset[twitterFeed], writePath: String): Unit = {
    tweetDataset.write.mode(SaveMode.Overwrite).parquet(writePath)
  }

}