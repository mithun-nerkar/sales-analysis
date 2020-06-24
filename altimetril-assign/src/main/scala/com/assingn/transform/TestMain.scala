package com.assingn.transform

import org.apache.spark.sql.SparkSession
import com.assingn.readsources.csvsources
import com.assingn.readsources.stageddata
import org.apache.spark.sql.SaveMode

object TestMain {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C://Users//DELL//winutils")

    // creating spark Session for local development
    val sparkSession = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

    // creating reader and writer class intermediate objects
    val csvLoad = new csvsources
    val stageLoad = new stageddata
    val kpi = new calculatekpi

    // reading employee data and writing to lake

    val empDf = csvLoad.loadEmployeeMaster(sparkSession, configfactory.getConfigs().getString("readpath.empdatapath")).get
    stageLoad.stageEmpData(sparkSession, empDf, configfactory.getConfigs().getString("writepath.empwritepath"))

    // reading products data and writing data to lake

    val productDf = csvLoad.loadProductMaster(sparkSession, configfactory.getConfigs().getString("readpath.proddatapath")).get
    stageLoad.stageProdData(sparkSession, productDf, configfactory.getConfigs().getString("writepath.prodwritepath"))

    // reading sales data and writing to lake
    val salesDf = csvLoad.loadSalesMaster(sparkSession, configfactory.getConfigs().getString("readpath.salesdatapath")).get
    stageLoad.stageSalesData(sparkSession, salesDf, configfactory.getConfigs().getString("writepath.saleswritepath"))

    // reading twitter and staging data
    val tweetDf = csvLoad.loadTwitterData(sparkSession, configfactory.getConfigs().getString("readpath.twitterdatapath")).get
    stageLoad.stageTweetData(sparkSession, tweetDf, configfactory.getConfigs().getString("writepath.twitterwritepath"))
    
    // calculating all KPI values
    
    val reviewCommentPerProduct = kpi.perProductReviewComments(sparkSession)
    reviewCommentPerProduct.repartition(1).write.mode(SaveMode.Overwrite).csv(configfactory.getConfigs().getString("resultpath.reviewpercomment"))
    
    // calculate sales per employee
    
    val salesByEmployeeDf = kpi.employeeWiseSaleUnits(sparkSession)
    salesByEmployeeDf.repartition(1).write.mode(SaveMode.Overwrite).csv(configfactory.getConfigs().getString("resultpath.salesperemployee"))
    
    val salesPerProductDf = kpi.productWiseSales(sparkSession)
    salesPerProductDf.repartition(1).write.mode(SaveMode.Overwrite).csv(configfactory.getConfigs().getString("resultpath.salesperproduct"))
    
    
    

    
    
    
  }

}