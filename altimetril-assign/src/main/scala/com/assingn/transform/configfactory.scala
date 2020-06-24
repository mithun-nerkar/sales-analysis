package com.assingn.transform

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

/**
 * @author singleton class used for loading config facory to load config from property files
 *
 */
object configfactory {

  def getConfigs(): Config = {

    val configObj = ConfigFactory.load("application.conf")
    return configObj

  }

}