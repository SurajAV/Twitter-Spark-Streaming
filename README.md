# Twitter-Spark-Streaming
This Document gives better understanding of performing analytics in Spark Streaming by using Twitter Real Time data

// Spark Streaming Twitter //

package com.suraj.practise.program

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf


object TwitterPopularTags {
  def main(args: Array[String]){
println("hello")

if(args.length < 4){
System.err.println{"Usage: TwitterPopularTags <consumer Key> <consumer secret>" +
"<access token> <access token secret> [<filters>]" }
System.exit(1)
}

StreamingExamples.setStreamingLogLevels()

val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4) 
val filters = args.takeRight(args.length - 4)

System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
System.setProperty("twitter4j.oauth.accessToken", accessToken)
System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

val sparkConf = new SparkConf().setAppName("TwitterPopularTags").setMaster("local[2]")
val ssc = new StreamingContext(sparkConf, Seconds(2))
val stream = TwitterUtils.createStream(ssc, None, filters)

val hashtags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

val topCounts60 = hashtags.map((_,3)).reduceByKeyAndWindow(_+_, Seconds(60))
                  .map{case (topic, count)=> (count, topic)}
                  .transform(_.sortByKey(false))  

val topCounts10 = hashtags.map((_,3)).reduceByKeyAndWindow(_+_, Seconds(10))
                  .map{case (topic, count)=> (count, topic)}
                  .transform(_.sortByKey(false)) 
 
// Printing popular Hash tags
topCounts60.foreachRDD(rdd => {
val topList = rdd.take(10)
println("\nPopular Topics in last 60 seconds (%s total):".format(rdd.count()))
topList.foreach{case(count,tag) => println("%s (%s tweets)".format(tag, count))}
})                  

topCounts10.foreachRDD(rdd => {
val topList = rdd.take(10)
println("\nPopular Topics in last 60 seconds (%s total):".format(rdd.count()))
topList.foreach{case(count,tag) => println("%s (%s tweets)".format(tag, count))}
})                  
ssc.start()
ssc.awaitTermination()

}}


// Streaming Examples code //

package com.suraj.practise.program

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


//Streaming Examples Code//

import org.apache.spark.Logging

import org.apache.log4j.{Level, Logger}

/** Utility functions for Spark Streaming examples. */
object StreamingExamples extends Logging {

  /** Set reasonable logging levels for streaming if the user has not configured log4j. */
  def setStreamingLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the
      // logging level.
      logInfo("Setting log level to [WARN] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}
