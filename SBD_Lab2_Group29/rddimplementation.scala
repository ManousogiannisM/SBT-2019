package rddimplementation

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io._
import java.util.Locale


	object rddimplementation{
		def main(args: Array[String]){
				val conf = new SparkConf().setAppName("PagecountsRDD")
				val sc = new SparkContext(conf)
				
				
				//For 30,000 segments: val segmentsPath="s3://gdelt-open-data/v2/gkg/2015*.gkg.csv"
				val segmentsPath="s3://gdelt-open-data/v2/gkg/*.gkg.csv"

				val raw_data=sc.textFile(segmentsPath)
				
				val datedata=raw_data.map(_.split("\t",-1)).filter(_.length > 23).map(a => (a(1).substring(0,8),a(23)))
 
				val ourData=datedata.flatMapValues(topics => (topics.split(';').map(_.split(',')(0))))
									.map{case (date, topic) => ((date, topic), 1)}
									.reduceByKey(_+_).map{case ((date, topic), count) => (date, (topic, count))}
									.groupByKey()
				
				val sortedValues=ourData.mapValues(a => a.toSeq.sortWith(_._2 > _._2).filter(element => element._1 != "Type ParentCategory").filter(element => element._1 != "").take(10))

				sortedValues.coalesce(1,shuffle = true).saveAsTextFile("s3://lab2group29/results")
				
				
			
		}
			
			
			
	}

