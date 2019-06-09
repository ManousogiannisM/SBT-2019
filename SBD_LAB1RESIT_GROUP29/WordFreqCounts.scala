// sbt assembly help: https://sparkour.urizone.net/recipes/building-sbt/
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io._
import java.util.Locale
import org.apache.commons.lang3.StringUtils
import org.apache.spark.mllib.rdd.RDDFunctions._

object WordFreqCounts 
{
	def main(args: Array[String]) 
	{
		 // Get input file's name from this command line argument
		val inputFile = args(0)
		val conf = new SparkConf().setMaster("local").setAppName("WordFreqCounts")
		val sc = new SparkContext(conf)
		
		println("Input file: " + inputFile)
		
		// Uncomment these two lines if you want to see a less verbose messages from Spark
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		val t0 = System.currentTimeMillis
		val inputFilerdd = sc.textFile(inputFile)
		val myRDD=inputFilerdd.flatMap(_.split(" ")).map(_.toLowerCase)   
											// RDD: (word1,word2,word3,...,wordn) -> we tokenize the input text and transform everything to LOWERCASE for the case-insensitivity requirement.
											//IMPORTANT:  After we split into words all \newlines are automatically replaced by whitespaces, so we do not need to handle newlines specifically.	
												.sliding(2)   
												// RDD ((word1,word2),(word2,word3),(word3,word4),...) --> create tuples of the co-occuring words 
												
												.map{ case Array(x, y) => ((x, y), 1) }  
												// RDD (((word1,word2),1),((word2,word3),1),...) --> initialize every co-occuracy of two words in the text to 1.
												
												.reduceByKey(_+_)   
												// RDD (((word1,word2),freq12),...)--> group all the UNIQUE co-occuracy tuples and count their frequency
												
												.map{case ((a,b),c) =>(b,(a,c))} 
												// RDD ((word2,(word1,freq12)) --> switch the second word of the tuple to the first position (key). In that way, word2 (key) is our basic word and word1 and freq12 (values) demonstrate how many times word2 is a preceded word of word1. 
												
												.groupByKey() 
												// RDD ((word2,BUFFER[(word1,freq12),(wordN,freq1N)...]))--> Now we have every word of the document as a key, and its corresponding value is a buffer of all its proceded words together with their frequencies.
												
												.mapValues(iter=>iter.toSeq.sortBy{case (a,b)=>(-b)->a}) 
												//RDD ((word2,SEQ[(word1,freq12),(wordN,freq1N)...]))--> Sort by frequency value and as a second option with alphabetic order of  preceded words
												
												.filter(x=>(x._1.takeRight(1).matches("[a-z]") && x._1.matches("[a-z]*"))) 
												// Remove all keys that do not START with a letter a-z OR they do not END with a letter a-z. As per requirement they are not considered words.
												
												.map{case (a,b)=>((a,b.map(_._2).sum),b)}
												// RDD ((word2,freq2), SEQ[(word1,freq12),(wordN,freq1N)..] , ...) --> We calculate the 'basic' word frequencies by adding up all the preceded word frequency values. 
												
												.mapValues(b=>b.filter(element=>(element._1.takeRight(1).matches("[a-z]") && element._1.matches("[a-z]*")))) 
												// filter to remove NON words just like above
												
												.sortBy{case((a,b),c)=>(-b)->a} 
												// sort by word frequency values and after that alphabetically.
												
												.map{case((a,b),c)=>a+": "+b + "\n" +"\t"+c.map{case(c,d)=>c +":"+d +"\n" +"\t"}.mkString} 
												// convert RDD to the required format anf store it in a textFile.
												
												.saveAsTextFile("C:\\Users\\pc1\\Desktop\\freq.txt")
																																											
		val et = (System.currentTimeMillis - t0) / 1000
		System.err.println("Done!\nTime taken = %d mins %d secs".format(et / 60, et % 60))
	}
}
