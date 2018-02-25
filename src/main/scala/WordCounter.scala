import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Koen Rodenburg on 2-2-2018.
  */
object WordCounter {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("Word Counter").setMaster("local")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("file:///spark/README.md")
    val tokenizedFileData = textFile.flatMap(line=>line.split(" "))
    val countPrep = tokenizedFileData.map(word=>(word,1))
    val counts = countPrep.reduceByKey((accumValue, newValue)=>accumValue + newValue)
    val sortedCounts = counts.sortBy(kvPair=>kvPair._2, ascending = false)
    sortedCounts.saveAsTextFile("file:///SparkData/ReadMeWordCountViaApp")
  }
}
