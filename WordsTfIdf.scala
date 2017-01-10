

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.ml.feature.IDF
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.feature.NGram


object WordsTfIdf {


  def tfidf(spark: SparkSession,inputFile:String): Unit ={
    val trainData = spark.sparkContext.textFile(inputFile, 2).map(line => (line.split(",")(0), line.split(",")(1).toInt))
    val dfdata = spark.createDataFrame(trainData).toDF("comments", "label")
    val tokenizer = new Tokenizer().setInputCol("comments").setOutputCol("words")
    val wordsData = tokenizer.transform(dfdata)
    //----------------------Simple Tf IDF----------------------------------------------
    val hashingTF1 = new HashingTF().setInputCol("words").setOutputCol("rawFeatures")
    val feautirizedData = hashingTF1.transform(wordsData)
    val idf1 = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel1 = idf1.fit(feautirizedData)
    val rescaledData1 = idfModel1.transform(feautirizedData)
    val puretfidf1 =rescaledData1.select("features","label").rdd
  }

  def ngramtfidf(spark: SparkSession,inputFile:String): Unit ={
    val trainData = spark.sparkContext.textFile(inputFile, 2).map(line => (line.split(",")(0), line.split(",")(1).toInt))
    val dfdata = spark.createDataFrame(trainData).toDF("comments", "label")
    val tokenizer = new Tokenizer().setInputCol("comments").setOutputCol("words")
    val wordsData = tokenizer.transform(dfdata)
    //----------------------N gram TF IDF-------------------------------------------------------
    val ngram = new NGram().setInputCol("words").setOutputCol("ngrams")
    val ngramWordsData = ngram.transform(wordsData)
    val hashingTF2 = new HashingTF().setInputCol("ngrams").setOutputCol("rawFeatures")
    val ngramfeautirizedData = hashingTF2.transform(ngramWordsData)
    val idf2 = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel2 = idf2.fit(ngramfeautirizedData)
    val rescaledData2 = idfModel2.transform(ngramfeautirizedData)
    val puretfidf2 =rescaledData2.select("features","label").rdd
  }

  def main(args: Array[String]): Unit = {
    var csvfilename: String = ""
    if (args.length != 0) {

      csvfilename = args(0)
      if (!(csvfilename == "--help")) {
        val conf = new SparkConf().setAppName("ML Auth App").setMaster("local[1]")
        val spark = SparkSession.builder().config(conf).getOrCreate()
        val currentDir = System.getProperty("user.dir")
        println(currentDir)
        val inputFile = "file://" + currentDir + "/"+csvfilename
        println(inputFile)
        val trainData = spark.sparkContext.textFile(inputFile, 2).map(line => (line.split(",")(0), line.split(",")(1).toInt))
        val dfdata = spark.createDataFrame(trainData).toDF("comments", "label")
        val tokenizer = new Tokenizer().setInputCol("comments").setOutputCol("words")
        val wordsData = tokenizer.transform(dfdata)
        //----------------------Simple Tf IDF----------------------------------------------
        val hashingTF1 = new HashingTF().setInputCol("words").setOutputCol("rawFeatures")
        val feautirizedData = hashingTF1.transform(wordsData)
        val idf1 = new IDF().setInputCol("rawFeatures").setOutputCol("features")
        val idfModel1 = idf1.fit(feautirizedData)
        val rescaledData1 = idfModel1.transform(feautirizedData)
        val puretfidf1 =rescaledData1.select("features","label").rdd
        //rescaledData1.printSchema()
        //println(rescaledData.head())
        //----------------------N gram TF IDF-------------------------------------------------------
        val ngram = new NGram().setInputCol("words").setOutputCol("ngrams")
        val ngramWordsData = ngram.transform(wordsData)
        val hashingTF2 = new HashingTF().setInputCol("ngrams").setOutputCol("rawFeatures")
        val ngramfeautirizedData = hashingTF2.transform(ngramWordsData)
        val idf2 = new IDF().setInputCol("rawFeatures").setOutputCol("features")
        val idfModel2 = idf2.fit(ngramfeautirizedData)
        val rescaledData2 = idfModel2.transform(ngramfeautirizedData)
        val puretfidf2 =rescaledData2.select("features","label").rdd
        //puretfidf2.foreach(line => println(line))



      } else {
        println("scala WordsTfIdf.scala csv_file_name")
      }

    } else {
      println("Please provide file path.")

    }

  }

}
