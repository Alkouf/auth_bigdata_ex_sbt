

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.ml.feature.IDF
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.feature.NGram
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator


object WordsTfIdf {


  def tfidf(spark: SparkSession,inputFile:String): Unit ={
    val trainData = spark.sparkContext.textFile(inputFile, 2).map(line => (line.split(",")(0), line.split(",")(1).toDouble))
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

  def ngramtf(spark: SparkSession,inputFile:String): Unit ={
    val trainData = spark.sparkContext.textFile(inputFile, 2).map(line => (line.split(",")(0), line.split(",")(1).toDouble))
    val dfdata = spark.createDataFrame(trainData).toDF("comments", "label")
    val tokenizer = new Tokenizer().setInputCol("comments").setOutputCol("words")
    val wordsData = tokenizer.transform(dfdata)
    //----------------------N gram TF -------------------------------------------------------
    val ngram = new NGram().setInputCol("words").setOutputCol("ngrams")
    val ngramWordsData = ngram.transform(wordsData)
    val hashingTF2 = new HashingTF().setInputCol("ngrams").setOutputCol("rawFeatures")
    val ngramTF = hashingTF2.transform(ngramWordsData)
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
        val trainData = spark.sparkContext.textFile(inputFile, 2).map(line => (line.split(",")(0), line.split(",")(1).toDouble))
        val dfdata = spark.createDataFrame(trainData).toDF("comments", "label")
        val tokenizer = new Tokenizer().setInputCol("comments").setOutputCol("words")
        val wordsData = tokenizer.transform(dfdata)
        //----------------------Simple Tf IDF----------------------------------------------
        val hashingTF1 = new HashingTF().setInputCol("words").setOutputCol("rawFeatures")
        val feautirizedData = hashingTF1.transform(wordsData)
        val idf1 = new IDF().setInputCol("rawFeatures").setOutputCol("features")
        val idfModel1 = idf1.fit(feautirizedData)
        val rescaledData1 = idfModel1.transform(feautirizedData)
        val Array(trainDataIDFTF,testDataIDFTF) =rescaledData1.randomSplit(Array(0.6,0.4))
        //println(puretfidf1)
        //rescaledData1.printSchema()
        //println(rescaledData.head())
        //----------------------N gram TF -------------------------------------------------------
        val ngram = new NGram().setInputCol("words").setOutputCol("ngrams")
        val ngramWordsData = ngram.transform(wordsData)
        val hashingTF2 = new HashingTF().setInputCol("ngrams").setOutputCol("rawFeatures")
        val ngramTF = hashingTF2.transform(ngramWordsData)



        //-----------------------First Attempt ML------------------------------------------------------------

        // Feature selection
        //TODO
        // Train a NaiveBayes model.
        //val model1 = new NaiveBayes()
        //model1.setFeaturesCol("features")
        val model = new NaiveBayes().fit(trainDataIDFTF)

        // Select example rows to display.
        val predictions = model.transform(testDataIDFTF)
        predictions.show()

        // Select (prediction, true label) and compute test error
        val evaluator = new MulticlassClassificationEvaluator()
          .setLabelCol("label")
          .setPredictionCol("prediction")
          .setMetricName("accuracy")
        val accuracy = evaluator.evaluate(predictions)
        println("Accuracy: " + accuracy)



        //-----------------------First Attempt ML------------------------------------------------------------


      } else {
        println("scala WordsTfIdf.scala csv_file_name")
      }

    } else {
      println("Please provide file path.")

    }

  }

}
