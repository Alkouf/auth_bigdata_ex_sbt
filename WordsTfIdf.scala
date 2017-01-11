

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession,Row}
import org.apache.spark.ml.feature.IDF
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.feature.NGram
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.classification.LogisticRegression

object WordsTfIdf {


  def tfidf(spark: SparkSession,inputFile:String,splitRate:Array[Double]): Array[Dataset[Row]] ={
    val trainData = spark.sparkContext.textFile(inputFile, 2).map(line => (line.split(",")(0), line.split(",")(1).toDouble))
    val dfdata = spark.createDataFrame(trainData).toDF("comments", "label")
    val tokenizer = new Tokenizer().setInputCol("comments").setOutputCol("words")
    val wordsData = tokenizer.transform(dfdata)
    //----------------------Simple Tf IDF----------------------------------------------
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures")
    val feautirizedData = hashingTF.transform(wordsData)
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(feautirizedData)
    val rescaledData = idfModel.transform(feautirizedData)
    rescaledData.randomSplit(splitRate)
  }

  def ngramtf(spark: SparkSession,inputFile:String,splitRate:Array[Double],nValue:Int=2): Array[Dataset[Row]] ={
    val trainData = spark.sparkContext.textFile(inputFile, 2).map(line => (line.split(",")(0), line.split(",")(1).toDouble))
    val dfdata = spark.createDataFrame(trainData).toDF("comments", "label")
    val tokenizer = new Tokenizer().setInputCol("comments").setOutputCol("words")
    val wordsData = tokenizer.transform(dfdata)
    //----------------------N gram TF -------------------------------------------------------
    val ngram = new NGram().setInputCol("words").setOutputCol("ngrams").setN(nValue)
    val ngramWordsData = ngram.transform(wordsData)
    val hashingTF2 = new HashingTF().setInputCol("ngrams").setOutputCol("features")
    val ngramTF = hashingTF2.transform(ngramWordsData)
    ngramTF.randomSplit(splitRate)
  }

  def words2Vec(spark: SparkSession,inputFile:String,splitRate:Array[Double],vectorsize:Int=100,minCount:Int=0): Array[Dataset[Row]]={
    val trainData = spark.sparkContext.textFile(inputFile, 2).map(line => (line.split(",")(0), line.split(",")(1).toDouble))
    val dfdata = spark.createDataFrame(trainData).toDF("comments", "label")
    val tokenizer = new Tokenizer().setInputCol("comments").setOutputCol("words")
    //----------------------Word2Vec -------------------------------------------------------
    val wordsData = tokenizer.transform(dfdata)
    val word2Vec = new Word2Vec().setInputCol("words").setOutputCol("features").setVectorSize(vectorsize).setMinCount(minCount)
    val model = word2Vec.fit(wordsData)
    val result = model.transform(wordsData)
    result.randomSplit(splitRate)
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
        // Data in TFIDF
        //val Array(trainData,testData) = tfidf(spark,inputFile,Array(0.6,0.4))
        // Data Ngram TF
        //val Array(trainData,testData) = ngramtf(spark,inputFile,Array(0.7,0.3))
        // Word2vector
        val Array(trainData,testData) = words2Vec(spark,inputFile,Array(0.7,0.3))
        /*
        //-----------------------First Attempt ML- TFIDF/NGRAM------------------------------------------------------------
        // Train a NaiveBayes model.
        val model = new NaiveBayes().fit(trainData)

        // Select example rows to display.
        val predictions = model.transform(testData)
        predictions.show()

        // Select (prediction, true label) and compute test error
        val evaluator = new MulticlassClassificationEvaluator()
          .setLabelCol("label")
          .setPredictionCol("prediction")
          .setMetricName("accuracy")
        val accuracy = evaluator.evaluate(predictions)
        println("Accuracy: " + accuracy)
        //-----------------------First Attempt ML------------------------------------------------------------
        */
        //-----------------------Second Attempt ML - Word2Vec------------------------------------------------------------
        // Train a NaiveBayes model.
        val layers = Array[Int](4, 5, 4, 3)

        val model = new  LogisticRegression()
          .setMaxIter(10)
          .setRegParam(0.3)
          .setElasticNetParam(0.8).fit(trainData)

        // Select example rows to display.
        val predictions = model.transform(testData)
        predictions.show()

        // Select (prediction, true label) and compute test error
        val evaluator = new MulticlassClassificationEvaluator()
          .setLabelCol("label")
          .setPredictionCol("prediction")
          .setMetricName("accuracy")
        val accuracy = evaluator.evaluate(predictions)
        println("Accuracy: " + accuracy)
        //-----------------------Second Attempt ML------------------------------------------------------------

      } else {
        println("scala WordsTfIdf.scala csv_file_name")
      }

    } else {
      println("Please provide file path.")

    }

  }

}
