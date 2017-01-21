

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.classification.LogisticRegression

object WordsTfIdf {


  def arrayToString(array: Array[String]) : String = {
    var str = array(array.length-1) + " ";
    for(i <- 0 to (array.length/2)-1)
    {
      str = str.concat(array(i)).concat(":" + array((array.length/2)+i) + " ")
    }
    str
  }


  def datasetToRDD(data:Dataset[Row], numOfFeatures: Int): Unit = {

    data.select("features", "label").rdd.map(line => line.mkString.replaceAll("\\(" + numOfFeatures + ",", ""))
      .map(line => {line.replaceAll("\\[", "").replaceAll("\\]","").replaceAll("\\)",",")})
      .map(line => {arrayToString(line.split(","))})
      //.foreach(x => println(x))
      .coalesce(1, true)
      .saveAsTextFile("data/rddData")
  }





  // tokenize words. I added it as a method, so that the code is not written over and over
  def tokenizeWords(spark: SparkSession,inputFile:String): Dataset[Row] =
  {
    val trainData = spark.sparkContext.textFile(inputFile, 2).map(line => (line.split(",")(0), line.split(",")(1).toDouble))
    val dfdata = spark.createDataFrame(trainData).toDF("comments", "label")
    dfdata.show(5)
    val tokenizer = new Tokenizer().setInputCol("comments").setOutputCol("words")
    val wordsData = tokenizer.transform(dfdata)
    wordsData.show(5)
    wordsData
  }


  def tfidf(spark: SparkSession,inputFile:String,splitRate:Array[Double],numFeatures:Int=100000, pca:Int = 0): Array[Dataset[Row]] ={
    val wordsData = tokenizeWords(spark: SparkSession,inputFile:String)
    //----------------------Simple Tf IDF----------------------------------------------
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(numFeatures)
    val feautirizedData = hashingTF.transform(wordsData)
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(feautirizedData)
    val rescaledData = idfModel.transform(feautirizedData)

    if(pca != 0)
      pcaTransform(rescaledData, splitRate, pca)
    else
      rescaledData.randomSplit(splitRate)
  }



  def ngramtf(spark: SparkSession,inputFile:String,splitRate:Array[Double],numOfFeatures:Int=3000, nValue:Int=2, pca:Int=0): Array[Dataset[Row]] ={
    val wordsData = tokenizeWords(spark: SparkSession,inputFile:String)
    //----------------------N gram TF -------------------------------------------------------
    val ngram = new NGram().setInputCol("words").setOutputCol("ngrams").setN(nValue)
    val ngramWordsData = ngram.transform(wordsData)
    val hashingTF2 = new HashingTF().setInputCol("ngrams").setOutputCol("features").setNumFeatures(numOfFeatures)
    val ngramTF = hashingTF2.transform(ngramWordsData)

    if(pca != 0)
      pcaTransform(ngramTF, splitRate, pca)
    else
      ngramTF.randomSplit(splitRate)
  }

  def words2Vec(spark: SparkSession,inputFile:String,splitRate:Array[Double],vectorsize:Int=100,minCount:Int=0, pca:Int=0): Array[Dataset[Row]]={
    val wordsData = tokenizeWords(spark: SparkSession,inputFile:String)
    //----------------------Word2Vec -------------------------------------------------------
    val word2Vec = new Word2Vec().setInputCol("words").setOutputCol("features").setVectorSize(vectorsize).setMinCount(minCount)
    val model = word2Vec.fit(wordsData)
    val result = model.transform(wordsData)

    if(pca != 0)
      pcaTransform(result, splitRate, pca)
    else
      result.randomSplit(splitRate)
  }


  def pcaTransform(data:Dataset[Row], splitRate:Array[Double], factors:Int): Array[Dataset[Row]] ={
    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(factors)
      .fit(data)

    val pcaDF = pca.transform(data)
    val result = pcaDF.select("pcaFeatures")
    result.show(10)
    result.foreach(x => println(x))
    println(pca.getK)
    pcaDF.randomSplit(splitRate)
  }


  def main(args: Array[String]): Unit = {
    var csvfilename: String = ""
    if (args.length != 0) {

      csvfilename = args(0)
      if (!(csvfilename == "--help")) {
        val conf = new SparkConf().setAppName("ML Auth App").setMaster("local[1]")
        val spark = SparkSession.builder().config(conf).getOrCreate()
        // No INFO messages
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)
        val currentDir = System.getProperty("user.dir")
        println(currentDir)
        val inputFile = "file://" + currentDir + "/"+csvfilename
        println(inputFile)
        // Data in TFIDF
        val Array(trainData,testData) = tfidf(spark,inputFile,Array(0.6,0.4))
        // Data Ngram TF
        //val Array(trainData,testData) = ngramtf(spark,inputFile,Array(0.7,0.3))
        // Word2vector
        //val Array(trainData,testData) = words2Vec(spark,inputFile,Array(0.7,0.3))
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

        println("EVALUATING 1.0")
        //----------------- Evaluate (precision, recall, accuracy) ------------------------
        val evaluator = new MulticlassClassificationEvaluator()
          .setLabelCol("label")
          .setPredictionCol("prediction")

        println("Accuracy: " + evaluator.setMetricName("accuracy").evaluate(predictions) + "\n" +
          "Weighted Precision: " + evaluator.setMetricName("weightedPrecision").evaluate(predictions) + "\n" +
          "Weighted Recall: " + evaluator.setMetricName("weightedRecall").evaluate(predictions) + "\n" +
          "F1: " + evaluator.setMetricName("f1").evaluate(predictions))


        println("EVALUATING 2.0")
        //----------------- Evaluate (areaUnderROC, areaUnderPR) ------------------------

        val binEvaluator = new BinaryClassificationEvaluator("prediction")
          .setLabelCol("label")



        println("Area under ROC: " + binEvaluator.setMetricName("areaUnderROC").evaluate(predictions) + "\n" +
          "Area Under PR: " + binEvaluator.setMetricName("areaUnderPR").evaluate(predictions))


        //-----------------------Second Attempt ML------------------------------------------------------------

      } else {
        println("scala WordsTfIdf.scala csv_file_name")
      }

    } else {
      println("Please provide file path.")

    }

  }

}
