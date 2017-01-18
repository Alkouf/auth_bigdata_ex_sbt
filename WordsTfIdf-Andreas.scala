

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.ml.feature.IDF
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.feature.NGram
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.Pipeline
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.feature.PCA




object WordsTfIdf {



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


  def tfidf(spark: SparkSession,inputFile:String,splitRate:Array[Double], numOfFeatures:Int = 20000, pca:Int): Array[Dataset[Row]] ={
    val wordsData = tokenizeWords(spark: SparkSession,inputFile:String)
    //----------------------Simple Tf IDF----------------------------------------------
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(numOfFeatures)
    val feautirizedData = hashingTF.transform(wordsData)
    feautirizedData.show(5)
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(feautirizedData)
    val rescaledData = idfModel.transform(feautirizedData)

    if(pca != 0)
      pcaTransform(rescaledData, splitRate, pca)
    else
      rescaledData.randomSplit(splitRate)
  }


  def ngramtf(spark: SparkSession,inputFile:String,splitRate:Array[Double], pca:Int, numOfFeatures:Int = 20000, nValue:Int=2): Array[Dataset[Row]] ={
    val wordsData = tokenizeWords(spark: SparkSession,inputFile:String)
    //----------------------N gram TF -------------------------------------------------------
    val ngram = new NGram().setInputCol("words").setOutputCol("ngrams").setN(nValue)
    val ngramWordsData = ngram.transform(wordsData)
    val hashingTF2 = new HashingTF().setInputCol("ngrams").setOutputCol("features").setNumFeatures(numOfFeatures)
    val ngramTF = hashingTF2.transform(ngramWordsData)
    ngramTF.select("features", "label").take(10).foreach(println)
    if(pca != 0)
      pcaTransform(ngramTF, splitRate, pca)
    else
      ngramTF.randomSplit(splitRate)
  }

  def words2Vec(spark: SparkSession,inputFile:String,splitRate:Array[Double], pca:Int, vectorsize:Int=100,minCount:Int=0): Array[Dataset[Row]]={
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
        val conf = new SparkConf().setAppName("ML Auth App").setMaster("local[4]")
        val spark = SparkSession.builder().config(conf).getOrCreate()
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        val currentDir = System.getProperty("user.dir")
        println(currentDir)
        val inputFile = "file://" + currentDir + "/"+csvfilename
        println(inputFile)



        // Data in TFIDF
        //val Array(trainData,testData) = tfidf(spark,inputFile,Array(0.7,0.3), 20000, 0)
        // Data Ngram TF
        //val Array(trainData,testData) = ngramtf(spark,inputFile,Array(0.7,0.3), 0)
        // Word2vector
        val Array(trainData,testData) = words2Vec(spark,inputFile,Array(0.7,0.3), 0)

        //----------------Seitaridis ML-------------------//

        //---- add ML algorithm here -----//

	//-----Evaluation-----------//

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

      // ------------------ Cross Validation ----------------


        //----------------Seitaridis ML------------------//
      } else {
        println("scala WordsTfIdf.scala csv_file_name")
      }

    } else {
      println("Please provide file path.")

    }

  }

}
