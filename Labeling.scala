/**
  * Created by george on 15/12/2016.
  */
import java.io.File
import java.io.PrintWriter
import java.io.FileOutputStream

import scala.io.Source

object Labeling {

  def getListOfFiles(directory: String): List[File] = {
    val dir = new File(directory)
    if (dir.exists() && dir.isDirectory) {
      dir.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def getListOfSubDir(directory: String): List[File] = {
    val dir = new File(directory)
    if (dir.isDirectory && dir.exists()) {
      dir.listFiles().filter(_.isDirectory).toList
    } else {
      List[File]()
    }
  }

  def readFiletoString(datafn: File): String = {
    Source.fromFile(datafn).getLines().mkString
  }

  def writetoCSV(file: File, csvfilename: String, flag:String, textCleaner: TextCleaner): Unit = {

    val pw = new PrintWriter(new FileOutputStream(new File(csvfilename),true))

    pw.println('"'+textCleaner.clearText(this.readFiletoString(file))+'"'+","+flag)

    pw.close()
  }


  def produceCSV(samplesdir: String,csvfile: String, textCleaner: TextCleaner): Unit ={
    this.getListOfSubDir(samplesdir).foreach(d =>if(d.toString.contains("/train")){

      this.getListOfSubDir(d.toString).foreach(d =>if(d.toString.contains("pos")) {
        println("Number of positive reviews: " +this.getListOfFiles(d.toString).size+ ", at directory: "+d.toString)
        println("Writing positive reviews to csv...")
        this.getListOfFiles(d.toString).foreach(f=>this.writetoCSV(f,csvfile,flag="1", textCleaner))
      }else if(d.toString.contains("neg")) {
        println("Number of negative reviews: " +this.getListOfFiles(d.toString).size+ ", at directory: "+d.toString)
        println("Writing negative reviews to csv...")
        this.getListOfFiles(d.toString).foreach(f=>this.writetoCSV(f,csvfile,flag="0", textCleaner))
      })

    }else {//test

      println("Number of test reviews: " +this.getListOfFiles(d.toString).size+ ", at directory: "+ "Test"+d.toString)
      println("Writing test set reviews to csv...")
      this.getListOfFiles(d.toString).foreach(f=>this.writetoCSV(f,"test"+csvfile,flag="?", textCleaner ))

    })
  }

  def deletePreviousCSV(csvfilename: String): Unit ={
    new File(csvfilename).delete()
    new File("test"+csvfilename).delete()
  }

  def lineDistinctWords(line:String):String= {
    var docdistinctWords = scala.collection.mutable.Set.empty[String]
    line.split(" ").foreach(word => docdistinctWords.add(word))
    docdistinctWords.mkString(" ")
  }

  def produceDFFilter(samplesdir: String,minDF:Int=2,maxDF:Int=23000):Array[String]= {

    val nf=this.getListOfSubDir(samplesdir).flatMap(d=>this.getListOfFiles(d.toString)).map(f=>1).sum
    val df = this.getListOfSubDir(samplesdir).flatMap(d=>this.getListOfFiles(d.toString)).
      flatMap(file=>Source.fromFile(file).getLines()).map(line=>this.lineDistinctWords(removePunctuation(line))).
      flatMap(_.split(" ")).foldLeft(Map.empty[String,Int]){
      (count,word)=> count + (word ->(count.getOrElse(word,0) + 1))
    }
    val dfiltered=df.filter(K=>(K._2<minDF)||(K._2>maxDF))
    dfiltered.map(K=>K._1).toArray
}


  def main(args: Array[String]): Unit = {

    val timeStart = System.currentTimeMillis()

    val textCleaner = new TextCleaner
    textCleaner.setup(1)

    var  csvfilename : String =""
    if (args.length != 0) {
      val samplesdir :String = args(0)
      if(!(samplesdir=="--help")){
        if (args.length < 2) {
          csvfilename = "data.csv"
        } else {
          csvfilename = args(1)
        }
      }else{
        println("scala Labeling.scala samples_dir_path csv_file_path")
      }
      println("Starting Labeling")
      println("Delete previous CSV files with the same name."+samplesdir)
      this.deletePreviousCSV(csvfilename)
      println("Process directory :"+samplesdir)
      //---------------------------------------------
      this.produceCSV(samplesdir,csvfilename, textCleaner)
      //---------------------------------------------
      println("Writing data to :"+csvfilename)
      println("Labeling finished.")
    }else{
      println("Please provide directory path.")
    }

    println("Time elapsed till now in seconds: "+ (System.currentTimeMillis()-timeStart)/1000.0)

    textCleaner.uniqueWordsCSV(csvfilename)

    println("Total time elapsed in seconds: "+ (System.currentTimeMillis()-timeStart)/1000.0)
  }


}
