import java.io.{File, FileOutputStream, PrintWriter}

import scala.io.Source

/**
  * Created by george on 15/12/2016.
  */

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

  def writetoCSV(file: File, csvfilename: String,flag:Boolean): Unit = {

    val pw = new PrintWriter(new FileOutputStream(new File(csvfilename),true))
      if(flag){
        pw.println('"'+this.readFiletoString(file)+'"'+",positive")
      }else{
        pw.println('"'+this.readFiletoString(file)+'"'+",negative")
      }
      pw.close()
  }

  def produceCSV(samplesdir: String,csvfile: String): Unit ={
    this.getListOfSubDir(samplesdir).foreach(d =>if(d.toString.contains("pos")) {
      this.getListOfFiles(d.toString).foreach(f=>this.writetoCSV(f,csvfile,flag=true))
      }else{
      this.getListOfFiles(d.toString).foreach(f=>this.writetoCSV(f,csvfile,flag=true))})
  }

  def deletePreviousCSV(csvfilename: String): Unit ={
    new File(csvfilename).delete()
  }

  def main(args: Array[String]): Unit = {
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
      this.produceCSV(samplesdir,csvfilename)
      //---------------------------------------------
      println("Writing data to :"+csvfilename)
      println("Labeling finished.")
    }else{
      println("Please provide directory path.")
    }


  }

}