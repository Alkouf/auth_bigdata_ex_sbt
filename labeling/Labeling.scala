package labeling

import java.io.{File, FileOutputStream, PrintWriter}

import feautext.Stemmer

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

  def writetoCSV(file: File, csvfilename: String,flag:Boolean, stemmer: Stemmer, stopWords: List[String]): Unit = {

    val pw = new PrintWriter(new FileOutputStream(new File(csvfilename),true))
    if(flag){
      pw.println('"'+stemNstopwords(this.readFiletoString(file),stemmer,stopWords)+'"'+",positive")
    }else{
      pw.println('"'+stemNstopwords(this.readFiletoString(file),stemmer,stopWords)+'"'+",negative")
    }
    pw.close()
  }


  def produceCSV(samplesdir: String,csvfile: String, stemmer: Stemmer, stopWords: List[String]): Unit ={
    this.getListOfSubDir(samplesdir).foreach(d =>if(d.toString.contains("pos")) {
      println("Number of positive reviews: " +this.getListOfFiles(d.toString).size+ ", at directory: "+d.toString)
      println("Writing positive reviews to csv...")
      this.getListOfFiles(d.toString).foreach(f=>this.writetoCSV(f,csvfile,flag=true, stemmer, stopWords))
    }else {
      println("Number of negative reviews: " +this.getListOfFiles(d.toString).size+ ", at directory: "+d.toString)
      println("Writing negative reviews to csv...")
      this.getListOfFiles(d.toString).foreach(f=>this.writetoCSV(f,csvfile,flag=false, stemmer, stopWords))})
  }

  def deletePreviousCSV(csvfilename: String): Unit ={
    new File(csvfilename).delete()
  }

  def main(args: Array[String]): Unit = {

    val stemmer =new Stemmer

    //Create a list with the stopwords
    val stopWords = scala.io.Source.fromFile(System.getProperty("user.dir")+"/stopWords.txt").getLines.toList

    //!!!
    //toCsv(stemmer, stopWords)


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
      this.produceCSV(samplesdir,csvfilename, stemmer, stopWords)
      //---------------------------------------------
      println("Writing data to :"+csvfilename)
      println("Labeling finished.")
    }else{
      println("Please provide directory path.")
    }
  }



  /**
    * @author Alexandros
    *
    * Alternative main <br>
    * proxeiri methodos pou prospernaei to args[String]
    *
    * @param stemmer
    * @param stopWords
    */
  def toCsv(stemmer: Stemmer, stopWords:List[String]): Unit ={

    //the file to write the results
    val csvfilename = System.getProperty("user.dir")+"/sampleData.csv"

    //the directory where the data are located
    val datadirectory = System.getProperty("user.dir")+"/sampleData/train"

    println("deleting existing file: "+ csvfilename)
    this.deletePreviousCSV(csvfilename)

    println("Process directory :"+datadirectory)
    println("Producing csv...")
    this.produceCSV(datadirectory,csvfilename, stemmer, stopWords)
    println("Done")

  }

  /**
    * @author Alexandros
    *
    * Stopwords and Stemming <br>
    *
    * Removes the stopwords and finds the root of every word in a sentence <br><br>
    *
    * 1. First converts the line to lower case, <br>
    * 2. Then removes the <em>punctuation<em/>, the <em>numbers<em/>, and the rest <em>special character<em/> (except apostrophe),<br>
    * 3. After that removes the stopwords, <br>
    * 4. Applies the stemming process<br>
    * 5. Finally removes the stopwords that appeared after stemming
    *
    * @param line
    * @param stemmer
    * @return the clean words of the sentence separated with spaces
    */
  def stemNstopwords(line: String, stemmer: Stemmer, stopwords: List[String]): String= {
    var cleanline =  line.toLowerCase()
    cleanline = removePunctuation(cleanline)
    cleanline = cleanline.split(" ").filter(word => !stopwords.contains(word)).mkString(" ")
    return stemmer.stemLine(cleanline).filter(w => !stopwords.contains(w)).mkString(" ")
  }

  /**
    * @author Alexandros
    *
    * Removes all special character except for "'" (apostrophe), because the list of stopwords has words like "isn't".
    *
    * @param line
    * @return
    */
  def removePunctuation(line: String): String ={
    val allowedSpecialChars :List[Char] = List('\'', ' ')
    return line.toCharArray.filter(c => {(allowedSpecialChars.contains(c) || c.isLetter)}).mkString
  }

}