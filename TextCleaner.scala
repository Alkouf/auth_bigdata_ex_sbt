/**
  * Created by Alex on 15/1/2017.
  */

import java.io.{File, FileInputStream}

import org.apache.poi.hssf.usermodel.HSSFWorkbook

import scala.collection.immutable.Set

class TextCleaner(){

  private var select = 0;

  private val stopwords =scala.io.Source.fromFile(System.getProperty("user.dir")+"/preprocess/stopWords.txt").getLines.toSet
  //------------------------------

  //positive count : 2006
  //negative words : 4783
  private val negativeWords1 = scala.io.Source.fromFile(System.getProperty("user.dir")+"/preprocess/sentimentLexicon/negative-words.txt").getLines.toSet
  private val positiveWords1 = scala.io.Source.fromFile(System.getProperty("user.dir")+"/preprocess/sentimentLexicon/positive-words.txt").getLines.toSet

  //------------------------------

  //pos: 1638
  //neg: 2006
  private var positiveHarvard = Set.empty[String]
  private var negativeHarvard = Set.empty[String]


  //3040
  private var harvardCompleteStemmed= Set.empty[String]


  //-------------------------------

  private val stemmer = new Stemmer


  def setup(select: Int): Unit ={
    this.select = select

    if(select==2){
      readFromXls(System.getProperty("user.dir")+"/preprocess/inquirerbasic.xls")

      println("words in stemmed positive: "+ positiveHarvard.size)
      println("words in stemmed negative: "+ negativeHarvard.size)

      harvardCompleteStemmed ++= positiveHarvard
      harvardCompleteStemmed ++= negativeHarvard

      println("words in stemmed complete: "+ harvardCompleteStemmed.size)

    }else if(select ==3){
      readFromXls(System.getProperty("user.dir")+"/preprocess/inquirerbasic.xls")

    }else if(select ==4){
      readFromXls(System.getProperty("user.dir")+"/preprocess/inquirerbasic.xls")

    }

  }


  /**
    * prints the words that are in the first sentiment lexicon
    */
  def printsentimentwords (): Unit ={

    println("Positive words:")
    positiveWords1.foreach(w=> {
      if(!w.forall(c => c.isLetter)){
        println(w)
      }
    })
    println("Negative words:")
    negativeWords1.foreach(w => {
      if(!w.forall(c => c.isLetter)){
        println(w)
      }
    })

    println("positive words : " + positiveWords1.size)
    println("negative words : "+ negativeWords1.size)
  }

  def clearText(line: String): String= {

    if(select ==0){

      return clean0Simple(line)

    }else if(select ==1){

      return clean1Sentiment(line)

    }else if(select ==2){

      return clean2HarvardLexiconComplete(line)

    }else if(select ==3){

      return clean3HarvardLexiconPositive(line)

    }else if(select==4){

      return clean4HarvardLexiconNegative(line)

    }

    println("default selection == 0")
    return clean0Simple(line)

  }

  /**
    * Vanilla cleaning
    *
    * To lower case.
    * Removes punctuation except for apostrophe and space.
    * Removes stopwords.
    * Keeps the stem of the remaining words (and remove again the stopwords, (e.g. throwing -(stem)-> throw \in stopwords)).
    *
    *
    * @param line, the document to get cleaned
    * @return
    */
  private def clean0Simple(line:String): String ={
    var cleanline =  line.toLowerCase()
    cleanline = removePunctuation(cleanline, List('\'', ' '))
    cleanline = cleanline.split(" ").filter(word => !stopwords.contains(word)).mkString(" ")
    return stemmer.stemLine(cleanline).filter(w => !stopwords.contains(w)).mkString(" ")
  }


  /**
    * Keeps only the words that have positive/negative meaning.
    * From the first lexicon which isn't too great.
    *
    * To lower case.
    * Remove punctuation except for -, +, *
    * Remove all the words that aren't in the sentiment lexicon.
    * Stem them ? mallon oxi giati exei polles lexeis to lexico pou exoun katalikseis ing klp
    *
    * @param line
    * @return
    */
  private def clean1Sentiment(line:String): String =    {
    var cleanline = line.toLowerCase
    cleanline = removePunctuation(cleanline, List('-','+','*'))
    cleanline = cleanline.split(" ").filter(word => negativeWords1.contains(word)||positiveWords1.contains(word)).mkString(" ")
    //cleanline = stemmer.stemLine(cleanline).filter(w => !stopwords.contains(w)).mkString(" ")
    return cleanline
  }

  /**
    * The line to lower case.
    * Remove special chars except for apostrophe and spaces.
    * Remove stopwords.
    * Stem the words.
    * If a stemmed word isn't a stopword, AND the word is in the stemmed harvard lexicon, keep it.
    *
    * @param line
    * @return
    */
  private def clean2HarvardLexiconComplete(line:String): String ={
    var cleanline =  line.toLowerCase()
    cleanline = removePunctuation(cleanline, List('\'', ' '))
    cleanline = cleanline.split(" ").filter(word => !stopwords.contains(word)).mkString(" ")
    return stemmer.stemLine(cleanline).filter(w => !stopwords.contains(w) && harvardCompleteStemmed.contains(w)).mkString(" ")
  }


  /**
    * Keeps only the positive words from the Harvard lexicon
    *
    * @param line
    * @return
    */
  private def clean3HarvardLexiconPositive(line:String): String ={
    var cleanline =  line.toLowerCase()
    cleanline = removePunctuation(cleanline, List('\'', ' '))
    cleanline = cleanline.split(" ").filter(word => !stopwords.contains(word)).mkString(" ")
    return stemmer.stemLine(cleanline).filter(w => !stopwords.contains(w) && positiveHarvard.contains(w)).mkString(" ")
  }

  /**
    * Only the negative words from Harvard lexicon
    *
    * @param line
    * @return
    */
  private def clean4HarvardLexiconNegative(line:String): String ={
    var cleanline =  line.toLowerCase()
    cleanline = removePunctuation(cleanline, List('\'', ' '))
    cleanline = cleanline.split(" ").filter(word => !stopwords.contains(word)).mkString(" ")
    return stemmer.stemLine(cleanline).filter(w => !stopwords.contains(w) && negativeHarvard.contains(w)).mkString(" ")
  }

  /**
    * Removes all the characters except for letters, spaces, and the characters that are given in the allowedSpecialChars list
    * and returns the resulted string.
    *
    * @param line
    * @param allowedSpecialChars
    * @return
    */
  private def removePunctuation(line: String, allowedSpecialChars : List[Char]): String ={
    return line.toCharArray.filter(c => {allowedSpecialChars.contains(c) || c.isLetter|| c==' '}).mkString
  }


  /**
    * Finds and prints the total number of words in the data.csv, as well as the testdata.csv.
    * Also prints the number of unique words in both files.
    *
    * @param csvfilename
    */
  def uniqueWordsCSV(csvfilename:String): Unit ={
    val  wordlist=  scala.io.Source.fromFile(csvfilename).getLines.toList.flatMap(line =>{
      line.split(",")(0).toCharArray.filter(c => c!='\"').mkString("").split(" ")//removing the quotes from the csv file
    })

    println("Total word on document "+ csvfilename+ " are :"+ wordlist.size+" , and distinct words: "+ wordlist.toSet.size)

    val  wordlistTest=  scala.io.Source.fromFile("test"+csvfilename).getLines.toList.flatMap(line =>{
      line.split(",")(0).toCharArray.filter(c => c!='\"').mkString("").split(" ")//removing the quotes from the csv file
    })

    println("Total word on document test"+ csvfilename+ " are :"+ wordlistTest.size+" , and distinct words: "+ wordlistTest.toSet.size)
  }


  /**
    * Read words from sentiment lexicon "Inquirer", and puts the words on the 4 "Global" sets
    *
    * @param filename
    */
  private def readFromXls(filename:String): Unit ={
    val file = new FileInputStream(new File(filename))
    val workbook = new HSSFWorkbook(file)
    val sheet = workbook.getSheetAt(0)
    val rowIterator = sheet.iterator()

    while(rowIterator.hasNext){
      val cellIterator = rowIterator.next().cellIterator()
      var counter =1
      var word = ""
      var tag = ""

      while(cellIterator.hasNext){
        if (counter== 1){
          word= stemmer.stemWord(cellIterator.next().toString.split('#')(0).toLowerCase)
        }
        tag = cellIterator.next().toString

        if(tag.compareToIgnoreCase("positiv")==0 || tag.compareToIgnoreCase("pstv")==0){
          positiveHarvard+= word
        }else if(tag.compareToIgnoreCase("negativ")==0 || tag.compareToIgnoreCase("ngtv")==0){
          negativeHarvard+=word
        }

        counter+=1
      }
    }
  }

  /**
    * print the words that have special chars
    *
    * THere was none!
    *
    */
  def printWordsWithSpecialChars(): Unit ={
    println("WORDS POSITIVE H4 WITH SPECIAL CHARS : ")
    positiveHarvard.filter( w => !w.toCharArray.forall(c => c.isLetter)).foreach(w => println(w))
    println("WORDS NEGATIVE H4 WITH SPECIAL CHARS : ")
    negativeHarvard.filter( w => !w.toCharArray.forall(c => c.isLetter)).foreach(w => println(w))
  }

}