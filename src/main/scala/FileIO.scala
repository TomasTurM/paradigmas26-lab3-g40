import scala.io.Source
import org.json4s._
import org.json4s.jackson.JsonMethods._

object FileIO {

  /**
   * Read subscriptions from JSON file.
   * @param filePath path to subscriptions file
   * @return list of options: Some(Subscription) for valid entries, None for malformed entries
   *         returns empty list if file not found
   */
  def readSubscriptions(filePath: String): List[Option[Subscription]] = {
    implicit val formats: Formats = DefaultFormats
    val fileContent =
      try {//error2
        val source = Source.fromFile(filePath)
        val content = source.mkString
        source.close()
        content
      }
      catch {
        case _: Exception =>
          println(s"Error: Could not load $filePath - file not found")
          sys.exit(1)
      }
    val subscriptions =  
      try {//error3
        val json = parse(fileContent)
        json.extract[List[Map[String, String]]]
      }
      catch {
        case _: Exception =>
          println("Error: Could not load $filePath - invalid JSON format")
          sys.exit(1)
      } 

    subscriptions.map { sub => //error1
      val name = sub.get("name") //some(name) o None
      val url = sub.get("url") //some(url) o None
      if (name.isEmpty || url.isEmpty) {
        println("Warning: Skipping malformed subscription (missing 'name' or 'url' field)")
        None
      } else {
        // Llegamos aquí solamente si ambos son Some(...).
        // Por eso podemos usar .get de forma segura
        // para extraer los Strings.
        Some(Subscription(name.get, url.get)) //tengo some("name") con get.name tengo "name".
      }                                       //porque Subscription(name: String, url: String)
    }
  }

  /**
   * Download feed JSON from URL.
   * @param url Reddit feed URL
   * @return Option containing JSON as String, None on network error or timeout
   */
  def downloadFeed(url: String): Option[String] = {
    try {//error8
      val source = Source.fromURL(url)
      val content = source.mkString
      source.close()
      Some(content)
    }
    catch {
      case _: Exception =>
        None
    }
  }

  /**
   * Read dictionary file line by line.
   * @param filePath path to dictionary file
   * @return Option containing list of entities, None if file missing
   */

  def readDictionaryFile(filePath: String): Option[List[String]] = {
    try {//error6
      val source = Source.fromFile(filePath)

      val lines = source.getLines()
        .map(_.trim)
        .filter(_.nonEmpty)
        .filterNot(_.startsWith("#"))
        .toList

      source.close()

      Some(lines)
    }
    catch {
      case _: Exception =>
        None
    }
  }
}
