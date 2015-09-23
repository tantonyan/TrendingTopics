import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.util.parsing.json._
//import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Date

/**
def convert_to_1hr(timestamp: Long): String = {
   
   val min30 = timestamp.slice(11,13).toInt/30*30
   timestamp.take(11) + f"${min30}%02d" + "00"
}
*/
def convert_to_1hr(time: Date): String = {
   val hourFormat = new SimpleDateFormat("yyyyMMddHH")
   hourFormat.format(time)
}
/**
def matchTweet(json_line: String): String = x match {
}
*/
//val tweet = JSON.parseFull("""{"text": "@ImYourEpiphany \ud83d\ude02\ud83d\ude02\ud83d\ude02 no. Not at all.", "coordinates": [], "timestamp_ms": "1442469035685", "entities": {"user_mentions": ["ImYourEpiphany","ZafulUs"], "hashtags": ["Zaful","THashTag"]}, "place": {"full_name": "Bell, CA", "country_code": "US", "name": "Bell", "place_type": "city"}, "user": {"id_str": "3271546344", "location": "Los Angeles, CA"}, "id_str": "644387956707295233"}""")

//val conf = new SparkConf().setAppName("PriceDataExercise")
//val sc = new SparkContext(conf)
//val fileIn = "hdfs://ec2-54-209-187-157.compute-1.amazonaws.com:9000/user/test-tweets/json-tweets-10.txt"
//val fileOut = "hdfs://ec2-54-209-187-157.compute-1.amazonaws.com:9000/user/test-tweets/jsons/json-tweets-10.out2"
val fileIn = "TrendingTopics/temp/jsons/json-tweets-10.txt"
val fileOut = "TrendingTopics/temp/jsons/json-tweets-10.out2"
val file = sc.textFile(fileIn)
val trends = file.map(line => {
// println("before a tweet")
// println(json_tweet)
// println("after a tweet")

 val json_tweet = JSON.parseFull(line)
 json_tweet match {
  case Some(t: Map[String, _]) => {
    val t_id = t("id_str") // String / Long
    val text = t("text") // String — the tweet
    val user = t("user") // Map
    val coords = t("coordinates") // List

    val time_ms_str = t("timestamp_ms").toString // String 
    val time_ms : Long = time_ms_str.toLong // Long
    val date = new Date(time_ms)
    val hourSlot = convert_to_1hr(date)

    val place = t("place") // Map
    val p = place.asInstanceOf[Map[String,String]]
    val city = p("name")
    val country = p("country_code")

    val entities = t("entities") // Map
    val e = entities.asInstanceOf[Map[String,List[String]]]
    val userMentions = e("user_mentions")
    val hashtags = e("hashtags")


    // ((time, (cc,city)), (tag, 1)) (id?)
    val trendingUsers = userMentions.map(user => {
				((hourSlot, (country, city), user), 1)
			})
    val trendingTags = hashtags.map(tag => {
				((hourSlot, (country, city), tag), 1)
			})

    val trendingItems = trendingTags ::: trendingUsers // combine both

    

//    println(trendingItems)
    
//    (t_id, place._1, place._2)
  } // case  t:
  case _ => ((0, (0,0),0), 1)
 }
//}).reduceByKey(_+_).flatten

})
print(trends)
trends.saveAsTextFile(fileOut)

