import com.snowflake.snowpark._
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.types._
import sys.process._
import scala.math.sin
import scala.math.cos
import scala.math.atan2
import scala.math.toRadians
import scala.math._
import java.time.LocalDateTime
import java.time.Duration;

object Main {
  def main(args: Array[String]): Unit = {
    // Replace the <placeholders> below.
    val configs = Map (
      "URL" -> "https://[ACCOUNT LOCATOR].snowflakecomputing.com:443",
      "USER" -> "[YOUR USERNAME]",
      "PASSWORD" -> "[YOUR PASSWORD]",
      "ROLE" -> "[YOUR ROLE]",
      "WAREHOUSE" -> "[YOUR WAREHOUSE]",
      "DB" -> "CITIBIKE",
      "SCHEMA" -> "DEMO"
    )
    val session = Session.builder.configs(configs).create
    
// SIMPLE AGGREGATIONS AND QUERIES OF TRIPS DATA (CREATE HOURLY AGGREGATE TABLE FROM INDIVIDUAL TRIPS)
    session.sql("show tables").show()
    val df_trips_hourly = session.table("trips_weather_vw")
    println("Raw hourly trips count: " + df_trips_hourly.count()) 

    val df_trips_daily = df_trips_hourly
                            .select(date_trunc("day", col("STARTTIME")) as "day")
                            .groupBy(col("day"))
                            .count().select(col("day"), col("COUNT"))
    println("Aggregated daily trips count: " + df_trips_daily.count())
    df_trips_daily.show(5)
    df_trips_daily.write.mode(SaveMode.Overwrite).saveAsTable("trips_daily")

    println("Aggregated daily trips count: " + session.table("trips_daily").count())

// SIMPLE TEST UDF -- JUST ADDS THE LAST NAME "JOHNSON" TO ANY FIRST NAME INPUT
    // Define the anonymous UDF.
    val appendLastNameUdf = udf((new UDFCode).appendLastNameFunc)
    // Create a DataFrame that has a column NAME with a single row with the value "Raymond".
    val df = session.sql("select 'Raymond' NAME")
    // Call the UDF, passing in the values in the NAME column.
    // Return a new DataFrame that has an additional column "Full Name" that contains the value returned by the UDF.
    df.withColumn("Full Name", appendLastNameUdf(col("NAME"))).show()
    
// MORE INTERESTING DISTANCE KMs UDF
    val geoDistUDF = udf((new DistanceCalculatorImpl).calculateDistanceInKilometer _)
    // SIMPLE TEST OF THE DISTANCE CALC UDF USING A SINGLE SET OF COORDINATES
      // DEFINE THE COORDINATES AND PUT THEM IN A DATA FRAME
        var lat1 = 43.784049; var lon1 = -79.128794; var lat2 = 43.66493975822026; var lon2 = -79.3754006905373
        val myDf = session.sql("select "+lat1+" LAT1,"+lon1+" LON1,"+lat2+" LAT2,"+lon2+" LON2")
    //RUN THE CALCULATION AGAINST OUR SINGLE RECORD AND SHOW THE RESULTS
    myDf.withColumn("DIST_KM", geoDistUDF(col("LAT1"),col("LON1"),col("LAT2"),col("LON2"))).show()

    // MORE INTERESTING USE OF PUSHDOWN + LAZY EXECUTION TO CALCUATE AND STORE TRIP DISTANCES
    val df_trips = session.table("trips")
    val df_stations = session.table("stations_vw")
    println("Trip Count: "+df_trips.count())
    //### Join our Trips to their matching Start and End Stations
    var df_trips_stations_start = df_trips
                  .join(df_stations,df_trips("START_STATION_ID") === df_stations("STATION_ID"))
                  .select(col("TRIPDURATION"),col("START_STATION_ID"),col("END_STATION_ID"),col("STATION_LAT").as("START_STATION_LAT"),col("STATION_LON").as("START_STATION_LON"))
    
    var df_trips_stations = df_trips_stations_start
                  .join(df_stations,df_trips_stations_start("END_STATION_ID") === df_stations("STATION_ID"))
                  .select(col("TRIPDURATION"),col("START_STATION_ID"),col("END_STATION_ID"),col("START_STATION_LAT"),col("START_STATION_LON"),col("STATION_LAT").as("END_STATION_LAT"),col("STATION_LON").as("END_STATION_LON"))
    // Show the new trips_stations table schema
    df_trips_stations.schema

    // Now build a real table... this step just defines what that table should look like
    val distancesDF = df_trips_stations.withColumn("DIST_KM", geoDistUDF(col("START_STATION_LAT"),col("START_STATION_LON"),col("END_STATION_LAT"),col("END_STATION_LON")))
    // Nothing has run yet, because of lazy execution. No joins, no table created, etc.

    // Here is where we actually push down the code to run the joins remotely and create the table with no I/O or local CPU
    distancesDF.write.mode(SaveMode.Overwrite).saveAsTable("trips_distances")

    session.table("trips_distances").limit(10).show
  }
}

// SIMPLE TEST UDF -- JUST ADDS THE LAST NAME "JOHNSON" TO ANY FIRST NAME INPUT
class UDFCode extends Serializable {
  val appendLastNameFunc = (s: String) => {
    s"$s Johnson"
  }
}

class DistanceCalculatorImpl extends Serializable{
    private val AVERAGE_RADIUS_OF_EARTH_KM = 6371
    def calculateDistanceInKilometer(fromLat:Double, fromLong:Double, toLat:Double, toLong:Double ) : Double = {
        val latDistance = math.toRadians(fromLat - toLat)
        val lngDistance = math.toRadians(fromLong - toLong)
        val sinLat = math.sin(latDistance / 2)
        val sinLng = math.sin(lngDistance / 2)
        val a = sinLat * sinLat +
        (math.cos(math.toRadians(fromLat)) *
            math.cos(math.toRadians(toLat)) *
            sinLng * sinLng)
        val c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        (AVERAGE_RADIUS_OF_EARTH_KM * c).toDouble
    }
  }