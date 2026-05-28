import org.apache.spark.{ HashPartitioner, Partitioner }
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.util.StatCounter

val conf = new SparkConf()
  .setMaster("local")
  .setAppName("RunTaxiTrips")

sc.setLogLevel("ERROR")

import java.text.SimpleDateFormat
val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

import com.esri.core.geometry.{ GeometryEngine, SpatialReference, Geometry, Point }
import com.github.nscala_time.time.Imports.{ DateTime, Duration }

case class TaxiTrip (
  pickupTime:  org.joda.time.DateTime,
  dropoffTime: org.joda.time.DateTime,
  pickupLoc:   com.esri.core.geometry.Point,
  dropoffLoc:  com.esri.core.geometry.Point) extends java.io.Serializable

def parse(line: String): (String, TaxiTrip) = {
  val fields = line.split(',')
  val license = fields(1)
  val pickupTime = new org.joda.time.DateTime(formatter.parse(fields(5)))
  val dropoffTime = new org.joda.time.DateTime(formatter.parse(fields(6)))
  val pickupLoc = new Point(fields(10).toDouble, fields(11).toDouble)
  val dropoffLoc = new Point(fields(12).toDouble, fields(13).toDouble)
  val trip = TaxiTrip(pickupTime, dropoffTime, pickupLoc, dropoffLoc)
  (license, trip)
}

def safe[S, T](f: S => T): S => Either[T, (S, Exception)] = {
  new Function[S, Either[T, (S, Exception)]] with Serializable {
    def apply(s: S): Either[T, (S, Exception)] = {
      try {
        Left(f(s))
      } catch {
        case e: Exception => Right((s, e))
      }
    }
  }
}

//----------------- Parse & Filter the Taxi Trips -------------------------

val taxiRaw = sc.textFile("../Data/nyc-taxi-trips") //.sample(false, 0.01) // use 1 percent sample size for debugging!
val taxiParsed = taxiRaw.map(safe(parse))
taxiParsed.cache()

val taxiBad = taxiParsed.collect({
  case t if t.isRight => t.right.get
})

// Better than random sampling: selecting just 1 day of the month does not disrupt the sessions per day!
val taxiGood = taxiParsed.collect({
  case t if t.isLeft => t.left.get
}).filter(trip => trip._2.pickupTime.getDayOfMonth() == 13)
taxiGood.cache() // cache good lines for later re-use

println("\n" + taxiGood.count() + " taxi trips parsed.")
println(taxiBad.count() + " taxi trips dropped.")

def getHours(trip: TaxiTrip): Long = {
  val d = new Duration(
    trip.pickupTime,
    trip.dropoffTime)
  d.getStandardHours
}

println("\nDistribution of trip durations in hours:")
taxiGood.values.map(getHours).countByValue().
  toList.sorted.foreach(println)

val taxiClean = taxiGood.filter {
  case (lic, trip) =>
    val hrs = getHours(trip)
    0 <= hrs && hrs < 3
}

val taxiDone = taxiClean.filter {
  case (lic, trip) =>
    val zero = new Point(0.0, 0.0)
    (zero != trip.pickupLoc && zero != trip.dropoffLoc)
}
taxiDone.cache()
taxiGood.unpersist()
taxiParsed.unpersist()

//----------------- Parse the NYC Boroughs Polygons -----------------------

import scala.io.Source
val geojson = Source.fromFile("../Data/nyc-borough-boundaries-polygon.geojson").mkString

import spray.json._
val json = geojson.parseJson

json.prettyPrint // prints a short version
//println(json.prettyPrint) // prints everyting!

import geojson2esri._ // this imports our custom GeoJson case classes
import geojson2esri.GeoJsonProtocol._ // .. and the converters!
val features = json.convertTo[FeatureCollection]

val p = new Point(-73.994499, 40.75066) // look up the borough of some test point
val b = features.find(f => f.geometry.contains(p))

val areaSortedFeatures = features.sortBy(f => {
  val borough = f("boroughCode").convertTo[Int]
  (borough, -f.geometry.area2D())
})
areaSortedFeatures.foreach(println)

val bFeatures = sc.broadcast(areaSortedFeatures)

def borough(trip: TaxiTrip): Option[String] = {
  val feature: Option[Feature] = bFeatures.value.find(f => {
    f.geometry.contains(trip.dropoffLoc)
  })
  feature.map(f => {
    f("borough").convertTo[String]
  })
}

println("\nDistribution of trips per borough:")
taxiDone.values.map(borough).countByValue().foreach(println)

println("\nTaxi trips with empty borough:")
taxiDone.values.filter(t => borough(t).isEmpty).take(10).foreach(println) 

//----------------- Helper Classes for "Sessionization" -------------------

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class FirstKeyPartitioner[K1, K2](partitions: Int) extends org.apache.spark.Partitioner {
  val delegate = new org.apache.spark.HashPartitioner(partitions)
  override def numPartitions: Int = delegate.numPartitions
  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[(K1, K2)]
    delegate.getPartition(k._1)
  }
}

def secondaryKey(trip: TaxiTrip) = trip.pickupTime.getMillis

def split(t1: TaxiTrip, t2: TaxiTrip): Boolean = {
  val p1 = t1.pickupTime
  val p2 = t2.pickupTime
  val d = new Duration(p1, p2)
  d.getStandardHours >= 4
}

def groupSorted[K, V, S](
  it:        Iterator[((K, S), V)],
  splitFunc: (V, V) => Boolean): Iterator[(K, List[V])] = {
  val init = List[(K, ArrayBuffer[V])]()
  it.foldLeft(init)((list, next) => list match {
    case Nil =>
      val ((lic, _), trip) = next
      List((lic, ArrayBuffer(trip)))
    case cur :: rest =>
      val (curLic, trips) = cur
      val ((lic, _), trip) = next
      if (lic != curLic || splitFunc(trips.last, trip)) {
        (lic, ArrayBuffer(trip)) :: list
      } else {
        trips.append(trip)
        list
      }
  }).map { case (lic, buf) => (lic, buf.toList) }.iterator
}

val sessions = taxiDone.map {
  case (lic, trip) => ((lic, secondaryKey(trip)), trip)
}.repartitionAndSortWithinPartitions(new FirstKeyPartitioner(30)).mapPartitions(groupSorted(_, split))
sessions.cache()

println("\nSample sessions:")
sessions.take(10).foreach(println)

println("\nSample sessions with more than one trip:")
sessions.filter(x=>x._2.size>1).take(10).foreach(println)

println("\nConsecutive trips within first larger session:")
sessions.filter(x=>x._2.size>2).first()._2.sliding(2).foreach(println)

//----------------- Final Analysis of the Trip Durations ------------------

def boroughDuration(t1: TaxiTrip, t2: TaxiTrip) = {
  val b = borough(t1)
  val d = new Duration(t1.dropoffTime, t2.pickupTime)
  (b, d)
}

val boroughDurations: RDD[(Option[String], Duration)] =
  sessions.values.flatMap(trips => {
    val iter: Iterator[Seq[TaxiTrip]] = trips.sliding(2)
    val viter = iter.filter(_.size == 2)
    viter.map(p => boroughDuration(p(0), p(1)))
  }).cache()

println("\nDistribution of wait-times in hours:")
boroughDurations.values.map(_.getStandardHours).countByValue().toList.
  sorted.foreach(println)

println("\nFinal stats of wait-times in seconds per borough:")
val boroughStats = boroughDurations.filter {
  case (b, d) => d.getMillis >= 0
}.mapValues(d => {
  val s = new StatCounter()
  s.merge(d.getStandardSeconds.toDouble)
}).
  reduceByKey((a, b) => a.merge(b)).collect()
boroughStats.foreach(println)