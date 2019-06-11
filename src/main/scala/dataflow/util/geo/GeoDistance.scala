package dataflow.util.geo

object GeoDistance {
  val EARTH_RADIUS_METERS = 6378100d   //https://en.wikipedia.org/wiki/Earth_radius

  /**
    *  The distance in meters between 2 points
    */
  def meters(geo1: (Double, Double), geo2: (Double, Double)): Double = {
    calculate(geo1._1, geo1._2, geo2._1, geo2._2)
  }

  private def calculate(startLon: Double, startLat: Double, endLon: Double, endLat: Double): Double = {
    val dLat = math.toRadians(endLat - startLat)
    val dLon = math.toRadians(endLon - startLon)
    val lat1 = math.toRadians(startLat)
    val lat2 = math.toRadians(endLat)

    val a =
      math.sin(dLat / 2) * math.sin(dLat / 2) +
        math.sin(dLon / 2) * math.sin(dLon / 2) * math.cos(lat1) * math.cos(lat2)
    val c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    EARTH_RADIUS_METERS * c
  }
}
