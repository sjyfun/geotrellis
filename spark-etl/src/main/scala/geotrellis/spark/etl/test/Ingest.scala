package geotrellis.spark.etl.test

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.index._
import geotrellis.spark.tiling._
import geotrellis.spark.util.SparkUtils

import org.apache.spark.SparkConf

object Ingest {
  def main(args: Array[String]): Unit = {
    implicit val sc = SparkUtils.createSparkContext("GeoTrellis NLCD Ingest", new SparkConf(true))
    val reader = LayerReader("s3://azavea-datahub/catalog")
    val writer = LayerWriter("s3://geotrellis-test/daunnc/old-test")

    val scheme = ZoomedLayoutScheme(WebMercator)

    val layer = reader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](LayerId("nlcd-tms-epsg3857", 12))
    val levels = geotrellis.spark.pyramid.Pyramid.levelStream(layer, scheme, 12, 0)
    
    levels.foreach { case (zoom, rdd) =>
      writer.write(LayerId("nlcd-tms-epsg3857", zoom), rdd, ZCurveKeyIndexMethod)
    }
  }
}
