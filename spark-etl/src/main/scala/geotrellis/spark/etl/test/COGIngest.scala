package geotrellis.spark.etl.test

import geotrellis.raster._
import geotrellis.raster.io.geotiff.compression.{NoCompression, DeflateCompression}
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.index._
import geotrellis.spark.io.s3._
import geotrellis.spark.io.s3.cog._
import geotrellis.spark.util.SparkUtils
import org.apache.spark.SparkConf

object COGIngest {
  def main(args: Array[String]): Unit = {
    implicit val sc = SparkUtils.createSparkContext("GeoTrellis COG NLCD Ingest", new SparkConf(true))
    val reader = LayerReader("s3://azavea-datahub/catalog")
    val attributeStore = new S3AttributeStore("geotrellis-test", "daunnc/new-test2")
    val writer = new S3COGLayerWriter(attributeStore)

    val layer = reader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](LayerId("nlcd-tms-epsg3857", 12))

    writer.write("nlcd-tms-epsg3857-cog", layer, 12, ZCurveKeyIndexMethod, NoCompression)
  }
}
