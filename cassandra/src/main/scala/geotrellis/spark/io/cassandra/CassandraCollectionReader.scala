/*
 * Copyright 2016 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.spark.io.cassandra

import geotrellis.spark.{Boundable, KeyBounds, LayerId}
import geotrellis.spark.io._
import geotrellis.spark.io.avro.codecs.KeyValueRecordCodec
import geotrellis.spark.io.avro.{AvroEncoder, AvroRecordCodec}
import geotrellis.spark.io.cassandra.conf.CassandraConfig
import geotrellis.spark.io.index.{KeyIndex, MergeQueue}
import geotrellis.spark.util.KryoWrapper
import org.apache.avro.Schema

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import java.math.BigInteger

object CassandraCollectionReader {
  final val defaultThreadCount = CassandraConfig.threads.collection.readThreads

  def read[K: Boundable : AvroRecordCodec : ClassTag, V: AvroRecordCodec : ClassTag](
    instance: CassandraInstance,
    keyspace: String,
    table: String,
    layerId: LayerId,
    queryKeyBounds: Seq[KeyBounds[K]],
    decomposeBounds: KeyBounds[K] => Seq[(BigInt, BigInt)],
    filterIndexOnly: Boolean,
    writerSchema: Option[Schema] = None,
    keyIndex: KeyIndex[K],
    threads: Int = defaultThreadCount
  ): Seq[(K, V)] = {
    if (queryKeyBounds.isEmpty) return Seq.empty[(K, V)]

    val indexStrategy = new CassandraIndexing[K](keyIndex, instance.cassandraConfig.tilesPerPartition)

    val includeKey = (key: K) => queryKeyBounds.includeKey(key)
    val _recordCodec = KeyValueRecordCodec[K, V]
    val kwWriterSchema = KryoWrapper(writerSchema) //Avro Schema is not Serializable

    val ranges = if (queryKeyBounds.length > 1)
      MergeQueue(queryKeyBounds.flatMap(decomposeBounds))
    else
      queryKeyBounds.flatMap(decomposeBounds)

    val query = indexStrategy.queryValueStatement(
      instance.cassandraConfig.indexStrategy,
      keyspace, table, layerId.name, layerId.zoom
    )

    instance.withSessionDo { session =>
      val statement = indexStrategy.prepareQuery(query)(session)

      LayerReader.njoin[K, V](ranges.toIterator, threads){ index: BigInt =>
        val row = session.execute(indexStrategy.bindQuery(
          instance.cassandraConfig.indexStrategy,
          statement, index: BigInteger
        ))

        if (row.asScala.nonEmpty) {
          val bytes = row.one().getBytes("value").array()
          val recs = AvroEncoder.fromBinary(kwWriterSchema.value.getOrElse(_recordCodec.schema), bytes)(_recordCodec)
          if (filterIndexOnly) recs
          else recs.filter { row => includeKey(row._1) }
        } else Vector.empty
      }
    }: Seq[(K, V)]
  }
}
