/*
 * Copyright 2020 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.smb.syntax

import com.spotify.scio.ScioContext
import com.spotify.scio.avro._
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.smb._
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecordBase
import org.apache.beam.sdk.extensions.smb.{AvroSortedBucketIO, SortedBucketIO}
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.io.fs.MatchResult.Status
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions
import org.apache.beam.sdk.io.fs.{EmptyMatchTreatment, ResourceId}
import org.apache.beam.sdk.values.TupleTag

import scala.reflect.ClassTag

trait SortMergeBucketAvroContextSyntax {
  implicit def asSMBAvroContext(sc: ScioContext): SortMergeBucketAvroContext =
    new SortMergeBucketAvroContext(sc)
}

object SortMergeBucketAvroContext {

  trait SmbJoinInput[V] {
    def smbRead: SortedBucketIO.Read[V]

    def nonSmbRead: ScioContext => SCollection[V] = ???

    def directories: List[ResourceId]

    def supportsSmb: Boolean =
      directories.forall { directory =>
        FileSystems
          .`match`(
            directory.resolve("metadata.json", StandardResolveOptions.RESOLVE_FILE).toString,
            EmptyMatchTreatment.DISALLOW
          )
          .status() == Status.OK
      }
  }

  object SmbJoinInput {
    import org.apache.beam.sdk.io.{AvroIO => BAvroIO}
    import scala.collection.JavaConverters._

    private def toResourceDir(filename: String): ResourceId = {
      val matchResult = FileSystems.`match`(filename, EmptyMatchTreatment.DISALLOW)

      matchResult.status() match {
        case Status.OK =>
          matchResult.metadata().get(0).resourceId().getCurrentDirectory
        case status: Status =>
          throw new IllegalArgumentException(s"Lookup of file $filename failed with status $status")
      }
    }

    def ofSpecificRecord[T <: SpecificRecordBase: ClassTag: Coder](
      paths: List[String]
    ): SmbJoinInput[T] = new SmbJoinInput[T] {
      override def directories: List[ResourceId] = paths.map(toResourceDir)

      override def smbRead: SortedBucketIO.Read[T] =
        AvroSortedBucketIO
          .read(new TupleTag[T](), ScioUtil.classOf[T])
          .from(directories.map(_.toString).asJava)

      override def nonSmbRead: ScioContext => SCollection[T] =
        directories match {
          case singleDir :: Nil => _.avroFile[T](s"$singleDir/*.avro")
          case _: List[ResourceId] =>
            _.parallelize(directories.map(dir => s"$dir/*.avro"))
              .readFiles(BAvroIO.readFiles(ScioUtil.classOf[T]))
        }
    }

    def ofGenericRecord(
      paths: List[String],
      schema: Schema
    ): SmbJoinInput[GenericRecord] = new SmbJoinInput[GenericRecord] {
      override def directories: List[ResourceId] = paths.map(toResourceDir)

      override def smbRead: SortedBucketIO.Read[GenericRecord] =
        AvroSortedBucketIO
          .read(new TupleTag[GenericRecord](), schema)
          .from(directories.map(_.toString).asJava)

      override def nonSmbRead: ScioContext => SCollection[GenericRecord] = {
        implicit val grCoder = Coder.avroGenericRecordCoder(schema)

        directories match {
          case singleDir :: Nil =>
            _.avroFile[GenericRecord](s"$singleDir/*.avro", schema)
          case _: List[ResourceId] =>
            _.parallelize(directories.map(dir => s"$dir/*.avro"))
              .readFiles(BAvroIO.readFilesGenericRecords(schema))
        }
      }
    }
  }
}

final class SortMergeBucketAvroContext(@transient private val self: ScioContext)
    extends Serializable {
  import SortMergeBucketAvroContext._

  def smbAvroFile[T <: SpecificRecordBase: ClassTag: Coder](
    paths: String*
  ): SmbAvroJoinBuilder[T] = SmbAvroJoinBuilder[T](SmbJoinInput.ofSpecificRecord[T](paths.toList))

  def smbAvroFile(paths: String*)(schema: Schema): SmbAvroJoinBuilder[GenericRecord] =
    SmbAvroJoinBuilder[GenericRecord](SmbJoinInput.ofGenericRecord(paths.toList, schema))

  final case class SmbAvroJoinBuilder[I1: Coder](input1: SmbJoinInput[I1]) {
    def groupByKey: SmbGroupByKeyBuilder[I1] =
      SmbGroupByKeyBuilder[I1](input1)

    def join[I2 <: SpecificRecordBase: ClassTag: Coder](
      paths: String*
    ): SmbJoinBuilder2[I1, I2] =
      SmbJoinBuilder2[I1, I2](input1, SmbJoinInput.ofSpecificRecord[I2](paths.toList))

    def join(paths: String*)(schema: Schema): SmbJoinBuilder2[I1, GenericRecord] =
      SmbJoinBuilder2[I1, GenericRecord](input1, SmbJoinInput.ofGenericRecord(paths.toList, schema))

    def cogroup[I2 <: SpecificRecordBase: ClassTag: Coder](
      paths: String*
    ): SmbCoGroupBuilder2[I1, I2] =
      SmbCoGroupBuilder2[I1, I2](input1, SmbJoinInput.ofSpecificRecord[I2](paths.toList))

    def cogroup(paths: String*)(schema: Schema): SmbCoGroupBuilder2[I1, GenericRecord] =
      SmbCoGroupBuilder2[I1, GenericRecord](
        input1,
        SmbJoinInput.ofGenericRecord(paths.toList, schema)
      )
  }

  final case class SmbJoinBuilder2[I1: Coder, I2: Coder](
    input1: SmbJoinInput[I1],
    input2: SmbJoinInput[I2]
  ) {
    def on[K: Coder](keyClass: Class[K]) =
      new SmbTryJoin[SCollection[(K, (I1, I2))], (SCollection[I1], SCollection[I2])](
        _.sortMergeJoin(keyClass, input1.smbRead, input2.smbRead),
        sc => (input1.nonSmbRead(sc), input2.nonSmbRead(sc)),
        (input1.supportsSmb && input2.supportsSmb)
      )
  }

  final case class SmbGroupByKeyBuilder[I1: Coder](
    input: SmbJoinInput[I1]
  ) {
    def on[K: Coder](keyClass: Class[K]) =
      new SmbTryJoin[SCollection[(K, Iterable[I1])], SCollection[I1]](
        _.sortMergeGroupByKey(keyClass, input.smbRead),
        sc => input.nonSmbRead(sc),
        input.supportsSmb
      )
  }

  final case class SmbCoGroupBuilder2[I1: Coder, I2: Coder](
    input1: SmbJoinInput[I1],
    input2: SmbJoinInput[I2]
  ) {
    def and[I3 <: SpecificRecordBase: ClassTag: Coder](
      paths: String*
    ): SmbCoGroupBuilder3[I1, I2, I3] =
      SmbCoGroupBuilder3[I1, I2, I3](
        input1,
        input2,
        SmbJoinInput.ofSpecificRecord[I3](paths.toList)
      )

    def and(paths: String*)(schema: Schema): SmbCoGroupBuilder3[I1, I2, GenericRecord] =
      SmbCoGroupBuilder3[I1, I2, GenericRecord](
        input1,
        input2,
        SmbJoinInput.ofGenericRecord(paths.toList, schema)
      )

    def on[K: Coder](keyClass: Class[K]) =
      new SmbTryJoin[
        SCollection[(K, (Iterable[I1], Iterable[I2]))],
        (SCollection[I1], SCollection[I2])
      ](
        _.sortMergeCoGroup(keyClass, input1.smbRead, input2.smbRead),
        sc => (input1.nonSmbRead(sc), input2.nonSmbRead(sc)),
        (input1.supportsSmb && input2.supportsSmb)
      )
  }

  final case class SmbCoGroupBuilder3[I1: Coder, I2: Coder, I3: Coder](
    input1: SmbJoinInput[I1],
    input2: SmbJoinInput[I2],
    input3: SmbJoinInput[I3]
  ) {
    def on[K: Coder](keyClass: Class[K]) =
      new SmbTryJoin[
        SCollection[(K, (Iterable[I1], Iterable[I2], Iterable[I3]))],
        (SCollection[I1], SCollection[I2], SCollection[I3])
      ](
        _.sortMergeCoGroup(keyClass, input1.smbRead, input2.smbRead, input3.smbRead),
        sc => (input1.nonSmbRead(sc), input2.nonSmbRead(sc), input3.nonSmbRead(sc)),
        (input1.supportsSmb && input2.supportsSmb && input3.supportsSmb)
      )
  }

  final class SmbTryJoin[SmbJoinResult, FallbackResult](
    toSmbJoinResult: ScioContext => SmbJoinResult,
    toFallbackResult: ScioContext => FallbackResult,
    isSmbReadableCondition: Boolean
  ) {
    def get: SmbJoinResult = toSmbJoinResult(self)

    def getOrFallback: Either[SmbJoinResult, FallbackResult] =
      if (isSmbReadableCondition) {
        Left(toSmbJoinResult(self))
      } else {
        Right(toFallbackResult(self))
      }
  }
}
