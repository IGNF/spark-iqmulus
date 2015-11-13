/*
 * Copyright 2015 IGN
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

package fr.ign.spark.iqmulus

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}
import org.apache.spark.deploy.SparkHadoopUtil

/**
 * Custom Input Format for reading and splitting flat binary files that contain records,
 * each of which are a fixed size in bytes. The fixed record size is specified through
 * a parameter recordLength in the Hadoop configuration.
 */
private[iqmulus] object FixedLengthBinarySectionInputFormat {
  /** Property name to set in Hadoop JobConfs for record length */
  val RECORD_OFFSET_PROPERTY = "org.apache.spark.input.FixedLengthBinarySectionInputFormat.recordOffset"
  val RECORD_COUNT_PROPERTY = "org.apache.spark.input.FixedLengthBinarySectionInputFormat.recordCount"
  val RECORD_LENGTH_PROPERTY = "org.apache.spark.input.FixedLengthBinarySectionInputFormat.recordLength"
  val RECORD_STRIDE_PROPERTY = "org.apache.spark.input.FixedLengthBinarySectionInputFormat.recordStride"

  /** Retrieves the record length property from a Hadoop configuration */
  def getConf(context : JobContext) = SparkHadoopUtil.get.getConfigurationFromJobContext(context)
  def getRecordOffset(context: JobContext): Long = getConf(context).get(RECORD_OFFSET_PROPERTY).toLong
  def getRecordCount(context: JobContext): Long = getConf(context).get(RECORD_COUNT_PROPERTY).toLong
  def getRecordLength(context: JobContext): Int = getConf(context).get(RECORD_LENGTH_PROPERTY).toInt
  def getRecordStride(context: JobContext): Int = getConf(context).get(RECORD_STRIDE_PROPERTY).toInt
}

private[iqmulus] class FixedLengthBinarySectionInputFormat
  extends FileInputFormat[LongWritable, BytesWritable] {

  private var recordOffset = -1L
  private var recordCount  = -1L
  private var recordLength = -1
  private var recordStride = -1

  /**
   * Override of isSplitable to ensure initial computation of the record length
   */
  override def isSplitable(context: JobContext, filename: Path): Boolean = {
    if (recordOffset == -1) {
      recordOffset = FixedLengthBinarySectionInputFormat.getRecordOffset(context)
    }
    if (recordCount == -1) {
      recordCount = FixedLengthBinarySectionInputFormat.getRecordCount(context)
    }
    if (recordLength == -1) {
      recordLength = FixedLengthBinarySectionInputFormat.getRecordLength(context)
    }
    if (recordStride == -1) {
      recordStride = FixedLengthBinarySectionInputFormat.getRecordStride(context)
    }
    if (recordLength <= 0) {
      println("record length is less than 0, file cannot be split")
      false
    } else {
      true
    }
  }

  /**
   * This input format overrides computeSplitSize() to make sure that each split
   * only contains full records. Each InputSplit passed to FixedLengthBinarySectionRecordReader
   * will start at the first byte of a record, and the last byte will the last byte of a record.
   */
  override def computeSplitSize(blockSize: Long, minSize: Long, maxSize: Long): Long = {
    val defaultSize = super.computeSplitSize(blockSize, minSize, maxSize)
    // If the default size is less than the length of a record, make it equal to it
    // Otherwise, make sure the split size is as close to possible as the default size,
    // but still contains a complete set of records, with the first record
    // starting at the first byte in the split and the last record ending with the last byte
    if (defaultSize < recordStride) {
      recordStride.toLong
    } else {
      (Math.floor(defaultSize / recordStride) * recordStride).toLong
    }
  }

  /**
   * Create a FixedLengthBinarySectionRecordReader
   */
  override def createRecordReader(split: InputSplit, context: TaskAttemptContext)
      : RecordReader[LongWritable, BytesWritable] = {
    new FixedLengthBinarySectionRecordReader
  }
}
