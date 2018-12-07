package fr.ign.spark.iqmulus.xyz
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.hadoop.mapreduce.TaskAttemptContext

private[xyz] class XyzOutputWriter(
  filename: String,
  dataSchema: StructType,
  context: TaskAttemptContext)
  extends OutputWriter {

  private lazy val dos = {
    val path = new org.apache.hadoop.fs.Path(filename)
    val fs = path.getFileSystem(context.getConfiguration)
    val f = fs.create(path)
    new java.io.DataOutputStream(f)
  }

  override def write(row: InternalRow): Unit = {
    dos.writeBytes(row.toSeq(dataSchema).mkString("", "\t", "\n"))
  }

  override def close(): Unit = dos.close
}
