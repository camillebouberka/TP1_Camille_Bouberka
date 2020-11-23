package samples
import java.util.logging.{Level, Logger}
import org.apache.spark.sql.functions._
import samples.config.ConfigParser
import samples.utils.SparkReaderWriter

object SampleProgram {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    val configCli = ConfigParser.getConfigArgs(args)
    val df = SparkReaderWriter.readData(configCli.inputPath, configCli.inputFormat,hasHeader = true, configCli.delimiter)

    val DfWithoutId = df.filter(col("ID") =!= configCli.id)
    SparkReaderWriter.writeData(DfWithoutId, configCli.outputPath, configCli.outputFormat, overwrite = true, Seq())

    DfWithoutId.show

    }
}
