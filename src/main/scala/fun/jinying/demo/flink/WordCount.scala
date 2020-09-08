package fun.jinying.demo.flink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
 *
 * @author jy 
 * @date 2020/9/8
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val port: Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        return
    }
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", port, '\n', 1)
    val counts = text.flatMap(_.toLowerCase.split("\\W+"))
      .map((_, 1))
      .keyBy(_._1)
      //      .timeWindow(Time.seconds(5), Time.seconds(1))
      .sum(1)


    //    windowCounts.print().setParallelism(1)
    counts.print()
    env.execute("socket window wordCount")
  }
}
