package com.luogh.spark.tasks

import java.io.{BufferedInputStream, InputStream}
import java.sql.DriverManager
import java.util

import com.luogh.spark.common.{LoadVerticaParam, Task}
import com.luogh.spark.util.HDFSUtil
import com.vertica.jdbc.{VerticaConnection, VerticaCopyStream}
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

/**
  * @author luogh 
  */
object VerticaFunction {

  val LOGGER = LoggerFactory.getLogger(VerticaFunction.getClass)

  Class.forName("com.vertica.jdbc.Driver")

  implicit def connection: VerticaConnection = {
    val conn = DriverManager.getConnection("jdbc:vertica://172.18.31.10:5433/tripapp", "trip_user", "Talkingdata@FE")
    conn.setAutoCommit(false)
    conn.asInstanceOf[VerticaConnection]
  }

  def withConnection[T](operate: VerticaConnection => T)(implicit con: VerticaConnection): T = {
    try {
      val result = operate(con)
      con.commit()
      result
    } catch {
      case e: Throwable =>
        con.rollback()
        LOGGER.error("vertica operation faild.", e)
        throw e
    } finally {
      con.close()
    }
  }

  implicit class LoadIntoVerticaOperate(task: Task) {

    def loadIntoVertica(param: LoadVerticaParam)(connection: VerticaConnection): Unit = {
      try {
        val path = param.dataPath
        val tableName = param.tableName
        LOGGER.info(s"Load into vertica params:${param}")
        val fileStatuses = HDFSUtil.fileSystem.globStatus(new Path(s"${path}/*"))
        for (status <- fileStatuses if status.isFile && status.getLen > 0) {
          val dataPath = status.getPath
          var stream: BufferedInputStream = null
          try {
            stream = new BufferedInputStream(HDFSUtil.open(dataPath.toString), 2048)
            val success = copy(tableName, stream, connection)
            if (!success) {
              throw new RuntimeException(s"Insert data ${dataPath} into Vertica table ${tableName}, failed.")
            }
            LOGGER.info(s"Insert data ${dataPath} into Vertica table ${tableName}, success.")
          } catch {
            case e: RuntimeException => throw e
            case other: Exception =>
              LOGGER.error(s"Insert data ${dataPath} into Vertica table ${tableName}, failed.", other)
          } finally {
            if (stream != null) {
              stream.close()
            }
          }
        }
      }

      def copy(tableName: String, inputStream: InputStream, connection: VerticaConnection): Boolean = {
        try {
          val sql: String = s"COPY  ${tableName} FROM STDIN DELIMITER ',' DIRECT ENFORCELENGTH"
          val stream = new VerticaCopyStream(connection, sql)
          stream.start()
          stream.addStream(inputStream)
          stream.execute()
          val rejects: util.List[java.lang.Long] = stream.getRejects
          // The size of the list gives you the number of rejected rows.
          val numRejects: Int = rejects.size
          LOGGER.warn("Number of rows rejected in load #" + ": " + numRejects)
          // List all of the rows that were rejected.
          val rejit: util.Iterator[java.lang.Long] = rejects.iterator
          var lineCount: Long = 0
          while (rejit.hasNext) {
            lineCount = lineCount + 1
            LOGGER.warn(s"Rejected row # ${lineCount} is row ${rejit.next}")
          }
          stream.finish
          true
        } catch {
          case e: Exception =>
            throw e
        }
      }
    }
  }

}

