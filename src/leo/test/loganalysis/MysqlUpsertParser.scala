package leo.test.loganalysis

import java.io.File
import java.util.regex._
import scala.io.Source
import scala.collection._

class MysqlUpsertParser(logFile: File, pid: String, showDiffOnly: Boolean = false, format: String = "console") {
  /* scala's Regex, any group, you can get the corresponding value very easily */
  
  // find any update and insert sqls
  private val UpsertSqlParser = (""".*\s+(update|insert\s+into)\s+(z[bfg]_\w+).*PID:""" + pid + """\s+\*/$""").r
  // have to group the variable in regexp.
  private val UpdateColumnParser = """.*set\s+(.*)\s+where\s+.*""".r
  private val UpdateColumn = """(\w+)\s*=\s*(.*)""".r
  private val UpdateIDParser = """.+where\s.*id=('[a-f0-9]{32}').*""".r
  
  private val InsertParser = """.*insert\s+into\s+\S+\s+\((.+)\)\s+values\s*\((.+)\)\s+.*""".r

  private val source = Source.fromFile(logFile, 1024 * 1024)
  val upsertSqls = source.getLines.filter(line => UpsertSqlParser.pattern.matcher(line).matches())
  // zipWithIndex is a way to make sequence number visible in the block.
  val tuple = upsertSqls.zipWithIndex.map(t => {
    val line = t._1
    val index = t._2
    // pattern matching to get the variables
    val UpsertSqlParser(action, table) = line

    var columnPairs: Array[(String, String)] = Array()
    action match {
      case "insert into" =>
        val InsertParser(columns, values) = line
        // the two set must have same size, by using `zip` get can get them paired.
        columnPairs = columns.split(",\\s").zip(values.split(",\\s"))
      case "update" =>
        // Add id column so that we can group/sort by id for same table
        val UpdateColumnParser(columnStr) = line
        if (UpdateIDParser.pattern.matcher(line).matches()){
          val UpdateIDParser(id) = line
          columnPairs = columnPairs :+ ("id", id)
        }
        // in format of set col=value, col2=value2, ...
        columnPairs = columnPairs ++: columnStr.split(",\\s").map(item => {
          val UpdateColumn(key, value) = item
          (key, value)
        })

    }

    (index, table, action, columnPairs)
  })
  // group the statements by tableName, <table_name, List[Ops]>
  // use an empty map as accumulator to group each sql, by this way, we don't need explicitly defind `var`
  val tableMap = tuple.foldLeft(mutable.Map[String, List[(Int, String, Array[(String, String)])]]()) { (m, t) =>
    if (m.contains(t._2))
      m(t._2) = m(t._2) :+ (t._1, t._3, t._4)
    else
      m(t._2) = List((t._1, t._3, t._4))
    m
  }

  private def getId(tp: (Int, String, Array[(String, String)])): String = {
    val option = tp._3.filter(p => p._1 == "id")
    if (option.isEmpty)
      null
    else
      option(0)._2
  }

  def prettyPrint = {
    tableMap.foreach(entry => {
      // sort then merge if same id
      // `ops` is one column in the output table since in <idx, action, values>, the `idx` and `action` are put in header of the column for `values`
      val ops = entry._2.sortBy(x => {
        val values = x._3
        val found = values.find(t => t._1 == "id")
        if (!found.isEmpty) found.get._2.hashCode()
        else x._1
      })
      
      // keep a record of previous one ?
      val mergedOps = if (showDiffOnly){
        var prev = ops(0)
        // the mapped result is set to `mergedOps`
        ops.zipWithIndex.map(oo => {
          val t = oo._1
          val idx = oo._2
          
          if (prev == t) {
            // do nothing
          } else if (getId(prev) == getId(t)) {
            // not first one, but same id
            // compare prev and t, clean t if it's value is same with prev
            t._3.zipWithIndex.foreach(tt => {
              val t2 = tt._1
                  val idx = tt._2
                  prev._3.foreach(t1 =>{
                    if (t1._1 == t2._1 && t1._2 == t2._2) // same column, same value
                      t._3.update(idx, (t2._1, ""))
                  })
            })
          } else {
            prev = t
          }
          t
        })
      } else ops
      
      // print header
      println(f"\n${entry._1}%-30s\n-========================================================")
      val opNames = mergedOps.map(op => op._1 + "/" + op._2)

      format match {
        case "console" =>
          println(f"${"Column Name"}%-30s${opNames.mkString(",")}\n-----------------------------------------------")
        case "csv" =>
          println(f"${"Column Name"},${opNames.mkString(",")}\n-----------------------------------------------")
      }

      // get all column names for current table
      val names = (for {
        op <- mergedOps;
        tp <- op._3
      } yield tp._1).distinct

      // <column_name, List[values]>
      // initialize list to empty string		  
      val byColumns = names.foldLeft(mutable.Map[String, mutable.Seq[String]]()) { (map, colName) =>
        map(colName) = mutable.Seq[String]((1 to mergedOps.size) map (i => ""): _*)
        map
      }

      mergedOps.zipWithIndex.foreach { t =>
        val oneOp = t._1
        val idx = t._2
        val colValues = oneOp._3
        colValues.foreach { tp =>
          byColumns(tp._1)(idx) = tp._2
        }
      }

      // print it out
      byColumns.foreach(t => {
        if (format == "console")
          println(f"${t._1 + ","}%-30s${t._2.mkString(", ")}")
        else
          println(f"${t._1},${t._2.mkString(",")}")
      })
    })

  }
}

object Parser extends App {
  val usage = """
    Usage: scala -cp sqlparser.jar Parser --log-file [log file path] --pid [process id] --diff-only [true|false] --format [console|csv]
    
    For example:
		 scala -cp sqlparser.jar  Parser --log-file /Users/leo/Documents/rest_combo.log --pid AE025F8137224994 --diff-only true
  """
  if (args == null || args.length == 0) {
    println(usage)
    System.exit(1)
  }
  type OptionMap = Map[String, Any]
  
  def isSwitch(option: String): Boolean = option.startsWith("-")
  
  def loadOptions(options: OptionMap, arguments: List[String]): OptionMap = arguments match {
    case "--log-file" :: logFile :: tail =>
      if (isSwitch(logFile)){
        println("Invalid or missing log file path")
        System.exit(1)
      }
      loadOptions(options ++ Map("log-file" -> logFile), tail)
    case "--pid" :: pid :: tail => 
      if (isSwitch(pid)){
        println("Invalid or missing pid")
        System.exit(1)
      }
      loadOptions(options ++ Map("pid" -> pid), tail)  
    case "--diff-only" :: diffOnly :: tail => 
      if (isSwitch(diffOnly)){
        println("Invalid or missing diff-only flag")
        System.exit(1)
      }
      loadOptions(options ++ Map("diffOnly" -> diffOnly), tail)
    case "--format" :: format :: tail => 
      if (isSwitch(format) || (format != "console" && format != "csv")){
        println("Invalid or missing format, format should be either `console` or `csv`")
        System.exit(1)
      }
      loadOptions(options ++ Map("format" -> format), tail)
    case Nil => options
    case option :: tail => {
    	println("   Unknown option" + option) 
    	System.exit(1)
    	options
    }
  }
  val options = loadOptions(Map(), args.toList)
  // check required options
  if (!options.contains("log-file")){
    println("  --log-file is required")
    System.exit(1)
  }
  if (!options.contains("pid")){
    println("  --pid is required")
    System.exit(1)
  }

  // sample file: "/Users/leo/Documents/rest_combo.log"
  // sample pid: "AE025F8137224994"
  val parser = new MysqlUpsertParser(new File(options("log-file").toString), options("pid").toString , 
      if (options.contains("diffOnly")) options("diffOnly").toString.toLowerCase() == "true" else false,
      if (options.contains("format")) options("format").toString else "console" );
  parser.prettyPrint

}
