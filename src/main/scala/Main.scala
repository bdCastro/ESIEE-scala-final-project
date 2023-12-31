import java.time.LocalDateTime
import scala.collection.mutable.TreeMap

@main def hello: Unit =
  val store = new MemoryStore()
  store.put("PEOPLE", "")
  store.put("PEOPLE#id", ColumnType.IntType.name)
  store.put("PEOPLE#name", ColumnType.StringType.name)
  store.put("PEOPLE#age", ColumnType.IntType.name)
  store.put("PEOPLE#id#1", "1")
  store.put("PEOPLE#name#1", "John")
  store.put("PEOPLE#age#1", "42")
  store.put("PEOPLE#id#2", "2")
  store.put("PEOPLE#name#2", "Jane")
  store.put("PEOPLE#age#2", "24")
  store.put("PEOPLE#id#3", "3")
  store.put("PEOPLE#name#3", "Jack")
  store.put("PEOPLE#age#3", "32")
  store.put("PEOPLE#id#4", "4")
  store.put("PEOPLE#name#4", "Jill")
  store.put("PEOPLE#age#4", "19")
  store.put("PEOPLE#id#5", "5")
  store.put("PEOPLE#name#5", "James")
  store.put("PEOPLE#age#5", "69")

  val tableScan = TableScan("PEOPLE",
    Option(Filter(FilterExpression.LessOrEqual(ColumnExpression.Column("age"), ColumnExpression.LitInt(40)),
      None,
      Option(Range(0, 3,
        Option(Projection(ColumnExpression.Column("id"),
          List(ColumnExpression.Column("name")),
          None
        )
      ))
    ))
  ))

  val executor = SQLExecutor(tableScan, store)
  val result = executor.execute()
  printResult(result)

def printResult(result: Either[DatabaseError, List[Record]]): Unit =
  result match
    case Left(error) => println(error)
    case Right(records) => records.foreach(r =>
      println(s"key: ${r.key.split("#").last}, value: ${r.value}")
      )

class SQLExecutor(tableScan: TableScan, store: Store) {
  def execute(): Either[DatabaseError, List[Record]] = {
    // chack that the table exists
    store.get(tableScan.tableName) match
      case Left(error) => return Left(DatabaseError.TableNotFound(tableScan.tableName))
      case Right(_) => ()

    // check if the column exists
    tableScan.next match
      case Some(filter: Filter) =>
        val next = filter.next match
          case Some(range: Range) =>
            val next = range.next match
              case Some(projection: Projection) => projection.next
              val columns = range.next match
                case Some(projection: Projection) => projection.column :: projection.otherColumns
                  val columns = projection.column :: projection.otherColumns
                  columns.foreach {
                    case ColumnExpression.Column(columnName) =>
                      store.get(s"${tableScan.tableName}#$columnName") match
                        case Left(error) => return Left(DatabaseError.ColumnNotFound(columnName))
                        case Right(_) => ()
                        case _ => ()
                      }
                    case _ => return Left(DatabaseError.InvalidOperation(range))
                case _ => return Left(DatabaseError.InvalidOperation(range))
          case _ => return Left(DatabaseError.InvalidOperation(filter))
      case _ => return Left(DatabaseError.InvalidOperation(tableScan))

      // // retrive the data
      // val data = store.scan() match
      //   case Left(error) => return Left(DatabaseError.InvalidOperation(tableScan))
      //   case Right(data) => data

      // filter the data
      var filteredData = tableScan.next match
        case Some(filter: Filter) =>
          val filterExpression = filter.filter
          filterExpression match
            case FilterExpression.Equal(ColumnExpression.Column(columnName), ColumnExpression.LitInt(value)) =>
              store.getPrefix(s"${tableScan.tableName}#${columnName}#") match
                case Left(error) => return Left(DatabaseError.InvalidOperation(filter))
                case Right(data) => data.map(r => if r.value.toInt == value then r else Record("0", "0"))
            case FilterExpression.NotEqual(ColumnExpression.Column(columnName), ColumnExpression.LitInt(value)) =>
              store.getPrefix(s"${tableScan.tableName}#${columnName}#") match
                case Left(error) => return Left(DatabaseError.InvalidOperation(filter))
                case Right(data) => data.map(r => if r.value.toInt != value then r else Record("0", "0"))
            case FilterExpression.Greater(ColumnExpression.Column(columnName), ColumnExpression.LitInt(value)) =>
              store.getPrefix(s"${tableScan.tableName}#${columnName}#") match
                case Left(error) => return Left(DatabaseError.InvalidOperation(filter))
                case Right(data) => data.map(r => if r.value.toInt > value then r else Record("0", "0"))
            case FilterExpression.GreaterOrEqual(ColumnExpression.Column(columnName), ColumnExpression.LitInt(value)) =>
              store.getPrefix(s"${tableScan.tableName}#${columnName}#") match
                case Left(error) => return Left(DatabaseError.InvalidOperation(filter))
                case Right(data) => data.map(r => if r.value.toInt >= value then r else Record("0", "0"))
            case FilterExpression.Less(ColumnExpression.Column(columnName), ColumnExpression.LitInt(value)) =>
              store.getPrefix(s"${tableScan.tableName}#${columnName}#") match
                case Left(error) => return Left(DatabaseError.InvalidOperation(filter))
                case Right(data) => data.map(r => if r.value.toInt < value then r else Record("0", "0"))
            case FilterExpression.LessOrEqual(ColumnExpression.Column(columnName), ColumnExpression.LitInt(value)) =>
              store.getPrefix(s"${tableScan.tableName}#${columnName}#") match
                case Left(error) => return Left(DatabaseError.InvalidOperation(filter))
                case Right(data) => data.map(r => if r.value.toInt <= value then r else Record("0", "0"))


      // detete the Nones
      filteredData = filteredData.filter(r => r != Record("0", "0"))

      // limit the data
      var start = 0
      var count = 0
      
      tableScan.next match
        case Some(filter: Filter) =>
          val next = filter.next match
            case Some(range: Range) => range.start
              start = range.start
              count = range.count


      val limitedData = filteredData.toList.slice(start, start + count)
      Right(limitedData.toList)
  }
}

// define type aliases.
// All keys and values are of type String
type Key = String
type Value = String

case class Record(
    key: Key,
    value: Value,
		/** timestamp the entry has been recorded in the store */
    // timestamp: LocalDateTime,
		/** indicate if the entry has been deleted */
    // deleted: Boolean
)

enum StoreError:
  case KeyNotFound(key: Key)

enum DatabaseError:
  case TableAlreadyExists(tableName: TableName)
  case TableNotFound(tableName: TableName)
  case ColumnNotFound(columnName: ColumnName)
  case ColumnAlreadyExists(columnName: ColumnName)
  case InvalidType(columnName: ColumnName, expected: ColumnType, actual: ColumnType)
  case InvalidValue(columnName: ColumnName, expected: ColumnType, actual: RowValue)
  case InvalidOperation(operation: Operation)
  case InvalidExpression(expression: ColumnExpression)
  case InvalidFilter(filter: FilterExpression)
  case InvalidRange(start: Int, count: Int)

trait Store:
  /** get the value associated to key */
  def get(key: Key): Either[StoreError, Value]
  /** add a key-value entry in the store */
  def put(key: Key, value: Value): Either[StoreError, Unit]
	/** remove a key from the store */
  def delete(key: Key): Either[StoreError, Unit]
	/** get an iterator over all entries from the store */
  def scan(): Either[StoreError, Iterator[Record]]
	/** get an iterator over entries starting from a given key */
  def getFrom(key: Key): Either[StoreError, Iterator[Record]]
	/** get an iterator over the entries which key starts with the same prefix */
  def getPrefix(prefix: String): Either[StoreError, Iterator[Record]]


class MemoryStore extends Store:
  // note: TreeMap is a kind of Map where data are sorted according to the key
  private val data: TreeMap[Key, Value] = TreeMap.empty

  override def get(key: Key): Either[StoreError, Value] =
    data.get(key).toRight(StoreError.KeyNotFound(key))

  override def put(key: Key, value: Value): Either[StoreError, Unit] = Right {
    data.update(key, value); ()
  }

  override def delete(key: Key): Either[StoreError, Unit] =
    data
      .get(key)
      .toRight(StoreError.KeyNotFound(key))
      .map(r => data.update(key, ""))

  // TODO: implement the retrieval of all entries
  override def scan(): Either[StoreError, Iterator[Record]] = Right {
    data.iterator.map((k, v) => Record(k, v))
  }

  // TODO: implement the retrieval of all entries
  override def getFrom(key: Key): Either[StoreError, Iterator[Record]] = Right {
    data.iteratorFrom(key).map((k, v) => Record(k, v))
  }

  // TODO: implement the retrieval of all entries
  override def getPrefix(prefix: String): Either[StoreError, Iterator[Record]] =
    Right { data.filter((k, _) => k.startsWith(prefix)).iterator.map((k, v) => Record(k, v)) }

// trait Database:
// 	def openOrCreate(tableName: TableName, schema: Schema): Either[DatabaseError, Table]
// 	def drop(tableName: TableName): Either[DatabaseError, Unit]

// trait Table(tableName: TableName, schema: Schema, store: Store):
// 	def insert(values: Map[ColumnName, Any]): Either[DatabaseError, Unit]
// 	def execute(executionPlan: ExecutionPlan): Either[DatabaseError, Result]

type TableName = String
type ColumnName = String
type RowValue = Any // value of any type

// SCHEMA

case class Schema(key: ColumnDeclaration, columns: List[ColumnDeclaration])

case class ColumnDeclaration(name: ColumnName, colType: ColumnType)

enum ColumnType(val name: String):
  case IntType extends ColumnType("INTEGER")
  case StringType extends ColumnType("STRING")
  case BooleanType extends ColumnType("BOOLEAN")
  case TimestampType extends ColumnType("TIMESTAMP")

object ColumnType:
  val byName = ColumnType.values.map(t => (t.name, t)).toMap
  def from(name: String): Option[ColumnType] = byName.get(name)

// RESULT

/** Result of an execution plan */
case class Result(
  /** available columns in the result */
	columns: List[String],
	/** rows of the result with their respective values */
	rows: List[Record]
)	

case class Row(data: Map[ColumnName, RowValue]):
	def get(columnName: ColumnName): Option[RowValue] = data.get(columnName)

  /** The operation to execute by a query engine.
  *
  * A query engine is a component responsible in managing the data storage in a
  * view to satisfy a SQL query. To do so, your request is Converted into an
  * execcution plan. An execution plan an organisation of a set of low level
  * operations to execute, in order to fulfill your request.
  *
  * To represent "SELECT * FROM my_table", you will write:
  * {{{
  * ExecutionPlan(TableScan("my_table", Projection(All, Nil, End)))
  * }}}
  *
  * "SELECT * FROM my_table WHERE count > 10"
  * {{{
  * ExecutionPlan(
  *   TableScan("my_table",
  *     Projection(All, Nil,
  *       Filter(Greater(Column("count"), LitInt(10)), Nil, End)
  *     )
  *   )
  * )
  * }}}
  *
  * "SELECT * FROM my_table RANGE 0, 10"
  * {{{
  * ExecutionPlan(
  *   TableScan("my_table",
  *     Projection(All, Nil,
  *       Range(0, 10, End)
  *     )
  *   )
  * )
  * }}}
  *
  * @param firstOperation
  *   the first operation to execute
  */

case class ExecutionPlan(firstOperation: Operation)

/** End of an execution plan. */
case object End extends Operation(None)

/** This is the representation of an operation to be executed by the request
  * engine.
  *
  * An Operation is an element of an execution plan. It can be a "table scan"
  * (ie. to read the rows in a table), a "projection" (ie. extracting and
  * transforming only the necessary columns), a "filter" (ie. removing some rows
  * according to criteria from the WHERE clause)...
  *
  * @param next
  *   the operation to execute after this one
  */
trait Operation(next: Option[Operation])

/** Read all the content of a table.
  *
  * @param tableName
  *   name of the table.
  * @param next
  */
case class TableScan(
    tableName: String,
    next: Option[Operation]
) extends Operation(next)

/** Represent a column expression.
  */
enum ColumnExpression:
  /** Represent a column.
    *
    * @param name
    *   name of the column
    */
  case Column(name: String)

  /** integer value */
  case LitInt(value: Int)

  /** double value */
  case LitDouble(value: Double)

  /** string value */
  case LitString(value: String)

  /** Represent a function call
    *
    * @param function
    *   name of the function
    * @param col
    *   column sub-expression
    */
  case FunctionCall(function: String, col: ColumnExpression)

/** represent all available column (like in "SELECT *") */
case object All

/** Apply a projection and transformations to a rows.
  *
  * @param column
  *   first column expression to project on
  * @param otherColumns
  *   following column expression to project on if any
  * @param next
  */
case class Projection(
    column: ColumnExpression | All.type,
    otherColumns: List[ColumnExpression | All.type],
    next: Option[Operation]
) extends Operation(next)

/** Represent an expression in a filter (ie in WHERE clause). It can be
  */
enum FilterExpression:
  // c1 = c2
  case Equal(c1: ColumnExpression, c2: ColumnExpression)
  // c1 != c2
  case NotEqual(c1: ColumnExpression, c2: ColumnExpression)
  // c1 > c2
  case Greater(c1: ColumnExpression, c2: ColumnExpression)
  // c1 >= c2
  case GreaterOrEqual(c1: ColumnExpression, c2: ColumnExpression)
  // c1 < c2
  case Less(c1: ColumnExpression, c2: ColumnExpression)
  // c1 <= c2
  case LessOrEqual(c1: ColumnExpression, c2: ColumnExpression)

/** Filter rows according to criterion.
  *
  * All filter expressions are implicitly bounded by AND.
  *
  * @param filter
  *   first filter expression
  * @param filters
  *   following filter expression, if any
  * @param next
  */
case class Filter(
    filter: FilterExpression,
    filters: Option[List[FilterExpression]],
    next: Option[Operation]
) extends Operation(next)

/** Limit the number of lines to output.
  *
  * @param start
  *   line index to start with
  * @param count
  *   number of lines to output
  * @param next
  */
case class Range(
    start: Int,
    count: Int,
    next: Option[Operation]
) extends Operation(next)