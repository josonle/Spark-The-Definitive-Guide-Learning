# Chapter 7：聚合操作
聚合操作相关函数的性质是对每一个group而言是**多输入单输出**。Spark 有复杂和成熟的聚合操作，具有各种不同的使用方法和可能性。在 Spark 中也可以对任何数据类型进行聚合，包括复杂数据类型

Spark 允许我们创建以下group分组：
- 最简单的group分组仅仅是在select子句中通过聚合来汇总一个完整的DataFrame
- 通过`group by`来指定一或多个key并通过一或多个聚合函数来转换Columns的值
- 通过窗口函数来分组，功能上和`group by`类似，但输入聚合函数的rows和当前row有关（就是说窗口大小如何指定）
- 通过分组集（grouping sets），这是可以用来在不同层级上进行聚合操作。在SQL中可以直接使用分组集，而在DataFrame中可通过`rollup`和`cube`操作
> 我理解是这样的，原文是`Grouping sets are available as a primitive in SQL and via rollups and cubes in DataFrames.`
- 通过`rollup`（汇总）来指定一或多个key并通过一或多个聚合函数来转换Columns的值，这些列的值将按照层次结构进行汇总
- 通过`cube`（多维数据集）来指定一或多个key并通过一或多个聚合函数来转换Columns的值，不过这些列的值将在所有列组合中进行汇总

每个分组都返回一个 RelationalGroupedDataset，我们在其上指定聚合操作

其实上面讲的这么多都是SQL中哪些进行分组的聚合函数，rollup也是，cube也是

> 作者这里提了一个注意点：
> 一个需要考虑的重要事情是你需要一个多么精确的答案。 在对大数据进行计算时，要得到一个问题的精确答案可能相当昂贵，而且简单地要求一个近似到合理程度的精确度通常要便宜得多。 你会注意到我们在整本书中提到了一些近似函数，通常这是一个很好的机会来提高 Spark 作业的速度和执行，特别是对于交互式和特别分析
> 就是用近似值代替精确值

这次用的数据集是目录`retail-data/`下的数据，这里还指定了DataFrame的分区数并且cache持久化缓存了它
```scala
# 通过coalesce指定分区，因为coalesce只能减少分区数，而我初识读入这个df时只有4个分区（和机器有关），不过可以通过repartition来增加分区
# df.rdd.getNumPartitions 可以获取DataFrame的分区数
scala> val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("data/retail-data/all/*.csv").coalesce(5)
df: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [InvoiceNo: string, StockCode: string ... 6 more fields]
# 缓存
scala> df.cache
res11: df.type = [InvoiceNo: string, StockCode: string ... 6 more fields]
#创建临时表
scala> df.createOrReplaceTempView("dfTable")
scala> df.show(5)
+---------+---------+--------------------+--------+----------------+---------+----------+--------------+
|InvoiceNo|StockCode|         Description|Quantity|     InvoiceDate|UnitPrice|CustomerID|       Country|
+---------+---------+--------------------+--------+----------------+---------+----------+--------------+
|   548704|    84077|WORLD WAR 2 GLIDE...|     576|  4/3/2011 11:45|     0.21|     17381|United Kingdom|
|   538641|    21871| SAVE THE PLANET MUG|      12|12/13/2010 14:36|     1.25|     15640|United Kingdom|
|   546406|    22680|FRENCH BLUE METAL...|       1| 3/11/2011 16:21|     2.46|      null|United Kingdom|
|   538508|    22577|WOODEN HEART CHRI...|       6|12/12/2010 13:32|     0.85|     15998|United Kingdom|
|   543713|    22624|IVORY KITCHEN SCALES|       1| 2/11/2011 11:46|    16.63|      null|United Kingdom|
+---------+---------+--------------------+--------+----------------+---------+----------+--------------+

```
最基本也是最简单的作用于整个DataFrame上的聚合操作就是统计行数
```scala
scala> df.count
res15: Long = 541909
```
`count` 是一个Action算子，它不仅用来统计数据集的大小，这里另一个作用是执行df的持久化到内存的cache操作（不过我之前用了`show`，它也是Action算子，所以我这里之前就缓存过了）

>Now, this method is a bit of an outlier because it exists as a method (in this case) as opposed to a function and is eagerly evaluated instead of a lazy transformation. In the next section, we will see count used as a lazy function, as well.

## Group分组和聚合函数

### 聚合函数
除了可能出现的特殊情况之外，比如在DataFrames或通过`.stat`，Chapter 6中有相关描述，所有聚合操作都可作为一个函数。你可以在`org.apache.spark.sql.functions`的包下找到大多数聚合函数

> 作者提出一个注意点：
> 可用的SQL函数与我们可以在Scala和Python中导入的函数之间存在一些差距。这会更改每个版本，因此没有包含明确的差异函数列表。在本节中会介绍最常见的聚合操作

#### count 和 countDistinct
就是统计行数,后者是去掉重复值后的行数,用法同SQL中一样
```scala
# import org.apache.spark.sql.functions._

scala> df.select(count($"StockCode")).show
+----------------+
|count(StockCode)|
+----------------+
|          541909|
+----------------+
scala> df.select(countDistinct($"StockCode")).show
+-------------------------+                                                     
|count(DISTINCT StockCode)|
+-------------------------+
|                     4070|
+-------------------------+
```
当你要统计整个DataFrame的行数时，SQL 中可以用`count(*)/count(1)`，而DataFrame 中我测试是可以`df.select(count("*"))/df.select(count(lit(1)))` （因为count接受Column参数，只传个1进去会以为1是Column的名字，会报错）

还有就是 `countDistinct` 是一个DataFrame函数，在SQL不能这样用的，SQL 中是 `count(distinct xxx)`
#### approx_count_distinct
这个函数我也是头一次见，从字面意思看是近似无重复统计。书上也说是，如果你的数据集非常大，但准确的去重统计是无关紧要的，而某种精度的近似统计值也可以正常工作，你就可以使用`approx_count_distinct`函数：
```scala
scala> df.select(approx_count_distinct($"StockCode",0.1)).show
+--------------------------------+
|approx_count_distinct(StockCode)|
+--------------------------------+
|                            3364|
+--------------------------------+
```
`approx_count_distinct`的第二个参数（`rsd:Double`）是指定“允许的最大估计误差”（默认0.05）。在这种情况下，我们指定了一个相当大的错误，并因此得到一个相差甚远的答案，但比`countDistinct`完成得更快。如果使用更大的数据集，性能将提升更多
> Saprk 2.1.0之前还有一个`approxCountDistinct`函数，不过之后废弃了

#### first 和 last
这个和SQL中用法一样，返回组内Column列中第一个和最后一个value，当然first和last取决于窗口rows。其次这个函数的返回结果是不确定性的，取决于rows顺序（如果进行shuffle操作，rows的顺序就是不确定性的）

> `first(columnName: String, ignoreNulls: Boolean): Column`，ignoreNulls是否忽略null，即返回第一个非null值，但全为null时只能返回null
> `first(columnName: String): Column`，默认ignoreNulls为false
> `last(columnName: String): Column`，同上
> `last(columnName: String, ignoreNulls: Boolean): Column`
#### min 和 max
没啥说的
```scala
df.select(min("Quantity"), max("Quantity")).show()
+-------------+-------------+
|min(Quantity)|max(Quantity)|
+-------------+-------------+
|       -80995|        80995|
+-------------+-------------+
```
#### sum 和 sumDistinct
也没啥说的，求组内总和以及不含重复值的组内总和
```scala
scala> df.select(sum("Quantity"),sumDistinct("Quantity")).show
+-------------+----------------------+                                          
|sum(Quantity)|sum(DISTINCT Quantity)|
+-------------+----------------------+
|      5176450|                 29310|
+-------------+----------------------+
```
#### avg 和 mean
求平均值的，当然也可以用sum/count表示
```scala
scala> df.select(avg("Quantity"),mean("Quantity")).show
+----------------+----------------+
|   avg(Quantity)|   avg(Quantity)|
+----------------+----------------+
|9.55224954743324|9.55224954743324|
+----------------+----------------+
```
####  方差和标准差
方差和标准差是统计中另外两个评估量，书里这里写了一大串废话，不过提到了方差是平方差与均值的平均值，标准差是方差的平方根。Spark 也提供了样本标准差（the sample standard deviation）公式和总体标准差（the population standard deviation）公式，默认在使用方差或 stddev 函数时是用的样本标准差公式
```scala
# var 是方差，stddev是标准差，samp结尾的就是样本xxx，pop结尾就是总体xxx
df.select(var_pop("Quantity"),var_samp("Quantity"),stddev_pop("Quantity"),stddev_samp("Quantity")).show()
+-----------------+------------------+--------------------+---------------------+
|var_pop(Quantity)|var_samp(Quantity)|stddev_pop(Quantity)|stddev_samp(Quantity)|
+-----------------+------------------+--------------------+---------------------+
|47559.30364660923| 47559.39140929892|  218.08095663447835|   218.08115785023455|
+-----------------+------------------+--------------------+---------------------+
```
#### 偏度和峰度
这两是极值点的度量值，偏度度量的是数据中围绕平均值的不对称性，而峰度度量的是数据的尾部，Spark 中提供相关函数：

```scala
df.select(skewness("Quantity"), kurtosis("Quantity")).show()
+--------------------+------------------+
|  skewness(Quantity)|kurtosis(Quantity)|
+--------------------+------------------+
|-0.26407557610528376|119768.05495530753|
+--------------------+------------------+
```
#### 协方差和相关性
这两方法涉及两列之间联系
- `covar_samp(columnName1: String, columnName2: String): Column`，返回两列的样本协方差
- `covar_pop(columnName1: String, columnName2: String): Column`，返回两列的总体协方差
- `corr(columnName1: String, columnName2: String): Column`，返回两列间的皮尔逊相关系数

#### 复杂数据类型的聚合
Spark 不仅可以在数值上聚合操作，也可以在复杂类型上执行聚合。比如收集给定列的值列表list，或者只收集给定列的唯一值集合set
```scala
scala> df.agg(collect_set("country"),collect_list("Country")).show
+--------------------+---------------------+
|collect_set(country)|collect_list(Country)|
+--------------------+---------------------+
|[Portugal, Italy,...| [United Kingdom, ...|
+--------------------+---------------------+

```
### 在表达式中使用分组（Grouping with Expressions）
表达式中使用和使用agg函数都是一样用的
```scala
// in Scala
import org.apache.spark.sql.functions.count

df.groupBy("InvoiceNo").agg(
  count("Quantity").alias("quan"),
  expr("count(Quantity)")).show()
```
### 通过Maps映射使用分组（Grouping with Maps）

有时，可以更容易地将转换指定为一系列映射，其中Key为列，Value为希望执行的聚合函数(以字符串形式)。 如果你在行内指定多个列名，你也可以重复使用它们:
```scala
df.groupBy("InvoiceNo").agg("Quantity"->"avg","UnitPrice"->"max").show
df.groupBy("InvoiceNo").agg(expr("mean(Quantity)"),expr("max(UnitPrice)")).show

# sql
spark.sql("select mean(Quantity),max(UnitPrice) from dfTable group by InvoiceNo").show
```

## 窗口函数

## 分组集（Grouping Sets）

## 用户自定义聚合函数（UDAF）