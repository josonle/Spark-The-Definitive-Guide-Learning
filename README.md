# Spark-The-Definitive-Guide-Learning
《Spark: The Definitive Guide Big Data Processing Made Simple》学习记录

同步更新在掘金：[《Spark 权威指南学习计划》](https://juejin.im/post/5cd3dc06e51d456e2d69a83e)

## 前言
本书出自OReilly的[《Spark: The Definitive Guide Big Data Processing Made Simple》](http://shop.oreilly.com/product/0636920034957.do)，由Matei Zaharia, Bill Chambers两位大佬所写，是2018年2月的第一版（我也不清楚有没有最新版，搜也没搜到第二版）
![Spark: The Definitive Guide](https://images-na.ssl-images-amazon.com/images/I/51z7TzI-Y3L._SX379_BO1,204,203,200_.jpg)
参考本书主页介绍，着眼于Spark 2.0的改进，探索Spark结构化API的基本操作和常用功能，以及用于构建端到端流应用程序的新型高级API Structured Streaming。学习监控，调优和调试Spark的基础知识，并探索机器学习技术和场景，以便使用Spark的可扩展机器学习库MLlib。
- 轻松了解大数据和Spark
- 通过工作示例了解DataFrames，SQL和Datasets-Spark的核心API
- 深入了解Spark的低级API，RDD以及SQL和DataFrame的执行
- 了解Spark如何在群集上运行
- 调试，监视和调整Spark集群和应用程序
- 了解结构流，Spark的流处理引擎的强大功能
- 了解如何将MLlib应用于各种问题，包括分类或推荐

OReilly它家的书都是把代码和案例放在github上的，这本书也不例外，见此[databricks/Spark-The-Definitive-Guid](https://github.com/databricks/Spark-The-Definitive-Guide)

实际上，这并非我初学Spark了，之前也有所涉猎，但想着能够深入学习，便计划写下文章加深自己理解，以及分享知识。

本书并非是对原作的翻译，好像目前国内也没有出版社翻译了这本书，仅仅是叙述自己读书的心得、想法，并结合自己之前所学加以新内容。

## 学习记录
> ### 计划
> - 计划第1、2、3章
>   - 计划作废，和大多书一样前面内容都是总览性内容，实际性的东西也很杂，所以先放置待后期补上吧
> - 计划4、5、6章吧，（～5.26）
> - 计划7、8、9、10章 （～6.14）

书籍分为以下七大部分：
- 大数据和Spark概述
  - Chapter 1 to 2：了解Apache Spark
  - Chapter 3：了解Spark的工具集
- 结构化API——DataFrames, SQL, and Datasets
  - [Chapter 4：结构化API预览](https://juejin.im/post/5ce8de846fb9a07ed136b120)
  - [Chapter 5：基本结构化API操作](https://juejin.im/post/5ce7e98a6fb9a07ea712ebd2)
  - [Chapter 6：处理不同类型的数据](https://juejin.im/post/5cf9bba8f265da1bd146490f)
  - Chapter 7：聚合操作
  - Chapter 8：join 连接操作
  - Chapter 9：数据源
  - Chapter 10：Spark SQL 用 SQL 来操作
- 底层API
  - Chapter 12：弹性分布式数据集（RDDs）
  - Chapter 13：高级的 RDDs
  - Chapter 14：分布式共享变量
- 生产上的应用
  - Chapter 15：Spark 如何在集群上运行
  - Chapter 16：开发 Spark 应用程序
  - Chapter 17：部署 Spark
  - Chapter 18：监控和调试
  - Chapter 19：性能调优
- Streaming流
  - Chapter 20：Stream 流处理基础
  - Chapter 21：结构化Streaming流的基础
  - Chapter 22：事件时间（Event-time）和状态处理
  - Chapter 23：生产中的结构化流处理
- 高级数据分析和机器学习
  - Chapter 24：高级分析和机器学习预览
  - Chapter 25：预处理和特征工程
  - Chapter 26：分类
  - Chapter 27：回归
  - Chapter 28：Recommendation 推荐
  - Chapter 29：非监督性学习
  - Chapter 30：图分析
  - Chapter 31：深度学习
- Spark 生态
  - Chapter 32：语言细节: Python (PySpark)和 r (SparkR 和 sparklyr)
  - Chapter 33：生态和社区

***
### foot
收录于此：[josonle/Spark-The-Definitive-Guide-Learning](https://github.com/josonle/Spark-The-Definitive-Guide-Learning)

>更多推荐：
[Coding Now](https://link.juejin.im/?target=https%3A%2F%2Fgithub.com%2Fjosonle%2FCoding-Now)
>
>学习记录的一些笔记，以及所看得一些电子书eBooks、视频资源和平常收纳的一些自己认为比较好的博客、网站、工具。涉及大数据几大组件、Python机器学习和数据分析、Linux、操作系统、算法、网络等