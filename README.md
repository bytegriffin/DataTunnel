![Image text](https://raw.githubusercontent.com/bytegriffin/DataTunnel/master/logo.png)
===========================
  DataTunnel是一款基于异构环境的数据处理管道，试图用类SQL语句来解决异构存储/数据流等环境下的数据清洗、数据同步、数据统计等工作。

# 特性
* 任务化管理模式，可同时运行不同的任务
* 支持定时、立即运行模式
* 支持配置使用或API调用（自定义Reader或Writer接口）
* 配置的核心就是类SQL语言，降低使用成本，如果情况特别复杂可使用编程嵌入的方式
* 支持不同的中间件进行数据流通：Mysql、MongoDB、HBase、Kafka、RabbitMQ等，可横向拓展。

# 安装
请查看INSTALL.txt文件

# License
[Apache 2.0 License.](/LICENSE)
