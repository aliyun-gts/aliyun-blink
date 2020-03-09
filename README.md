# aliyun-blink
阿里云 Blink DataStream程序基础依赖和Example

## 依赖

在POM文件中增加了当前3.4.0支持的Connector的依赖，根据实际需求选择。
1. Kafka
1. RabbitMQ
1. ElasticSearch
1. JDBC
1. HIVE
1. HBase
1. nifi
1. Cassandra

默认Connector和Flink自身的依赖Scope都是Provided，由真实的ExecutionEnvironment提供
如果需要调试代码，通过本地模式运行，需要将Provided注释掉，默认为Compile

## 例子
### WordCount

