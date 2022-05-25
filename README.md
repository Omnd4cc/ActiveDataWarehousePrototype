# ActiveDataWarehousePrototype
Prototype of an active data warehouse analysis process based on Flink and Kafka with ECA rules

利用Apache Flink连接流特性、Kafka消息订阅机制，设计并实现了查询控制信息机制，以此为基础完成了基于ECA规则的主动数据仓库分析过程的实现；通过Flink有状态计算，提出算子数据存储优化方式，完成高度自定义化窗口查询实现，弥补Flink框架长时间高频滑动窗口实现方式的缺陷，实现窗口分析原语；通过设计主动查询触发及规则过期机制，实现主动查询分析原语。

## 项目概念

主动数据仓库旨在当满足一个或多个预设条件的事件发生时自动触发对应的操作。主动决策引擎作为主动数据仓库的核心的组成部分，负责检测特定条件事件的发生、触发对应操作、作出主动决策，从而指导决策制定者的业务开展。

本项目可以通过json命令集的形式对流数据进行查询。

## 项目来源

项目基于主动数据仓库的思想，从Flink欺诈检测博客出发，在Flink欺诈检测应用的基础上加入了规则控制机制，以此完成主动数据仓库的ECA自动概念。
https://flink.apache.org/news/2020/01/15/demo-fraud-detection.html
[Advanced Flink Application Patterns Vol.1: Case Study of a Fraud Detection System]: https://flink.apache.org/news/2020/01/15/demo-fraud-detection.html

本项目添加特性：

1.定时器实现滑动窗口。

2.聚合值出发主动规则。

3.主动规则唯一id生成。

4.主动规则生效，”节流“控制信息。

5.查询规则过期机制。

.......

## 项目架构设计

![struc.png](https://github.com/Omnd4cc/ActiveDataWarehousePrototype/blob/main/images/struc.png)

控制流包括描述查询分析的控制信息，通过Flink连接流特性与待分析事件流一起接入到动态键生成流程中。在动态键生成流程，根据查询规则集合对待分析事件进行键值型格式化处理及隐式复制，让每一条规则都有其对应的格式化数据。接下来查询流程根据查询规则对数据进行分析。控制信息可以动态控制上述格式化处理流程中的分区状态及查询流程中的查询状态。待分析数据经过分析流程得到分析结果如果满足主动规则触发条件，触发主动查询规则，最后对分析结果进行进一步处理如可视化。

工作流如下：

![dataflow.png](https://github.com/Omnd4cc/ActiveDataWarehousePrototype/blob/main/images/dataflow.png)

## 项目环境

### 1.JDK8

### 2.Kafka2.12-2.8.1

windows下常用命令：

1.windows启动：

.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

.\bin\windows\kafka-server-start.bat .\config\server.properties

2.windows创建生产者：
.\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic test
3.创建消费者：
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test --from-beginning

### 3.maven

## 项目入口

src/main/java/ActiveDataWarehouse/App.java

向Kafka的rules topic写入示例查询使用。

## 示例查询

```json
//当有车辆的平均速度超速时，新增查询对该车辆的实时行驶状态进行监控。
{
	"lastTime": -1,
	"windowMilliseconds": 10000,
	"frequencyMilliseconds": 0,
	"groupingKeyNames": ["carId"],
	"windowFilterRules": [],
	"alertRules": [{
		"windowFilterRules": [],
		"lastTime": 10000,
		"windowMilliseconds": 5000,
		"frequencyMilliseconds": 0,
		"groupingKeyNames": ["$carId"],
		"aggregatorFunctionType": "MAX",
		"limitOperatorType": "GREATER",
		"limit": 10,
		"queryState": "ACTIVE",
		"aggregateFieldName": "speed",
		"queryId": 2
	}],
	"aggregatorFunctionType": "AVG",
	"limitOperatorType": "GREATER",
	"limit": 120,
	"queryState": "ACTIVE",
	"aggregateFieldName": "speed",
	"queryId": 1
}
//去除回车压缩形式。
{"lastTime":-1,"windowMilliseconds":10000,"frequencyMilliseconds":0,"groupingKeyNames":["carId"],"windowFilterRules":[],"alertRules":[{"windowFilterRules":[],"lastTime":10000,"windowMilliseconds":5000,"frequencyMilliseconds":0,"groupingKeyNames":["$carId"],"aggregatorFunctionType":"MAX","limitOperatorType":"GREATER","limit":10,"queryState":"ACTIVE","aggregateFieldName":"speed","queryId":2}],"aggregatorFunctionType":"AVG","limitOperatorType":"GREATER","limit":120,"queryState":"ACTIVE","aggregateFieldName":"speed","queryId":1}

//查询某一个路段的平均速度以获取拥堵信息
{
	"lastTime": -1,
	"windowMilliseconds": 60000,
	"frequencyMilliseconds": 0,
	"windowFilterRules": [{
		"field": "lon",
		"value": "121.513011",
		"operator": ">"
	}, {
		"field": "lon",
		"value": "121.515430",
		"operator": "<"
	}, {
		"field": "lat",
		"value": "31.234928",
		"operator": "<"
	}, {
		"field": "lat",
		"value": "31.233456",
		"operator": ">"
	}],
	"aggregatorFunctionType": "AVG",
	"limitOperatorType": "GREATER",
	"limit": 120,
	"queryState": "ACTIVE",
	"aggregateFieldName": "speed",
	"queryId": 1,
	"activeTime": 123000
}
{"lastTime":-1,"windowMilliseconds":60000,"frequencyMilliseconds":0,"windowFilterRules":[{"field":"lon","value":"121.513011","operator":">"},{"field":"lon","value":"121.515430","operator":"<"},{"field":"lat","value":"31.234928","operator":"<"},{"field":"lat","value":"31.233456","operator":">"}],"aggregatorFunctionType":"AVG","limitOperatorType":"GREATER","limit":120,"queryState":"ACTIVE","aggregateFieldName":"speed","queryId":1,"activeTime":123000}
```

