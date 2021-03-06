# 基于Apache Flink的轻量级批流融合通用TableSource

## 项目简述

随着数据技术的不断演进，"流批一体"这一概念正不断从原型设计向着落地进发。Apache Flink自开始起就将"流批一体"作为顶层成员进行考虑。而Source作为数据的入口，是我们实现流批一致性的第一个重要部件。
本项目试图提出一种轻量级的Source批流融合算法，并基于Flink进行实现。
此外，设计一套全新的Connector，可以在**尽可能复用已有的Sql Connector组件下**，以非常小的实现代价，**组合任意两种Source**，进行数据融合，生成RowKind语义完备，不重不漏的数据流。最后，我们将尝试进行多Source表数据一致性相关的探索，试图完成一套完整的机制和对应的实现。

[项目Repo](https://github.com/shouweikun/flink-hackathon-2021)

## 背景

相信大家对Table和Stream的统一性多少都一定的了解。Table的Changelog天然就是Stream，而Changelog在时间上的"积分"得到了Table。回到我们实际生产中的数据系统中，对于一张表，往往历史数据部分会被放在一套存储系统上，而较新的数据会以Changelog的形式存储在对应的消息系统（Kafka/Pulsar）中。传统的处理思路往往利用批处理进行全量数据与增量数据的整合。此外，对于该表来说，批处理和流处理的计算逻辑往往有着很大不同。对于流处理，我们往往要将Changelog语义相关的逻辑进行显性的处理，但是即便如此，流处理也不能完全胜任批处理的所能完成的全部逻辑或功能。我们需要这样一套Source系统，可以全量 + 增量的数据以流的形式完美整合。这样不论从计算逻辑上，还是数据本身，都能实现流批统一。从而在数据计算层梁助力相当多的业务场景，譬如数据湖，实时物化视图等

## 目标

 - 完成数据整合算法的设计与及基于Flink的通用实现

 - 设计一套可以最大复用已有Flink SQL Connector的机制，组合任何Source完成整合

 - （Optional）最后希望可以进行一致性相关的探索 

## 实施方案

 - 设计轻量级数据整合算法
 - 基于Flink对该算法进行实现
 - 设计一套DynamicTableSource及相关组件，尽可能复用已有Flink SQL Connector
 - （Optional） 探索多表读取时一致性相关机制

## 成员介绍
<br>

[刘首维](https://github.com/shouweikun) （天池ID：首维君1）code for fun~
<br>

[张茄子](https://www.zhihu.com/people/chase-zh) (天池ID：chasezh) 




