> 该包实现社区发现的第二个算法-PCPM算法

### 算法核心



### 核心代码

- `Executor.java`

  算法执行器

- `FScoreIndex.java`

  计算性能评价指标的程序

- `HandleData.java`

  处理算法输出结果的程序

- `Clique_Mapper.java`

  寻找极大完全子图的map函数实现

- `Clique_Reducer.java`

  寻找极大完全子图的reduce函数实现

- `KMerge_Mapper.java`

  寻找K派系社区的map函数实现

- `KMerge_Reducer.java`

  寻找K派系社区的reduce函数实现

- `KAllCombined_Mapper.java`

  寻找所有K派系连接社区的map函数实现

- `KAllCombined_Reducer.java`

  寻找所有K派系连接社区的reduce函数实现

### 其他代码

- `package-info.java`

  该包说明