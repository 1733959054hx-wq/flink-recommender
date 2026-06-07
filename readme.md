# 项目启动指南

## 一、文件介绍

### 1.1 `initialization` 文件夹
- **`recommend_db_20260527.sql`**：最新数据库脚本。
- **`data_import_mysql_UF.py`** 和 **`data_import_mysql_UB.py`**：用于导入历史数据。
- **`大项目启动流程.txt`**：作者自用流程，仅供参考。

### 1.2 `flink` 文件夹
- 右键 `pom.xml` 创建 Maven 项目，解决可能出现的依赖问题后，打包成 `jar` 包。

### 1.3 `web` 文件夹
- 常规前后端项目结构。
- **注意**：需在系统环境变量中配置 `python` 变量，否则无法运行 Python 文件。

## 二、启动流程

1. **数据库初始化**  
   运行 `recommend_db_20260527.sql` 脚本。

2. **历史数据导入**  
   依次运行 `data_import_mysql_UF.py` 和 `data_import_mysql_UB.py`。

3. **启动 Flink 环境**  
   在虚拟机中启动 Flink、ZooKeeper 和 Kafka，并打开 Flink Web 页面。

4. **提交 Flink 任务**  
   将打包好的 `jar` 包上传至 Flink，并提交（Submit）任务。

5. **启动前后端服务**  
   启动 `web` 目录下的前后端项目。

6. **准备数据模拟终端**  
   在 Shell 中打开三个 `node1` 页面（终端窗口）。

7. **分配终端用途**
    - 第一个页面：用于监听（详见 `大项目启动流程.txt`）。
    - 第二个页面：用于运行 `data_simulator_kafka.py`。
    - 第三个页面：备用。

8. **启动数据模拟器**  
   运行 `data_simulator_kafka.py` 后即可在页面上观察到效果。

## 三、注意事项

- **MySQL 与 Redis 配置**：  
  主要需配置 MySQL，虽然配置可能因环境而异，但通常差异不大。  
  Redis 默认为无密码，如有密码请自行修改配置。
- **log文件需要删除**：  
  evaluating文件夹内的log文件在运行久后会非常大