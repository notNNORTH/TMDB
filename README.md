# TMDB

Totem Mobile Database at WHU.

本项目为2024年武汉大学计算机弘毅班"数据库系统实现"课程的大作业。

## 任务列表

### *Task 1: show table功能实现*

本项目数据库采用对象代理模型，其中对象及其代理之间的关系采用系统表来实现。

**任务目标**：补全文件`edu.whu.tmdb.util.DbOperation.java`中相关系统表展示功能函数

**任务描述**：实现以下指令执行

- `show ClassTable`：展示ClassTable中的"class name", "class id", "attribute name", "attribute id", "attribute type"
- `show DeputyTable`：展示DeputyTable中的"origin class id", "deputy class id"
- `show SwitchingTable`：展示SwitchingTable中的"origin class id", "origin attribute id", "origin attribute name", "deputy class id", "deputy attribute id", "deputy attribute name"
- `show BiPointerTable`：展示双向指针表中的"class id", "object id", "deputy id", "deputy object id"



### *Task 2: create deputy class功能实现*

**任务目标**：补全文件`edu.whu.tmdb.query.operations.impl.CreateDeputyClassImpl.java`中相关创建代理类的函数，实现代理类创建功能

**任务描述**：对象代理下，类和代理之间的关系通过系统表维护，因此在创建代理类时不仅要根据代理类型创建新的ClassTable并插入元组，还需要更新维护DeputyTable、SwitchingTable和BiPointerTable。具体地，补全以下函数：

- `createDeputyClass`：创建并插入新的classTableItem和switchingTableItem
- `createDeputyTableItem`：创建并插入新的deputyTableItem
- `createBiPointerTableItem`：创建并插入新的biPointerTableItem



### *Task 3: drop class功能实现*

**任务目标**：补全文件`edu.whu.tmdb.query.operations.impl.DropImpl`中相关函数，实现数据库drop class功能

**任务描述**：对象代理下，类和代理之间的关系通过系统表维护，因此在删除一个类时，需要同时删除类的对象、类相关的系统表，并递归删除以该类为源类的代理类。



### *Task 4: 带表达式的select实现*

**任务目标**：补全文件`edu.whu.tmdb.query.operations.impl.SelectImpl.java`中的`projectSelectExpression`函数，使系统支持带表达式的select查询

**任务描述**：
