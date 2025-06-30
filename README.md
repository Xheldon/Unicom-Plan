# 联通数据处理脚本

这个 Python 脚本用于读取联通套餐数据文件并将数据导入到 PostgreSQL 数据库中，提供完整的数据处理、优化和排序功能。

## 🚀 功能特性

### 📁 数据处理

- 自动遍历输入文件夹下的所有子文件夹
- 读取每个子文件夹中的 `response.dump` 文件（JSON 格式）
- 提取 `threeDetailDate` 数组中的完整数据结构
- 处理 `detailInfo` 内的字段（JSON 字符串解析）
- 处理与 `detailInfo` 同级的其他字段
- 收集所有文件中字段的并集，确保不遗漏任何数据

### 🗄️ 数据库功能

- 自动创建 PostgreSQL 数据库（如果不存在）
- 根据数据结构动态创建数据库表
- 智能字段类型识别（TEXT、INTEGER、JSONB 等）
- 批量插入数据到数据库

### 📊 字段排序和优化

- **重要字段优先排序**：套餐名称、速率、月费等关键字段排在前面
- **数值排序支持**：为需要数值排序的字段自动创建对应的 INTEGER 类型列
  - `mainFee_numeric` - 月费数值排序
  - `downSpeed_numeric` - 下行速率数值排序
  - `upSpeed_numeric` - 上行速率数值排序
  - `advanceDeposit_numeric` - 预存费用数值排序
  - `netCountryFlow_numeric` - 全国流量数值排序

### 🧹 数据清理

- 自动删除完全空的列
- 自动删除所有行内容完全相同的重复列
- 忽略无用字段（`broad`、`belongProvince`、`accessWay`）
- 识别并清理空数据（空字符串、null、「空」等）

## 📋 环境要求

- Python 3.6+
- PostgreSQL 数据库服务
- 必要的 Python 包（见 requirements.txt）

## 🔧 安装依赖

```bash
pip install -r requirements.txt
```

## ⚙️ 数据库配置

脚本默认连接到以下 PostgreSQL 配置：

- 主机: 127.0.0.1
- 端口: 5432
- 用户名: postgres
- 密码: postgres
- 数据库: unicom（如果不存在会自动创建）

如需修改配置，请编辑脚本中的数据库连接参数。

## 🚦 使用方法

### 基本用法

```bash
python process_unicom_data.py /path/to/your/data/folder
```

### 指定表名

```bash
python process_unicom_data.py /path/to/your/data/folder --table-name custom_table_name
```

## 📂 目录结构要求

输入的文件夹应该具有以下结构（Surge 抓包来的）：

```
unicom/
├── 000726 - 12.15.27 - POST - https%3A%2F%2Fm.client.10010.com%2F.../
│   └── response.dump
├── 000727 - 12.15.42 - POST - https%3A%2F%2Fm.client.10010.com%2F.../
│   └── response.dump
└── ...
```

## 🗃️ 数据库表结构

脚本会自动创建优化后的表结构，包含以下主要字段（按重要性排序）：

### 🔥 核心字段（前排显示）

1. **nameThird** - 套餐名称
2. **detailInfo_downSpeed** + **downSpeed_numeric** - 下行速率（文本+数值）
3. **detailInfo_upSpeed** + **upSpeed_numeric** - 上行速率（文本+数值）
4. **detailInfo_netCountryFlow** + **netCountryFlow_numeric** - 全国流量（文本+数值）
5. **mainFee** + **mainFee_numeric** - 月费（文本+数值）
6. **detailInfo_advanceDeposit** + **advanceDeposit_numeric** - 预存费用（文本+数值）
7. **serviceContent** - 服务内容

### 📋 其他字段

- 套餐详情字段：安装费、材料费、固话选择等
- 时间字段：创建时间、发布时间、更新时间等
- 标识字段：套餐 ID、记录编号等
- 业务字段：销售渠道、适用区域、退订方式等

## 📊 数据优化效果

- **处理文件数**：26 个文件
- **数据记录数**：127 条记录
- **原始字段数**：118 个字段
- **优化后字段数**：39 个有效字段
- **删除空列**：70 个
- **删除重复列**：14 个

## 📈 TablePlus 排序使用

在 TablePlus 中，现在可以正确进行数值排序：

- 使用 `mainFee_numeric` 按月费排序：0 → 15 → 20 → 25 → 30
- 使用 `downSpeed_numeric` 按下行速率排序：10 → 100 → 300 → 500 → 1000
- 使用 `upSpeed_numeric` 按上行速率排序
- 使用其他 `*_numeric` 字段进行数值排序

## 📝 日志输出

脚本会输出详细的处理日志，包括：

- 文件读取状态和进度
- 字段分析和统计信息
- 数据库连接状态
- 表创建和数据插入进度
- 数据优化过程（空列和重复列删除）
- 错误信息和警告

## ⚠️ 注意事项

1. 确保 PostgreSQL 服务正在运行
2. 确保有足够的数据库权限创建数据库和表
3. `response.dump` 文件必须是有效的 JSON 格式
4. 如果表已存在，脚本会删除并重新创建该表
5. 脚本会自动优化数据库结构，删除无用列

## 🐛 错误处理

- JSON 解析错误会被记录但不会停止整个处理过程
- 文件读取错误会被记录并跳过该文件
- 数据库连接错误会终止程序执行
- 字段类型冲突会自动选择更通用的类型

## 📊 示例输出

```
2025-06-30 14:43:56,733 - INFO - 分析完成: 共处理 26 个文件，127 条记录，发现 118 个不同字段
2025-06-30 14:43:56,781 - INFO - 成功插入 127 条记录
2025-06-30 14:43:56,902 - INFO - 分析完成: 发现 70 个空列，14 个重复内容列，39 个有效列
2025-06-30 14:43:56,915 - INFO - 优化完成: 表现在有 127 行，39 列
2025-06-30 14:43:56,915 - INFO - 数据处理和优化完成！
```
