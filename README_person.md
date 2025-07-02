# 联通个人套餐数据处理工具

这是一个专门处理联通个人套餐数据的 Python 脚本，用于将 Surge 抓包得到的 JSON 数据导入 PostgreSQL 数据库。

## 🚀 功能特点

### 📊 数据处理能力

- 读取每个子文件夹中的 `response.dump` 文件（JSON 格式）
- 提取 `tariffDetailInfoList` 数组中的完整数据结构
- 处理 `packageinfo` 内的字段（只处理原始值：字符串、数字、布尔值）
- 收集所有文件中字段的并集，确保不遗漏任何数据

### 🗄️ 数据库功能

- 自动创建 PostgreSQL 数据库（如果不存在）
- 根据数据结构动态创建数据库表
- 智能字段类型识别（TEXT、INTEGER、JSONB 等）
- 批量插入数据到数据库

### 📊 字段排序和优化

- **重要字段优先排序**：套餐名称、月费、服务内容、适用区域等关键字段排在前面
- **自动优化**：删除所有行都没有数据的空列和内容完全相同的重复列

## 📂 目录结构要求

输入的文件夹应该具有以下结构（Surge 抓包来的）：

```
unicom_person/
├── 002083 - 15.18.15 - POST - https%3A%2F%2Fm.client.10010.com%2F.../
│   └── response.dump
├── 002084 - 15.18.15 - POST - https%3A%2F%2Fm.client.10010.com%2F.../
│   └── response.dump
└── ...
```

## 📋 环境要求

- Python 3.6+
- PostgreSQL 数据库
- 必要的 Python 包（见 requirements.txt）

## 🔧 安装依赖

```bash
pip install -r requirements.txt
```

## 🏃 使用方法

### 基本用法

```bash
python process_unicom_person_data.py unicom_person/
```

### 指定表名

```bash
python process_unicom_person_data.py unicom_person/ --table-name my_person_data
```

### 参数说明

- `folder_path`: 包含 response.dump 文件的文件夹路径（必需）
- `--table-name`: 数据库表名（可选，默认：unicom_person_data）

## ⚙️ 数据库配置

脚本默认使用以下数据库配置：

```python
DB_CONFIG = {
    'host': '127.0.0.1',
    'port': 5432,
    'user': 'postgres',
    'password': 'postgres',
    'database': 'unicom_person'
}
```

如需修改，请直接编辑脚本中的 `DB_CONFIG` 字典。

## 📊 数据结构说明

### 输入数据格式

脚本处理的 JSON 数据应包含 `tariffDetailInfoList` 字段：

```json
{
  "tariffDetailInfoList": [
    {
      "taocanName": "套餐名称",
      "packageinfo": {
        "pagePackName": "套餐名称",
        "pageMonthfee": "月费",
        "serviceContent": "服务内容",
        "suitArea": "适用区域",
        "其他字段": "值"
      }
    }
  ]
}
```

### 字段优先级

脚本会按以下顺序排列字段：

1. **pagePackName** - 套餐名称（第一列）
2. **pageMonthfee** - 月费（第二列）
3. **serviceContent** - 服务内容（第三列）
4. **suitArea** - 适用区域（第四列）
5. 其他字段按字母顺序排列

### 处理的字段类型

- **原始值字段**：字符串、数字、布尔值、null
- **忽略的字段**：复杂对象（dict、list）会被跳过

## 🔍 处理流程

1. **读取文件**：扫描指定文件夹中的所有 response.dump 文件
2. **分析数据**：收集所有文件中 packageinfo 的字段并集
3. **创建表结构**：根据字段类型动态创建数据库表
4. **插入数据**：批量插入所有套餐数据
5. **优化数据库**：删除空列和重复内容列
6. **完成处理**：显示处理结果统计

## 📈 典型输出示例

```
2024-01-01 10:00:00 - INFO - 检查并创建数据库...
2024-01-01 10:00:01 - INFO - 数据库 unicom_person 已存在
2024-01-01 10:00:01 - INFO - 开始读取文件夹: unicom_person/
2024-01-01 10:00:02 - INFO - 总共读取到 13 个文件
2024-01-01 10:00:02 - INFO - 分析所有文件，收集tariffDetailInfoList完整字段信息...
2024-01-01 10:00:03 - INFO - 分析完成: 共处理 13 个文件，65 条记录，发现 45 个不同字段
2024-01-01 10:00:03 - INFO - 字段类型统计:
2024-01-01 10:00:03 - INFO -   INTEGER: 2 个字段
2024-01-01 10:00:03 - INFO -   TEXT: 43 个字段
2024-01-01 10:00:04 - INFO - 表 unicom_person_data 创建成功，包含 45 个字段
2024-01-01 10:00:04 - INFO - 成功插入 65 条记录
2024-01-01 10:00:05 - INFO - 分析完成: 发现 15 个空列，8 个重复内容列，22 个有效列
2024-01-01 10:00:05 - INFO - 优化完成: 表现在有 65 行，22 列
2024-01-01 10:00:05 - INFO - 数据处理和优化完成！
```

## 🛠️ 故障排除

### 常见问题

1. **数据库连接失败**

   - 检查 PostgreSQL 服务是否启动
   - 确认数据库配置信息正确

2. **JSON 解析错误**

   - 检查 response.dump 文件格式是否正确
   - 确认文件编码为 UTF-8

3. **没有找到数据**
   - 确认文件夹结构正确
   - 检查 JSON 数据是否包含 tariffDetailInfoList 字段

### 日志信息

脚本会输出详细的处理日志，包括：

- 文件读取进度
- 字段分析结果
- 数据插入状态
- 优化处理结果

## �� 许可证

MIT License
