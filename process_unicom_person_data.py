#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
联通个人套餐数据处理脚本
读取文件夹中的response.dump文件并导入到PostgreSQL数据库
处理 tariffDetailInfoList 数组中每个项的 packageinfo 字段
"""

import os
import json
import sys
import argparse
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 数据库配置
DB_CONFIG = {
    'host': '127.0.0.1',
    'port': 5432,
    'user': 'postgres',
    'password': 'postgres',
    'database': 'unicom_person'
}

def create_database_if_not_exists():
    """如果数据库不存在则创建"""
    try:
        # 连接到默认的postgres数据库
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database='postgres'
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # 检查数据库是否存在
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_CONFIG['database'],))
        exists = cursor.fetchone()
        
        if not exists:
            cursor.execute(f"CREATE DATABASE {DB_CONFIG['database']}")
            logger.info(f"数据库 {DB_CONFIG['database']} 创建成功")
        else:
            logger.info(f"数据库 {DB_CONFIG['database']} 已存在")
            
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"创建数据库时出错: {e}")
        raise

def get_db_connection():
    """获取数据库连接"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"连接数据库失败: {e}")
        raise

def create_table_from_all_data(conn, data_list, table_name='unicom_person_data'):
    """根据所有数据文件创建表结构 - 收集tariffDetailInfoList中所有字段的并集"""
    logger.info("分析所有文件，收集tariffDetailInfoList完整字段信息...")
    cursor = conn.cursor()
    
    # 删除已存在的表（如果存在）
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    logger.info(f"删除已存在的表: {table_name}")
    
    # 收集所有字段及其类型
    all_fields = {}  # {field_name: {type_info, sample_value, record_count}}
    processed_files = 0
    total_records = 0
    
    for item in data_list:
        file_path = item['file_path']
        data = item['data']
        
        if data and 'tariffDetailInfoList' in data and data['tariffDetailInfoList']:
            file_has_data = False
            
            for detail_item in data['tariffDetailInfoList']:
                if detail_item and 'packageinfo' in detail_item:  # 确保有packageinfo字段
                    file_has_data = True
                    total_records += 1
                    
                    packageinfo = detail_item['packageinfo']
                    if packageinfo and isinstance(packageinfo, dict):
                        # 处理packageinfo内的所有原始值字段
                        for key, value in packageinfo.items():
                            # 只处理原始值（字符串、数字、布尔值），忽略复杂对象
                            if isinstance(value, (str, int, float, bool)) or value is None:
                                _process_field(all_fields, key, value)
            
            if file_has_data:
                processed_files += 1
                if processed_files % 5 == 0:  # 每5个文件报告一次进度
                    logger.info(f"已处理 {processed_files} 个文件...")
    
    if not all_fields:
        logger.error("没有找到有效的tariffDetailInfoList数据用于创建表结构")
        raise ValueError("没有找到有效的tariffDetailInfoList数据用于创建表结构")
    
    logger.info(f"分析完成: 共处理 {processed_files} 个文件，{total_records} 条记录，发现 {len(all_fields)} 个不同字段")
    
    # 按字段名排序以保持一致性
    sorted_fields = sorted(all_fields.items())
    
    # 显示字段统计
    type_stats = {}
    for _, field_info in sorted_fields:
        field_type = field_info['type']
        type_stats[field_type] = type_stats.get(field_type, 0) + 1
    
    logger.info("字段类型统计:")
    for field_type, count in sorted(type_stats.items()):
        logger.info(f"  {field_type}: {count} 个字段")
    
    # 按指定顺序排列字段，重要字段在前面
    field_order = [
        'pagePackName',      # 第一列：套餐名称
        'pageMonthfee',      # 第二列：月费
        'serviceContent',    # 第三列：服务内容
        'suitArea'          # 第四列：适用区域
    ]
    
    # 创建表结构
    columns = []
    processed_fields = set()
    
    # 1. 首先添加指定顺序的重要字段
    for field_name in field_order:
        field_dict = dict(sorted_fields)
        if field_name in field_dict:
            field_info = field_dict[field_name]
            columns.append(f'"{field_name}" {field_info["type"]}')
            processed_fields.add(field_name)
            sample_str = str(field_info['sample'])[:50] if field_info['sample'] else 'NULL'
            logger.info(f"字段: {field_name} ({field_info['type']}) - 出现在 {field_info['record_count']} 条记录中 - 示例: {sample_str}...")
    
    # 2. 然后添加其他字段（按字母顺序）
    for field_name, field_info in sorted_fields:
        if field_name not in processed_fields:
            columns.append(f'"{field_name}" {field_info["type"]}')
            sample_str = str(field_info['sample'])[:50] if field_info['sample'] else 'NULL'
            logger.info(f"字段: {field_name} ({field_info['type']}) - 出现在 {field_info['record_count']} 条记录中 - 示例: {sample_str}...")
    
    # 创建表
    create_table_sql = f"""
    CREATE TABLE {table_name} (
        {', '.join(columns)}
    )
    """
    
    logger.info(f"创建表SQL预览: CREATE TABLE {table_name} (...{len(columns)} 个字段...)")
    cursor.execute(create_table_sql)
    conn.commit()
    logger.info(f"表 {table_name} 创建成功，包含 {len(columns)} 个字段")
    
    cursor.close()
    return table_name

def _process_field(all_fields, field_name, value):
    """处理单个字段，更新all_fields字典"""
    if field_name not in all_fields:
        # 第一次遇到这个字段，记录类型和示例值
        # 特殊处理pageMonthfee字段，强制设为NUMERIC类型
        if field_name == 'pageMonthfee':
            field_type = 'NUMERIC'
        elif isinstance(value, (list, dict)):
            field_type = 'JSONB'
        elif isinstance(value, bool):
            field_type = 'BOOLEAN'
        elif isinstance(value, int):
            field_type = 'INTEGER'
        elif isinstance(value, float):
            field_type = 'NUMERIC'
        else:
            field_type = 'TEXT'
        
        all_fields[field_name] = {
            'type': field_type, 
            'sample': value, 
            'record_count': 1
        }
    else:
        # 已经有这个字段，检查类型一致性并更新计数
        all_fields[field_name]['record_count'] += 1
        
        # 特殊处理pageMonthfee字段，保持NUMERIC类型
        if field_name == 'pageMonthfee':
            # 保持NUMERIC类型不变
            pass
        else:
            if isinstance(value, (list, dict)):
                current_type = 'JSONB'
            elif isinstance(value, bool):
                current_type = 'BOOLEAN'
            elif isinstance(value, int):
                current_type = 'INTEGER'
            elif isinstance(value, float):
                current_type = 'NUMERIC'
            else:
                current_type = 'TEXT'
            
            # 如果类型不一致，选择更通用的类型
            if all_fields[field_name]['type'] != current_type:
                # 优先级：JSONB > TEXT > NUMERIC > INTEGER > BOOLEAN
                type_priority = {'JSONB': 5, 'TEXT': 4, 'NUMERIC': 3, 'INTEGER': 2, 'BOOLEAN': 1}
                if type_priority.get(current_type, 0) > type_priority.get(all_fields[field_name]['type'], 0):
                    all_fields[field_name]['type'] = current_type
                    all_fields[field_name]['sample'] = value

def optimize_database(conn, table_name='unicom_person_data'):
    """优化数据库，删除所有行都没有任何数据的列和所有行内容完全相同的列"""
    from psycopg2.extras import RealDictCursor
    
    logger.info("开始优化数据库，分析空列和重复内容列...")
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # 获取所有列名
    cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = %s 
        ORDER BY ordinal_position
    """, (table_name,))
    
    columns = [row['column_name'] for row in cursor.fetchall()]
    logger.info(f"表 {table_name} 总共有 {len(columns)} 个列")
    
    empty_columns = []
    duplicate_columns = []
    valid_columns = []
    
    # 检查每个列是否为空或内容完全相同
    for column in columns:
        # 先获取列的数据类型
        cursor.execute("""
            SELECT data_type 
            FROM information_schema.columns 
            WHERE table_name = %s AND column_name = %s
        """, (table_name, column))
        
        data_type = cursor.fetchone()['data_type']
        
        # 根据数据类型使用不同的查询
        if data_type == 'jsonb':
            # 对于JSONB类型，检查是否为NULL或空对象/数组
            cursor.execute(f"""
                SELECT COUNT(*) as total_count,
                       COUNT(CASE WHEN "{column}" IS NOT NULL AND "{column}" != 'null'::jsonb THEN 1 END) as non_empty_count
                FROM {table_name}
            """)
        elif data_type in ('integer', 'numeric', 'boolean'):
            # 对于数值和布尔类型，只检查是否为NULL
            cursor.execute(f"""
                SELECT COUNT(*) as total_count,
                       COUNT(CASE WHEN "{column}" IS NOT NULL THEN 1 END) as non_empty_count
                FROM {table_name}
            """)
        else:
            # 对于文本类型，检查是否为NULL、空字符串、或「空」等值
            cursor.execute(f"""
                SELECT COUNT(*) as total_count,
                       COUNT(CASE WHEN "{column}" IS NOT NULL 
                                  AND "{column}" != '' 
                                  AND "{column}" != 'null'
                                  AND "{column}" != '空'
                                  AND "{column}" != 'NULL'
                                  AND TRIM("{column}") != ''
                             THEN 1 END) as non_empty_count
                FROM {table_name}
            """)
        
        result = cursor.fetchone()
        total_count = result['total_count']
        non_empty_count = result['non_empty_count']
        
        if non_empty_count == 0:
            empty_columns.append(column)
            logger.info(f"发现空列: {column} (0/{total_count} 有数据)")
        elif non_empty_count == total_count:
            # 检查是否所有行的内容都相同
            if data_type == 'jsonb':
                cursor.execute(f'SELECT COUNT(DISTINCT "{column}") as distinct_count FROM {table_name} WHERE "{column}" IS NOT NULL')
            elif data_type in ('integer', 'numeric', 'boolean'):
                cursor.execute(f'SELECT COUNT(DISTINCT "{column}") as distinct_count FROM {table_name} WHERE "{column}" IS NOT NULL')
            else:
                cursor.execute(f'SELECT COUNT(DISTINCT "{column}") as distinct_count FROM {table_name} WHERE "{column}" IS NOT NULL AND "{column}" != \'\'')
            
            distinct_count = cursor.fetchone()['distinct_count']
            
            if distinct_count <= 1:
                # 获取这个重复的值作为示例
                if data_type == 'jsonb':
                    cursor.execute(f'SELECT "{column}" FROM {table_name} WHERE "{column}" IS NOT NULL LIMIT 1')
                elif data_type in ('integer', 'numeric', 'boolean'):
                    cursor.execute(f'SELECT "{column}" FROM {table_name} WHERE "{column}" IS NOT NULL LIMIT 1')
                else:
                    cursor.execute(f'SELECT "{column}" FROM {table_name} WHERE "{column}" IS NOT NULL AND "{column}" != \'\' LIMIT 1')
                
                sample_result = cursor.fetchone()
                sample_value = sample_result[column] if sample_result else 'NULL'
                duplicate_columns.append(column)
                logger.info(f"发现重复内容列: {column} (所有行都是: {str(sample_value)[:50]}...)")
            else:
                valid_columns.append(column)
        else:
            valid_columns.append(column)
    
    logger.info(f"分析完成: 发现 {len(empty_columns)} 个空列，{len(duplicate_columns)} 个重复内容列，{len(valid_columns)} 个有效列")
    
    # 合并需要删除的列
    columns_to_delete = empty_columns + duplicate_columns
    
    # 删除空列和重复内容列
    if columns_to_delete:
        logger.info(f"开始删除 {len(columns_to_delete)} 个列（{len(empty_columns)} 个空列 + {len(duplicate_columns)} 个重复内容列）...")
        for column in columns_to_delete:
            try:
                cursor.execute(f'ALTER TABLE {table_name} DROP COLUMN "{column}"')
                if column in empty_columns:
                    logger.info(f"已删除空列: {column}")
                else:
                    logger.info(f"已删除重复内容列: {column}")
            except Exception as e:
                logger.error(f"删除列 {column} 失败: {e}")
                conn.rollback()
                cursor.close()
                raise
        
        conn.commit()
        logger.info(f"成功删除 {len(columns_to_delete)} 个列")
        
        # 显示优化后的表信息
        cursor.execute(f"SELECT COUNT(*) as row_count FROM {table_name}")
        row_count = cursor.fetchone()['row_count']
        
        cursor.execute("""
            SELECT COUNT(*) as column_count 
            FROM information_schema.columns 
            WHERE table_name = %s
        """, (table_name,))
        column_count = cursor.fetchone()['column_count']
        
        logger.info(f"优化完成: 表现在有 {row_count} 行，{column_count} 列")
    else:
        logger.info("没有发现空列，无需优化")
    
    cursor.close()

def insert_data_to_db(conn, data_list, table_name='unicom_person_data'):
    """将数据插入数据库"""
    if not data_list:
        logger.warning("没有数据需要插入")
        return
    
    cursor = conn.cursor()
    
    # 获取数据库表的所有字段
    cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = %s 
        ORDER BY ordinal_position
    """, (table_name,))
    
    db_columns = [row[0] for row in cursor.fetchall()]
    logger.info(f"数据库表有 {len(db_columns)} 个字段")
    
    # 构建插入SQL，字段名加双引号防止关键字冲突
    fields = [f'"{col}"' for col in db_columns]
    placeholders = ', '.join(['%s'] * len(fields))
    insert_sql = f"INSERT INTO {table_name} ({', '.join(fields)}) VALUES ({placeholders})"
    
    # 准备数据 - 处理tariffDetailInfoList数组中的每个元素
    insert_data = []
    total_records = 0
    
    for item in data_list:
        tariff_detail_info_list = item['data'].get('tariffDetailInfoList', [])
        
        if isinstance(tariff_detail_info_list, list):
            for detail_item in tariff_detail_info_list:
                if detail_item and 'packageinfo' in detail_item:  # 确保有packageinfo字段
                    packageinfo = detail_item['packageinfo']
                    
                    if packageinfo and isinstance(packageinfo, dict):
                        # 构建数据行
                        row_data = []
                        for field in db_columns:
                            value = packageinfo.get(field)
                            # 特殊处理pageMonthfee字段，转换为浮点数
                            if field == 'pageMonthfee' and value is not None:
                                try:
                                    # 将字符串转换为浮点数
                                    value = float(str(value))
                                except (ValueError, TypeError):
                                    logger.warning(f"无法将pageMonthfee值 '{value}' 转换为浮点数，设为NULL")
                                    value = None
                            # 如果值是字典或列表，转换为JSON字符串
                            elif isinstance(value, (dict, list)):
                                value = json.dumps(value, ensure_ascii=False)
                            row_data.append(value)
                        
                        insert_data.append(row_data)
                        total_records += 1
    
    # 批量插入
    if insert_data:
        cursor.executemany(insert_sql, insert_data)
        conn.commit()
        logger.info(f"成功插入 {len(insert_data)} 条记录")
    else:
        logger.warning("没有有效的数据需要插入")
    
    cursor.close()

def read_response_dump_files(folder_path):
    """读取文件夹中所有response.dump文件"""
    folder_path = Path(folder_path)
    
    if not folder_path.exists():
        logger.error(f"文件夹不存在: {folder_path}")
        return []
    
    data_list = []
    
    # 遍历子文件夹
    for subfolder in folder_path.iterdir():
        if subfolder.is_dir():
            response_file = subfolder / 'response.dump'
            
            if response_file.exists():
                try:
                    with open(response_file, 'r', encoding='utf-8') as f:
                        content = f.read()
                        logger.info(f"文件内容长度: {len(content)} 字符")
                        logger.info(f"文件内容前200字符: {content[:200]}...")
                        
                        # 尝试解析JSON
                        json_data = json.loads(content)
                        logger.info(f"JSON解析成功，根键: {list(json_data.keys()) if isinstance(json_data, dict) else type(json_data)}")
                        
                        data_list.append({
                            'folder_name': subfolder.name,
                            'file_path': str(response_file),
                            'data': json_data
                        })
                        logger.info(f"成功读取: {response_file}")
                        
                except json.JSONDecodeError as e:
                    logger.error(f"JSON解析错误 {response_file}: {e}")
                    logger.error(f"文件内容: {content[:500]}...")
                except Exception as e:
                    logger.error(f"读取文件错误 {response_file}: {e}")
            else:
                logger.warning(f"文件不存在: {response_file}")
    
    logger.info(f"总共读取到 {len(data_list)} 个文件")
    return data_list

def clear_database():
    """清空数据库"""
    try:
        # 连接到unicom_person数据库
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database=DB_CONFIG['database']
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # 删除所有表
        cursor.execute("""
            SELECT tablename FROM pg_tables 
            WHERE schemaname = 'public'
        """)
        tables = cursor.fetchall()
        
        for table in tables:
            table_name = table[0]
            cursor.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')
            logger.info(f"删除表: {table_name}")
        
        cursor.close()
        conn.close()
        logger.info("数据库清空完成")
        
    except Exception as e:
        logger.error(f"清空数据库时出错: {e}")
        raise

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='处理联通个人套餐数据并导入PostgreSQL数据库')
    parser.add_argument('folder_path', help='包含response.dump文件的文件夹路径')
    parser.add_argument('--table-name', default='unicom_person_data', help='数据库表名 (默认: unicom_person_data)')
    parser.add_argument('--clear-db', action='store_true', help='清空数据库后重新创建')
    
    args = parser.parse_args()
    
    try:
        # 1. 创建数据库（如果不存在）
        logger.info("检查并创建数据库...")
        create_database_if_not_exists()
        
        # 1.5. 如果指定了清空数据库，则先清空
        if args.clear_db:
            logger.info("清空数据库...")
            clear_database()
        
        # 2. 读取所有response.dump文件
        logger.info(f"开始读取文件夹: {args.folder_path}")
        data_list = read_response_dump_files(args.folder_path)
        
        if not data_list:
            logger.error("没有找到有效的数据文件")
            return
        
        # 3. 连接数据库
        logger.info("连接数据库...")
        conn = get_db_connection()
        
        # 4. 创建表结构（基于所有文件的字段并集）
        logger.info("创建表结构...")
        table_name = create_table_from_all_data(conn, data_list, args.table_name)
        
        # 5. 插入数据
        logger.info("开始插入数据...")
        insert_data_to_db(conn, data_list, table_name)
        
        # 6. 优化数据库（删除空列）
        logger.info("开始优化数据库...")
        optimize_database(conn, table_name)
        
        # 7. 关闭连接
        conn.close()
        
        logger.info("数据处理和优化完成！")
        
    except Exception as e:
        logger.error(f"处理过程中出错: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main() 