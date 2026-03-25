"""
SampleR
=======

一个基于 Tkinter 的抽样处理桌面工具。

"""

import math
import os
import random
import re
import threading
import time
import traceback
import warnings
from datetime import datetime

import chardet
import numpy as np
import openpyxl
import pandas as pd
import tkinter as tk
import xlsxwriter
from tkinter import filedialog, messagebox, ttk

# 忽略警告
warnings.filterwarnings('ignore')

class SamplingProcessor:
    """负责规则解析、条件筛选、抽样执行与结果保存。"""

    def __init__(self, rule_file_path, sample_path, dict_path=None, sheet_name=None, log_file=None,default_random_seed=666):
        self.rule_file_path = rule_file_path
        self.sample_path = sample_path
        self.dict_path = dict_path
        self.sheet_name = sheet_name
        self.default_random_seed = default_random_seed
        self.rule_df = None
        self.dict_data = {}
        self.company_rules = {}
        self.results = {}
        self.remark_cols = []
        self.total_rules = 0
        self.processed_rules = 0
        self.processed_files = 0
        self.total_files = 0

        # 日志文件
        self.log_file = log_file
        self.log_lock = threading.Lock()

        # 加载规则和字典
        self.load_rules()
        self.load_dictionaries()
        self.process_rules()

    def log(self, message):
        """记录日志到控制台和日志文件"""
        print(message)
        if self.log_file:
            try:
                with self.log_lock:
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    self.log_file.write(f"[{timestamp}] {message}\n")
                    self.log_file.flush()
            except Exception as e:
                print(f"写入日志文件时出错: {str(e)}")

    def read_file_with_encoding_detection(self, file_path):
        """读取文件并自动检测编码（主要用于CSV文件）"""
        self.log(f"尝试读取文件: {file_path}")

        if file_path.endswith('.csv'):
            try:
                self.log("尝试UTF-8编码")
                return pd.read_csv(file_path, encoding='utf-8')
            except UnicodeDecodeError:
                try:
                    self.log("尝试UTF-8 with BOM编码")
                    return pd.read_csv(file_path, encoding='utf-8-sig')
                except UnicodeDecodeError:
                    try:
                        self.log("尝试GBK编码")
                        return pd.read_csv(file_path, encoding='gbk')
                    except UnicodeDecodeError:
                        try:
                            self.log("尝试GB2312编码")
                            return pd.read_csv(file_path, encoding='gb2312')
                        except UnicodeDecodeError:
                            try:
                                self.log("常见编码失败，使用chardet自动检测编码")
                                # 读取文件的一部分来检测编码
                                with open(file_path, 'rb') as f:
                                    raw_data = f.read(10000)  # 读取前10000字节用于检测
                                    result = chardet.detect(raw_data)
                                    encoding = result['encoding']
                                    confidence = result['confidence']
                                    self.log(f"检测到编码: {encoding}, 置信度: {confidence}")

                                # 使用检测到的编码读取文件
                                return pd.read_csv(file_path, encoding=encoding)
                            except Exception as e:
                                self.log(f"自动编码检测失败: {e}")
                                # 最后尝试使用错误忽略策略
                                self.log("尝试使用错误忽略策略")
                                return pd.read_csv(file_path, encoding='utf-8', errors='replace')
            except Exception as e:
                self.log(f"读取CSV文件时出错: {e}")
                # 如果所有方法都失败，抛出异常
                raise
        else:
            try:
                return pd.read_excel(file_path)
            except Exception as e:
                self.log(f"读取Excel文件时出错: {e}")
                raise

    def read_csv_with_encoding_detection(self, file_path):
        """读取CSV文件并自动检测编码"""
        self.log(f"尝试读取CSV文件: {file_path}")

        # 按顺序尝试多种常见编码
        encodings_to_try = ['utf-8', 'utf-8-sig', 'gbk', 'gb2312', 'latin1']

        for encoding in encodings_to_try:
            try:
                self.log(f"尝试编码: {encoding}")
                df = pd.read_csv(file_path, encoding=encoding)
                self.log(f"成功使用编码: {encoding}")
                return df
            except UnicodeDecodeError:
                self.log(f"编码 {encoding} 失败")
                continue
            except Exception as e:
                self.log(f"使用编码 {encoding} 时出错: {e}")
                continue

        # 如果常见编码都失败，使用chardet自动检测编码
        try:
            self.log("常见编码失败，使用chardet自动检测编码")
            # 读取文件的一部分来检测编码
            with open(file_path, 'rb') as f:
                raw_data = f.read(10000)  # 读取前10000字节用于检测
                result = chardet.detect(raw_data)
                encoding = result['encoding']
                confidence = result['confidence']
                self.log(f"检测到编码: {encoding}, 置信度: {confidence}")

            # 使用检测到的编码读取文件
            return pd.read_csv(file_path, encoding=encoding)
        except Exception as e:
            self.log(f"自动编码检测失败: {e}")
            # 最后尝试使用错误忽略策略
            self.log("尝试使用错误忽略策略")
            return pd.read_csv(file_path, encoding='utf-8', errors='replace')

    def normalize_symbols(self, text):
        """将全角符号转换为半角符号"""
        if not isinstance(text, str):
            return text

        replacements = {
            '＃': '#', '＠': '@', '％': '%', '，': ',', '、': ',', "\u3000": " ", '：': ':',
            '（': '(', '）': ')', '【': '[', '】': ']', '「': '[', '」': ']'
        }

        for full, half in replacements.items():
            text = text.replace(full, half)

        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def load_rules(self):
        """加载并预处理规则文件"""
        self.log(f"开始加载规则文件: {os.path.basename(self.rule_file_path)}")
        try:
            self.rule_df = pd.read_excel(self.rule_file_path, dtype=str,header=0)
            self.log(f"规则文件加载成功，包含 {len(self.rule_df)} 行规则")

            # 重命名重复列名，去除后缀
            new_columns = []
            col_count = {}
            for col in self.rule_df.columns:
                normalized_col = self.normalize_symbols(col)
                # 移除后缀如 .1, .2
                base_col = re.sub(r'\.\d+$', '', normalized_col)

                # 统计每个基础列名出现的次数
                if base_col not in col_count:
                    col_count[base_col] = 1
                else:
                    col_count[base_col] += 1

                # 只保留基础列名
                new_columns.append(base_col)

            self.rule_df.columns = new_columns
            self.log(f"处理后的列名: {list(self.rule_df.columns)}")

        except Exception as e:
            self.log(f"加载规则文件失败: {str(e)}")
            raise

        # 创建规则数据结构
        self.rules = []
        self.company_rules = {}

        # 提取所有列名并规范化
        columns = [self.normalize_symbols(col) for col in self.rule_df.columns]
        self.log(f"规则文件列名: {columns}")

        # 查找关键列索引
        try:
            self.file_name_col = columns.index("#文件名")
            self.sampling_method_col = columns.index("#抽样方式")
            self.log(f"文件名列索引: {self.file_name_col}")
            self.log(f"抽样方式列索引: {self.sampling_method_col}")
        except ValueError as e:
            error_msg = f"规则文件缺少必要的列标题: {str(e)}"
            self.log(error_msg)
            raise ValueError(error_msg)

        # 分类其他列
        self.start_date_cols = [i for i, col in enumerate(columns) if "#开始时间@" in col]
        self.end_date_cols = [i for i, col in enumerate(columns) if "#结束时间@" in col]
        self.exclude_cols = [i for i, col in enumerate(columns) if "#剔除@" in col]
        self.include_cols = [i for i, col in enumerate(columns) if "#筛选@" in col]
        self.startswith_cols = [i for i, col in enumerate(columns) if "#开头为@" in col]
        self.remark_cols = [i for i, col in enumerate(columns) if "#备注:" in col]

        self.log(f"开始时间列索引: {self.start_date_cols}")
        self.log(f"结束时间列索引: {self.end_date_cols}")
        self.log(f"剔除列索引: {self.exclude_cols}")
        self.log(f"筛选列索引: {self.include_cols}")
        self.log(f"开头为列索引: {self.startswith_cols}")
        self.log(f"备注列索引: {self.remark_cols}")

    def load_dictionaries(self):
        """加载字典文件"""
        if not self.dict_path:
            self.log("未提供字典路径，跳过字典加载")
            return

        self.log(f"开始加载字典文件: {self.dict_path}")
        if os.path.isfile(self.dict_path):
            self._load_dict_file(self.dict_path)
        elif os.path.isdir(self.dict_path):
            dict_files = [f for f in os.listdir(self.dict_path) if f.endswith('.txt')]
            self.log(f"在字典文件夹中找到 {len(dict_files)} 个字典文件")
            for file in dict_files:
                self._load_dict_file(os.path.join(self.dict_path, file))

    def _load_dict_file(self, file_path):
        """加载单个字典文件"""
        name = os.path.splitext(os.path.basename(file_path))[0]
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                items = [self.normalize_symbols(line.strip()) for line in f]
                items = [item for item in items if item]

                if items:
                    self.dict_data[name] = items
                    self.log(f"加载字典 '{name}' 成功，包含 {len(items)} 个项目")
        except Exception as e:
            self.log(f"加载字典文件 '{file_path}' 失败: {str(e)}")

    def parse_company_list(self, cell_value):
        """解析公司名称列表，处理字典引用"""
        if not isinstance(cell_value, str):
            self.log("公司单元格内容不是字符串")
            return []

        self.log(f"解析公司列表 - 原始内容: '{cell_value}'")
        normalized_value = self.normalize_symbols(cell_value)
        self.log(f"规范化后: '{normalized_value}'")

        parts = [p.strip() for p in normalized_value.split(',') if p.strip()]
        self.log(f"分割后的部分: {parts}")

        companies = []

        for part in parts:
            self.log(f"处理部分: '{part}'")
            if part.startswith('#字典%'):
                dict_name = part[4:].strip()
                self.log(f"发现字典引用: {part} -> 字典名称: '{dict_name}'")
                if dict_name in self.dict_data:
                    self.log(f"从字典 '{dict_name}' 获取公司列表: {self.dict_data[dict_name]}")
                    companies.extend(self.dict_data[dict_name])
                else:
                    self.log(f"警告: 未找到字典 '{dict_name}'")
            else:
                self.log(f"添加公司: {part}")
                companies.append(part)

        self.log(f"最终公司列表: {companies}")
        return companies

    def parse_condition(self, condition_text):
        """解析条件文本，返回(类型, 值)元组"""
        if not isinstance(condition_text, str):
            return None, None

        condition_text = condition_text.strip()
        if not condition_text:
            return None, None

        self.log(f"解析条件: '{condition_text}'")
        condition_text = condition_text.replace('丨', '|')

        # 预处理：检测是否包含字典引用和其他条件混合
        parts = [p.strip() for p in condition_text.split('、') if p.strip()]

        # 处理字典引用并收集所有条件
        all_conditions = []
        for part in parts:
            if part.startswith('#字典%'):
                # 专门处理字典引用
                dict_name = part[4:].strip()
                self.log(f"发现字典引用: {dict_name}")
                if dict_name in self.dict_data:
                    values = self.dict_data[dict_name]
                    self.log(f"从字典 '{dict_name}' 获取值列表: {values}")
                    all_conditions.extend([('value', val) for val in values])
                else:
                    self.log(f"警告: 未找到字典 '{dict_name}'")
            else:
                # 处理其他条件
                cond_type, cond_value = self._parse_single_condition(part)
                if cond_type == 'multiple':
                    all_conditions.extend(cond_value)
                elif cond_type:
                    all_conditions.append((cond_type, cond_value))

        # 根据结果数量返回不同类型
        if not all_conditions:
            return None, None
        elif len(all_conditions) == 1:
            return all_conditions[0]  # 返回单个条件
        else:
            self.log(f"合并多个条件: {all_conditions}")
            return 'multiple', all_conditions  # 返回平铺的条件列表

    def _parse_single_condition(self, condition_text):
        """解析单个条件"""
        if not isinstance(condition_text, str):
            return None, None

        condition_text = condition_text.strip()
        if not condition_text:
            return None, None

        self.log(f"解析单个条件: '{condition_text}'")

        if condition_text.startswith('#字典%'):
            dict_name = condition_text[4:].strip()
            self.log(f"发现字典引用: {dict_name}")
            if dict_name in self.dict_data:
                values = self.dict_data[dict_name]
                self.log(f"从字典 '{dict_name}' 获取值列表: {values}")
                # 直接返回多个值条件
                return 'multiple',[('value', val) for val in values]
            else:
                self.log(f"警告: 未找到字典 '{dict_name}'")
                return None, None

        if condition_text.startswith('#等于'):
            value = condition_text[3:].strip()
            self.log(f"解析为相等条件: {value}")
            return 'equal', value
        elif condition_text.startswith('#不等于'):
            value = condition_text[4:].strip()
            self.log(f"解析为不等条件: {value}")
            return 'not_equal', value
        elif condition_text.startswith('#大于'):
            value = condition_text[3:].strip()
            self.log(f"解析为大于条件: {value}")
            return 'greater', value
        elif condition_text.startswith('#小于'):
            value = condition_text[3:].strip()
            self.log(f"解析为小于条件: {value}")
            return 'less', value
        elif condition_text.startswith('#开头为'):
            value = condition_text[3:].strip()
            self.log(f"解析为开头为条件: {value}")
            return 'startswith', value
        elif condition_text.startswith('#排除%'):
            parts = condition_text[4:].split('|', 1)
            if len(parts) == 2:
                A = parts[0].strip()
                B = parts[1].strip()
                self.log(f"解析为排除相关条件: A={A}, B={B}")
                return 'exclude_related', (A, B)
            else:
                self.log("解析为普通值条件")
                return 'value', condition_text
        else:
            if ',' in condition_text:
                values = [c.strip() for c in condition_text.split(',')]
                self.log(f"解析为列表条件: {values}")
                # 返回平铺的多个值条件
                return 'multiple', [('value', val) for val in values]
            else:
                self.log(f"解析为普通值条件: {condition_text}")
                return 'value', condition_text

    def parse_date(self, date_input):
        """解析多种格式的日期 - 增强版"""
        if isinstance(date_input, (int, float)):
            date_str = str(int(date_input))
        elif isinstance(date_input, str):
            date_str = date_input.strip()
        else:
            return None

        if not date_str:
            return None

        self.log(f"解析日期: {date_str}")
        # 预处理非标准格式
        if re.match(r'^\d{4}-\d{1,2}-\d{1,2}$', date_str):
            parts = date_str.split('-')
            if len(parts[1]) == 1: parts[1] = '0' + parts[1]
            if len(parts[2]) == 1: parts[2] = '0' + parts[2]
            date_str = '-'.join(parts)
        elif re.match(r'^\d{4}/\d{1,2}/\d{1,2}$', date_str):
            parts = date_str.split('/')
            if len(parts[1]) == 1: parts[1] = '0' + parts[1]
            if len(parts[2]) == 1: parts[2] = '0' + parts[2]
            date_str = '/'.join(parts)
        elif re.match(r'^\d{4}年\d{1,2}月\d{1,2}日$', date_str):
            date_str = date_str.replace('年', '-').replace('月', '-').replace('日', '')
            parts = date_str.split('-')
            if len(parts[1]) == 1: parts[1] = '0' + parts[1]
            if len(parts[2]) == 1: parts[2] = '0' + parts[2]
            date_str = '-'.join(parts)
        elif re.match(r'^\d{8}$', date_str):
            date_str = f"{date_str[0:4]}-{date_str[4:6]}-{date_str[6:8]}"

        formats = [
            '%Y%m%d',  # 20241001
            '%Y-%m-%d',  # 2024-10-01
            '%Y/%m/%d',  # 2024/10/01
            '%Y年%m月%d日',  # 2024年10月01日
            '%Y-%m-%d %H:%M:%S',
            '%Y/%m/%d %H:%M:%S',
            '%Y%m%d%H%M%S',
            '%Y%m%d'  # 再次尝试8位数字格式
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                self.log(f"成功解析日期: {dt.strftime('%Y-%m-%d')}")
                return dt
            except ValueError:
                continue

        self.log(f"无法解析日期: {date_str}")
        return None

    def process_rules(self):
        """处理所有规则 - 重构版"""
        self.log(f"开始处理规则，共 {len(self.rule_df)} 条规则")
        self.total_rules = len(self.rule_df)

        # 遍历每一行规则
        for idx, row in self.rule_df.iterrows():
            self.processed_rules += 1
            self.log(f"\n===== 处理规则 {self.processed_rules}/{self.total_rules} =====")

            # 准备规则字典
            rule_dict = {
                'start_dates': [],
                'end_dates': [],
                'excludes': [],
                'includes': [],
                'startswiths': [],  # 问题5: 添加开头为条件
                'remarks': []  # 新增：存储备注信息
            }

            # 处理公司名称
            company_cell = row.iloc[self.file_name_col]
            companies = self.parse_company_list(company_cell)
            if not companies:
                self.log("未找到有效公司名称，跳过此规则")
                continue

            # 处理开始时间
            for col_idx in self.start_date_cols:
                col_name = self.rule_df.columns[col_idx]
                cell_value = row.iloc[col_idx]

                if pd.notna(cell_value) and cell_value != "":
                    # 提取目标列名（去掉"#开始时间@"前缀）
                    target_col = col_name.replace("#开始时间@", "", 1)

                    # 解析日期值
                    date_val = self.parse_date(cell_value)
                    if date_val:
                        rule_dict['start_dates'].append({
                            'column': target_col,  # 使用提取的目标列名
                            'value': date_val
                        })
                        self.log(f"添加开始时间条件: 列={target_col}, 值={date_val.strftime('%Y-%m-%d')}")

            # 处理结束时间
            for col_idx in self.end_date_cols:
                col_name = self.rule_df.columns[col_idx]
                cell_value = row.iloc[col_idx]

                if pd.notna(cell_value) and cell_value != "":
                    # 提取目标列名（去掉"#结束时间@"前缀）
                    target_col = col_name.replace("#结束时间@", "", 1)

                    date_val = self.parse_date(cell_value)
                    if date_val:
                        rule_dict['end_dates'].append({
                            'column': target_col,  # 使用提取的目标列名
                            'value': date_val
                        })
                        self.log(f"添加结束时间条件: 列={target_col}, 值={date_val.strftime('%Y-%m-%d')}")

            # 处理剔除条件
            for col_idx in self.exclude_cols:
                col_name = self.rule_df.columns[col_idx]
                cell_value = row.iloc[col_idx]
                if pd.notna(cell_value) and cell_value != "":
                    cond_type, cond_value = self.parse_condition(cell_value)
                    if cond_type:
                        target_col = col_name.replace("#剔除@", "", 1)
                        rule_dict['excludes'].append({
                            'column': target_col,
                            'type': cond_type,
                            'value': cond_value
                        })
                        self.log(f"添加剔除条件: 列={target_col}, 类型={cond_type}, 值={cond_value}")

            # 处理筛选条件
            for col_idx in self.include_cols:
                col_name = self.rule_df.columns[col_idx]
                cell_value = row.iloc[col_idx]
                if pd.notna(cell_value) and cell_value != "":
                    cond_type, cond_value = self.parse_condition(cell_value)
                    if cond_type:
                        target_col = col_name.replace("#筛选@", "", 1)
                        rule_dict['includes'].append({
                            'column': target_col,
                            'type': cond_type,
                            'value': cond_value
                        })
                        self.log(f"添加筛选条件: 列={target_col}, 类型={cond_type}, 值={cond_value}")

            for col_idx in self.startswith_cols:
                col_name = self.rule_df.columns[col_idx]
                cell_value = row.iloc[col_idx]
                if pd.notna(cell_value) and cell_value != "":
                    cond_type, cond_value = self.parse_condition(cell_value)
                    if cond_type:
                        target_col = col_name.replace("#开头为@", "", 1)
                        rule_dict['startswiths'].append({
                            'column': target_col,
                            'type': cond_type,
                            'value': cond_value
                        })
                        self.log(f"添加开头为条件: 列={target_col}, 类型={cond_type}, 值={cond_value}")

            for col_idx in self.remark_cols:
                col_name = self.rule_df.columns[col_idx]
                cell_value = row.iloc[col_idx]
                if pd.notna(cell_value) and cell_value != "":
                    # 提取目标列名（去掉"#备注:"前缀）
                    target_col = col_name.replace("#备注:", "", 1)
                    rule_dict['remarks'].append({
                        'column': target_col,
                        'value': str(cell_value)
                    })
                    self.log(f"添加备注: 列={target_col}, 值={cell_value}")

            # 处理抽样方式
            if self.sampling_method_col is not None:
                sampling_method = row.iloc[self.sampling_method_col]
                if pd.notna(sampling_method) and isinstance(sampling_method, str):
                    sampling_method = sampling_method.replace('（', '(').replace('）', ')')
                    parsed_method = self.parse_sampling_method(sampling_method)
                    if parsed_method:
                        rule_dict['sampling_method'] = parsed_method
                        self.log(f"添加抽样方法: 类型={parsed_method['type']}, 参数={parsed_method}")

            # 添加到总规则列表
            self.rules.append(rule_dict)

            # 为每个公司添加规则
            for company in companies:
                if company not in self.company_rules:
                    self.company_rules[company] = []
                self.company_rules[company].append(rule_dict)

        # 打印规则统计信息
        self.log(f"\n规则处理完成，总共加载 {len(self.rules)} 条规则")
        self.log(f"公司规则字典包含 {len(self.company_rules)} 家公司")
        for company, rules in self.company_rules.items():
            self.log(f"公司 '{company}' 有 {len(rules)} 条规则")

    def parse_sampling_method(self, method_text):
        """解析抽样方法文本"""
        method_text = method_text.strip()
        if not method_text:
            return None

        self.log(f"解析抽样方法: {method_text}")

        # 方法17，#随机抽XX个，随机数种子XXX 或 #随机抽XX个
        if method_text.startswith("#随机抽"):
            self.log("检测到抽样方法“#随机抽XX个，随机数种子XXX 或 #随机抽XX个”，方法序号17，具体解释请查阅https://t.lwb.net.cn/web/#/660119647/120456986")
            try:
                # 解析格式1: #随机抽XX个，随机数种子XXX
                pattern1 = r'#随机抽(\d+)个，随机数种子(\d+)'
                match1 = re.search(pattern1, method_text)

                # 解析格式2: #随机抽XX个
                pattern2 = r'#随机抽(\d+)个'
                match2 = re.search(pattern2, method_text)

                if match1:
                    n_samples = int(match1.group(1))
                    random_seed = int(match1.group(2))
                    self.log(f"方法17参数: 样本数={n_samples}, 种子={random_seed}")
                elif match2:
                    n_samples = int(match2.group(1))
                    random_seed = self.default_random_seed  # 使用默认种子
                    self.log(f"方法17参数: 样本数={n_samples}, 使用默认种子={random_seed}")
                else:
                    self.log("无法解析随机抽样格式")
                    return None

                return {
                    'type': 17,
                    'n_samples': n_samples,
                    'random_seed': random_seed
                }
            except Exception as e:
                self.log(f"解析随机抽样方法出错: {e}")
                import traceback
                self.log(traceback.format_exc())
            return None

        # 方法7，#（（@A+@B…）前X的@C）整体（@A+@B…）取绝对值后每X月取前X
        elif "整体" in method_text and "取绝对值后每" in method_text and "前" in method_text and "的@" in method_text:
            self.log("检测到抽样方法“#（（@A+@B…）前X的@C）整体（@A+@B…）取绝对值后每X月取前X”，方法序号7，具体解释请查阅https://t.lwb.net.cn/web/#/660119647/120456986")
            try:
                # 提取分组信息（前X的@C）
                group_match = re.search(r'前(\d+)的@(\w+)', method_text)
                if not group_match:
                    self.log("无法提取分组信息")
                    return None

                top_n = int(group_match.group(1))  # 前X的X
                group_col = group_match.group(2)  # @C的C

                # 提取分组值列名（（@A+@B…））
                value_match1 = re.search(r'#\(\(@(.+?)\)前', method_text)
                if not value_match1:
                    self.log("无法提取分组值列名")
                    return None

                group_cols = value_match1.group(1).split('+@')  # @A+@B分割为列表

                # 提取抽样值列名（@A+@B…）
                value_match2 = re.search(r'整体\(@(.+?)\)取绝对值后每', method_text)
                if not value_match2:
                    self.log("无法提取抽样值列名")
                    return None

                sample_cols = value_match2.group(1).split('+@')

                # 从"每"字后面开始提取数字
                numbers_match = re.search(r'每(\d+)月取前(\d+)', method_text)
                if not numbers_match:
                    self.log("无法提取月份间隔和TopN值")
                    return None

                month_interval = int(numbers_match.group(1))
                sample_top_n = int(numbers_match.group(2))

                self.log(f"方法7参数: 分组列={group_col}, 分组值列={group_cols}, 抽样值列={sample_cols}, "
                         f"TopN={top_n}, 月间隔={month_interval}, 样本TopN={sample_top_n}")
                return {
                    'type': 7,
                    'group_column': group_col,
                    'group_columns': group_cols,
                    'columns': sample_cols,
                    'top_n': top_n,
                    'month_interval': month_interval,
                    'sample_top_n': sample_top_n,
                }
            except Exception as e:
                self.log(f"解析方法7出错: {e}")
                import traceback
                self.log(traceback.format_exc())
            return None

        # 方法8，#（（@A+@B…）前X的@C）整体（@A+@B…）取绝对值后的前X
        elif "整体" in method_text and "取绝对值后的前" in method_text and "前" in method_text and "的@" in method_text:
            self.log("检测到抽样方法“#（（@A+@B…）前X的@C）整体（@A+@B…）取绝对值后的前X”，方法序号8，具体解释请查阅https://t.lwb.net.cn/web/#/660119647/120456986")
            try:
                # 提取分组信息（前X的@C）
                group_match = re.search(r'前(\d+)的@(\w+)', method_text)
                if not group_match:
                    self.log("无法提取分组信息")
                    return None

                top_n = int(group_match.group(1))  # 前X的X
                group_col = group_match.group(2)  # @C的C

                # 提取分组值列名（（@A+@B…））
                value_match1 = re.search(r'#\(\(@(.+?)\)前', method_text)
                if not value_match1:
                    self.log("无法提取分组值列名")
                    return None

                group_cols = value_match1.group(1).split('+@')  # @A+@B分割为列表

                # 提取抽样值列名（@A+@B…）
                value_match2 = re.search(r'整体\(@(.+?)\)取绝对值后的前', method_text)
                if not value_match2:
                    self.log("无法提取抽样值列名")
                    return None

                sample_cols = value_match2.group(1).split('+@')

                # 从"前"字后面提取数字
                sample_top_n_match = re.search(r'后的前(\d+)', method_text)
                if not sample_top_n_match:
                    self.log("无法提取样本TopN值")
                    return None

                sample_top_n = int(sample_top_n_match.group(1))

                self.log(f"方法8参数: 分组列={group_col}, 分组值列={group_cols}, 抽样值列={sample_cols}, "
                         f"TopN={top_n}, 样本TopN={sample_top_n}")
                return {
                    'type': 8,
                    'group_column': group_col,
                    'group_columns': group_cols,
                    'columns': sample_cols,
                    'top_n': top_n,
                    'sample_top_n': sample_top_n,
                }
            except Exception as e:
                self.log(f"解析方法8出错: {e}")
                import traceback
                self.log(traceback.format_exc())
            return None

        # 方法15，#（（@A+@B…）前X的@C）整体（@A+@B…）取绝对值后大于XXX
        elif "整体" in method_text and "取绝对值后大于" in method_text and "前" in method_text and "的@" in method_text:
            self.log("检测到抽样方法“#（（@A+@B…）前X的@C）整体（@A+@B…）取绝对值后大于XXX”，方法序号15，具体解释请查阅https://t.lwb.net.cn/web/#/660119647/120456986")
            try:
                # 提取分组信息（前X的@C）
                group_match = re.search(r'前(\d+)的@(\w+)', method_text)
                if not group_match:
                    self.log("无法提取分组信息")
                    return None

                top_n = int(group_match.group(1))  # 前X的X
                group_col = group_match.group(2)  # @C的C

                # 提取分组值列名（（@A+@B…））
                value_match1 = re.search(r'#\(\(@(.+?)\)前', method_text)
                if not value_match1:
                    self.log("无法提取分组值列名")
                    return None

                group_cols = value_match1.group(1).split('+@')  # @A+@B分割为列表

                # 提取抽样值列名（@A+@B…）
                value_match2 = re.search(r'整体\(@(.+?)\)取绝对值后大于', method_text)
                if not value_match2:
                    self.log("无法提取抽样值列名")
                    return None

                sample_cols = value_match2.group(1).split('+@')

                # 提取阈值
                threshold_match = re.search(r'大于(\d+(\.\d+)?)', method_text)
                if not threshold_match:
                    self.log("无法提取阈值")
                    return None

                threshold = float(threshold_match.group(1))

                self.log(f"方法15参数: 分组列={group_col}, 分组值列={group_cols}, 抽样值列={sample_cols}, "
                         f"TopN={top_n}, 阈值={threshold}")
                return {
                    'type': 15,
                    'group_column': group_col,
                    'group_columns': group_cols,
                    'columns': sample_cols,
                    'top_n': top_n,
                    'threshold': threshold,
                    'comparison': 'greater',
                }
            except Exception as e:
                self.log(f"解析方法15出错: {e}")
                import traceback
                self.log(traceback.format_exc())
            return None

        # 方法16，#（（@A+@B…）前X的@C）整体（@A+@B…）取绝对值后小于XXX
        elif "整体" in method_text and "取绝对值后小于" in method_text and "前" in method_text and "的@" in method_text:
            self.log("检测到抽样方法“#（（@A+@B…）前X的@C）整体（@A+@B…）取绝对值后小于XXX”，方法序号16，具体解释请查阅https://t.lwb.net.cn/web/#/660119647/120456986")
            try:
                # 提取分组信息（前X的@C）
                group_match = re.search(r'前(\d+)的@(\w+)', method_text)
                if not group_match:
                    self.log("无法提取分组信息")
                    return None

                top_n = int(group_match.group(1))  # 前X的X
                group_col = group_match.group(2)  # @C的C

                # 提取分组值列名（（@A+@B…））
                value_match1 = re.search(r'#\(\(@(.+?)\)前', method_text)
                if not value_match1:
                    self.log("无法提取分组值列名")
                    return None

                group_cols = value_match1.group(1).split('+@')  # @A+@B分割为列表

                # 提取抽样值列名（@A+@B…）
                value_match2 = re.search(r'整体\(@(.+?)\)取绝对值后小于', method_text)
                if not value_match2:
                    self.log("无法提取抽样值列名")
                    return None

                sample_cols = value_match2.group(1).split('+@')

                # 提取阈值
                threshold_match = re.search(r'小于(\d+(\.\d+)?)', method_text)
                if not threshold_match:
                    self.log("无法提取阈值")
                    return None

                threshold = float(threshold_match.group(1))

                self.log(f"方法16参数: 分组列={group_col}, 分组值列={group_cols}, 抽样值列={sample_cols}, "
                         f"TopN={top_n}, 阈值={threshold}")
                return {
                    'type': 16,
                    'group_column': group_col,
                    'group_columns': group_cols,
                    'columns': sample_cols,
                    'top_n': top_n,
                    'threshold': threshold,
                    'comparison': 'less',
                }
            except Exception as e:
                self.log(f"解析方法16出错: {e}")
                import traceback
                self.log(traceback.format_exc())
            return None

        # 方法9，#（@A+@B…）取绝对值后大于XXX
        elif "取绝对值后大于" in method_text and not "每个@" in method_text and not "前" in method_text:
            self.log("检测到抽样方法“#（@A+@B…）取绝对值后大于XXX”，方法序号9，具体解释请查阅https://t.lwb.net.cn/web/#/660119647/120456986")
            # 提取列名部分
            cols_match = re.search(r'#\(@(.+?)\)取绝对值后大于', method_text)
            if not cols_match:
                self.log("无法提取列名")
                return None

            cols = cols_match.group(1).split('+@')
            # 提取阈值
            threshold_match = re.search(r'大于(\d+(\.\d+)?)', method_text)
            if not threshold_match:
                self.log("无法提取阈值")
                return None

            threshold = float(threshold_match.group(1))
            self.log(f"方法9参数: 列={cols}, 阈值={threshold}")
            return {
                'type': 9,
                'columns': cols,
                'threshold': threshold,
                'comparison': 'greater'
            }

        # 方法11，#每个@A（@B+@C…）取绝对值后大于XXX
        elif "取绝对值后大于" in method_text and "每个@" in method_text:
            self.log("检测到抽样方法“#每个@A（@B+@C…）取绝对值后大于XXX”，方法序号11，具体解释请查阅https://t.lwb.net.cn/web/#/660119647/120456986")
            # 提取分组列名
            group_match = re.search(r'#每个@(\w+)', method_text)
            if not group_match:
                self.log("无法提取分组列名")
                return None

            group_col = group_match.group(1)

            # 提取值列名
            value_match = re.search(r'\((.*?)\)', method_text)
            if not value_match:
                self.log("无法提取值列名")
                return None

            cols = [col.lstrip('@') for col in value_match.group(1).split('+')]

            # 提取阈值
            threshold_match = re.search(r'大于(\d+(\.\d+)?)', method_text)
            if not threshold_match:
                self.log("无法提取阈值")
                return None

            threshold = float(threshold_match.group(1))
            self.log(f"方法11参数: 分组列={group_col}, 值列={cols}, 阈值={threshold}")
            return {
                'type': 11,
                'group_column': group_col,
                'columns': cols,
                'threshold': threshold,
                'comparison': 'greater'
            }

        # 方法13，#（（@A+@B…）前X的@C）（@A+@B…）取绝对值后大于XXX
        elif "取绝对值后大于" in method_text and "前" in method_text and "的@" in method_text:
            self.log("检测到抽样方法“#（（@A+@B…）前X的@C）（@A+@B…）取绝对值后大于XXX”，方法序号13，具体解释请查阅https://t.lwb.net.cn/web/#/660119647/120456986")
            # 提取分组信息（前X的@C）
            group_match = re.search(r'前(\d+)的@(\w+)', method_text)
            if not group_match:
                self.log("无法提取分组信息")
                return None

            top_n = int(group_match.group(1))  # 前X的X
            group_col = group_match.group(2)  # @C的C

            # 提取分组值列名（（@A+@B…））
            value_match1 = re.search(r'#\(\(@(.+?)\)前', method_text)
            if not value_match1:
                self.log("无法提取分组值列名")
                return None

            group_cols = value_match1.group(1).split('+@')  # @A+@B分割为列表

            # 提取抽样值列名（@A+@B…）
            value_match2 = re.search(r'\)\(@(.+?)\)取绝对值后大于', method_text)
            if not value_match2:
                self.log("无法提取抽样值列名")
                return None

            sample_cols = value_match2.group(1).split('+@')

            # 提取阈值
            threshold_match = re.search(r'大于(\d+(\.\d+)?)', method_text)
            if not threshold_match:
                self.log("无法提取阈值")
                return None

            threshold = float(threshold_match.group(1))
            self.log(f"方法13参数: 分组列={group_col}, 分组值列={group_cols}, 抽样值列={sample_cols}, "
                     f"TopN={top_n}, 阈值={threshold}")
            return {
                'type': 13,
                'group_column': group_col,
                'group_columns': group_cols,
                'columns': sample_cols,
                'top_n': top_n,
                'threshold': threshold,
                'comparison': 'greater'
            }

        # 方法10，#（@A+@B…）取绝对值后小于XXX
        elif "取绝对值后小于" in method_text and not "每个@" in method_text and not "前" in method_text:
            self.log("检测到抽样方法“#（@A+@B…）取绝对值后小于XXX”，方法序号10，具体解释请查阅https://t.lwb.net.cn/web/#/660119647/120456986")
            # 提取列名部分
            cols_match = re.search(r'#\(@(.+?)\)取绝对值后小于', method_text)
            if not cols_match:
                self.log("无法提取列名")
                return None

            cols = cols_match.group(1).split('+@')
            # 提取阈值
            threshold_match = re.search(r'小于(\d+(\.\d+)?)', method_text)
            if not threshold_match:
                self.log("无法提取阈值")
                return None

            threshold = float(threshold_match.group(1))
            self.log(f"方法10参数: 列={cols}, 阈值={threshold}")
            return {
                'type': 10,
                'columns': cols,
                'threshold': threshold,
                'comparison': 'less'
            }

        # 方法12，#每个@A（@B+@C…）取绝对值后小于XXX
        elif "取绝对值后小于" in method_text and "每个@" in method_text:
            self.log("检测到抽样方法“#每个@A（@B+@C…）取绝对值后小于XXX”，方法序号12，具体解释请查阅https://t.lwb.net.cn/web/#/660119647/120456986")
            # 提取分组列名
            group_match = re.search(r'#每个@(\w+)', method_text)
            if not group_match:
                self.log("无法提取分组列名")
                return None

            group_col = group_match.group(1)

            # 提取值列名
            value_match = re.search(r'\((.*?)\)', method_text)
            if not value_match:
                self.log("无法提取值列名")
                return None

            cols = [col.lstrip('@') for col in value_match.group(1).split('+')]

            # 提取阈值
            threshold_match = re.search(r'小于(\d+(\.\d+)?)', method_text)
            if not threshold_match:
                self.log("无法提取阈值")
                return None

            threshold = float(threshold_match.group(1))
            self.log(f"方法12参数: 分组列={group_col}, 值列={cols}, 阈值={threshold}")
            return {
                'type': 12,
                'group_column': group_col,
                'columns': cols,
                'threshold': threshold,
                'comparison': 'less'
            }

        # 方法14，#（（@A+@B…）前X的@C）（@A+@B…）取绝对值后小于XXX
        elif "取绝对值后小于" in method_text and "前" in method_text and "的@" in method_text:
            self.log("检测到抽样方法“#（（@A+@B…）前X的@C）（@A+@B…）取绝对值后小于XXX”，方法序号14，具体解释请查阅https://t.lwb.net.cn/web/#/660119647/120456986")
            # 提取分组信息（前X的@C）
            group_match = re.search(r'前(\d+)的@(\w+)', method_text)
            if not group_match:
                self.log("无法提取分组信息")
                return None

            top_n = int(group_match.group(1))  # 前X的X
            group_col = group_match.group(2)  # @C的C

            # 提取分组值列名（（@A+@B…））
            value_match1 = re.search(r'#\(\(@(.+?)\)前', method_text)
            if not value_match1:
                self.log("无法提取分组值列名")
                return None

            group_cols = value_match1.group(1).split('+@')  # @A+@B分割为列表

            # 提取抽样值列名（@A+@B…）
            value_match2 = re.search(r'\)\(@(.+?)\)取绝对值后小于', method_text)
            if not value_match2:
                self.log("无法提取抽样值列名")
                return None

            sample_cols = value_match2.group(1).split('+@')

            # 提取阈值
            threshold_match = re.search(r'小于(\d+(\.\d+)?)', method_text)
            if not threshold_match:
                self.log("无法提取阈值")
                return None

            threshold = float(threshold_match.group(1))
            self.log(f"方法14参数: 分组列={group_col}, 分组值列={group_cols}, 抽样值列={sample_cols}, "
                     f"TopN={top_n}, 阈值={threshold}")
            return {
                'type': 14,
                'group_column': group_col,
                'group_columns': group_cols,
                'columns': sample_cols,
                'top_n': top_n,
                'threshold': threshold,
                'comparison': 'less'
            }

        # 方法5，#（（@A+@B…）前X的@C）（@A+@B…）取绝对值后每X月取前X
        elif method_text.startswith("#((") and "的@" in method_text and "取绝对值后每" in method_text:
            self.log("检测到抽样方法“#（（@A+@B…）前X的@C）（@A+@B…）取绝对值后每X月取前X”，方法序号5，具体解释请查阅https://t.lwb.net.cn/web/#/660119647/120456986")
            # 提取分组信息（前X的@C）
            group_match = re.search(r'前(\d+)的@(\w+)', method_text)
            if not group_match:
                self.log("无法提取分组信息")
                return None

            top_n1 = int(group_match.group(1))  # 前X的X
            group_col = group_match.group(2)  # @C的C

            # 提取分组值列名（（@A+@B…））
            value_match1 = re.search(r'#\(\(@(.+?)\)前', method_text)
            if not value_match1:
                self.log("无法提取分组值列名")
                return None

            group_cols = value_match1.group(1).split('+@')  # @A+@B分割为列表

            # 提取抽样值列名（@A+@B…）
            value_match2 = re.search(r'\)\(@(.+?)\)取绝对值后每', method_text)
            if not value_match2:
                self.log("无法提取抽样值列名")
                return None

            sample_cols = value_match2.group(1).split('+@')

            # 从"每"字后面开始提取数字
            numbers_match = re.search(r'每(\d+)月取前(\d+)', method_text)
            if not numbers_match:
                self.log("无法提取月份间隔和TopN值")
                return None

            month_interval = int(numbers_match.group(1))
            top_n2 = int(numbers_match.group(2))
            self.log(f"方法5参数: 分组列={group_col}, 分组值列={group_cols}, 抽样值列={sample_cols}, "
                  f"TopN1={top_n1}, 月间隔={month_interval}, TopN2={top_n2}")
            return {
                'type': 5,
                'group_column': group_col,
                'group_columns': group_cols,
                'columns': sample_cols,
                'top_n1': top_n1,
                'month_interval': month_interval,
                'top_n2': top_n2
            }

        # 方法6，#（（@A+@B…）前X的@C）（@A+@B…）取绝对值后的前X
        elif method_text.startswith("#((") and "的@" in method_text and "取绝对值后的前" in method_text:
            self.log("检测到抽样方法“#（（@A+@B…）前X的@C）（@A+@B…）取绝对值后的前X”，方法序号6，具体解释请查阅https://t.lwb.net.cn/web/#/660119647/120456986")
            # 提取分组信息（前X的@C）
            group_match = re.search(r'前(\d+)的@(\w+)', method_text)
            if not group_match:
                self.log("无法提取分组信息")
                return None

            top_n1 = int(group_match.group(1))  # 前X的X
            group_col = group_match.group(2)  # @C的C

            # 提取分组值列名（（@A+@B…））
            value_match1 = re.search(r'#\(\(@(.+?)\)前', method_text)
            if not value_match1:
                self.log("无法提取分组值列名")
                return None

            group_cols = value_match1.group(1).split('+@')  # @A+@B分割为列表

            # 提取抽样值列名（@A+@B…）
            value_match2 = re.search(r'\)\(@(.+?)\)取绝对值后的前', method_text)
            if not value_match2:
                self.log("无法提取抽样值列名")
                return None

            sample_cols = value_match2.group(1).split('+@')

            # 从"前"字后面提取数字
            top_n_match = re.search(r'后的前(\d+)', method_text)
            if not top_n_match:
                self.log("无法提取TopN值")
                return None

            top_n2 = int(top_n_match.group(1))
            self.log(f"方法6参数: 分组列={group_col}, 分组值列={group_cols}, 抽样值列={sample_cols}, "
                  f"TopN1={top_n1}, TopN2={top_n2}")
            return {
                'type': 6,
                'group_column': group_col,
                'group_columns': group_cols,
                'columns': sample_cols,
                'top_n1': top_n1,
                'top_n2': top_n2
            }

        # 方法3，#每个@A（@B+@C…）取绝对值后每X月取前X
        elif method_text.startswith("#每个@") and "取绝对值后每" in method_text:
            self.log("检测到抽样方法“#每个@A（@B+@C…）取绝对值后每X月取前X”，方法序号3，具体解释请查阅https://t.lwb.net.cn/web/#/660119647/120456986")
            # 提取分组列名（@A）
            group_match = re.search(r'#每个@(\w+)', method_text)
            if not group_match:
                self.log("无法提取分组列名")
                return None

            group_col = group_match.group(1)

            # 提取值列名（@B+@C…）
            value_match = re.search(r'\((.*?)\)', method_text)
            if not value_match:
                self.log("无法提取值列名")
                return None

            cols = [col.lstrip('@') for col in value_match.group(1).split('+')]

            # 从"每"字后面开始提取数字
            numbers_match = re.search(r'每(\d+)月取前(\d+)', method_text)
            if not numbers_match:
                self.log("无法提取月份间隔和TopN值")
                return None

            month_interval = int(numbers_match.group(1))
            top_n = int(numbers_match.group(2))
            self.log(f"方法3参数: 分组列={group_col}, 值列={cols}, 月间隔={month_interval}, TopN={top_n}")
            return {
                'type': 3,
                'group_column': group_col,
                'columns': cols,
                'month_interval': month_interval,
                'top_n': top_n
            }

        # 方法4，#每个@A（@B+@C…）取绝对值后的前X
        elif method_text.startswith("#每个@") and "取绝对值后的前" in method_text:
            self.log("检测到抽样方法“#每个@A（@B+@C…）取绝对值后的前X”，方法序号4，具体解释请查阅https://t.lwb.net.cn/web/#/660119647/120456986")
            # 提取分组列名（@A）
            group_match = re.search(r'#每个@(\w+)', method_text)
            if not group_match:
                self.log("无法提取分组列名")
                return None

            group_col = group_match.group(1)

            # 提取值列名（@B+@C…）
            value_match = re.search(r'\((.*?)\)', method_text)
            if not value_match:
                self.log("无法提取值列名")
                return None

            cols = [col.lstrip('@') for col in value_match.group(1).split('+')]

            # 从"前"字后面提取数字
            top_n_match = re.search(r'后的前(\d+)', method_text)
            if not top_n_match:
                self.log("无法提取TopN值")
                return None

            top_n = int(top_n_match.group(1))
            self.log(f"方法4参数: 分组列={group_col}, 值列={cols}, TopN={top_n}")
            return {
                'type': 4,
                'group_column': group_col,
                'columns': cols,
                'top_n': top_n
            }

        # 方法1，#（@A+@B…）取绝对值后每X月取前X
        elif "取绝对值后每" in method_text:
            self.log("检测到抽样方法“#（@A+@B…）取绝对值后每X月取前X”，方法序号1，具体解释请查阅https://t.lwb.net.cn/web/#/660119647/120456986")
            # 使用正则表达式提取列名部分（括号内的@A+@B…）
            cols_match = re.search(r'#\(@(.+?)\)取绝对值后每', method_text)
            if not cols_match:
                self.log("无法提取列名")
                return None

            # 将列名分割为列表（如@A+@B分割为['A','B']）
            cols = cols_match.group(1).split('+@')

            # 从"每"字后面开始提取数字
            numbers_match = re.search(r'每(\d+)月取前(\d+)', method_text)
            if not numbers_match:
                self.log("无法提取月份间隔和TopN值")
                return None

            month_interval = int(numbers_match.group(1))
            top_n = int(numbers_match.group(2))

            self.log(f"方法1参数: 列={cols}, 月间隔={month_interval}, TopN={top_n}")
            return {
                'type': 1,
                'columns': cols,
                'month_interval': month_interval,
                'top_n': top_n
            }

        # 方法2，#（@A+@B…）取绝对值后的前X
        elif "取绝对值后的前" in method_text:
            self.log("检测到抽样方法“#（@A+@B…）取绝对值后的前X”，方法序号2，具体解释请查阅https://t.lwb.net.cn/web/#/660119647/120456986")
            # 使用正则表达式提取列名部分
            cols_match = re.search(r'#\(@(.+?)\)取绝对值后的前', method_text)
            if not cols_match:
                self.log("无法提取列名")
                return None

            # 将列名分割为列表
            cols = cols_match.group(1).split('+@')

            # 从"前"字后面提取数字
            top_n_match = re.search(r'后的前(\d+)', method_text)
            if not top_n_match:
                self.log("无法提取TopN值")
                return None

            top_n = int(top_n_match.group(1))
            self.log(f"方法2参数: 列={cols}, TopN={top_n}")
            return {
                'type': 2,
                'columns': cols,
                'top_n': top_n
            }

        self.log("无法识别的抽样方法")
        return None

    def apply_conditions(self, df, conditions):
        """应用筛选条件到DataFrame - 重构版"""
        if df.empty:
            self.log("数据框为空，跳过条件应用")
            return df

        self.log(f"应用条件到数据框，原始行数: {len(df)}")
        filtered_df = df.copy()

        for rule in conditions:
            # 应用开始时间筛选
            for date_rule in rule.get('start_dates', []):
                col = date_rule['column']
                if col in filtered_df:
                    start_date = date_rule['value']
                    try:
                        if not pd.api.types.is_datetime64_any_dtype(filtered_df[col]):
                            self.log(f"转换日期列 '{col}' 为日期类型")
                            filtered_df[col] = pd.to_datetime(filtered_df[col], errors='coerce')
                        self.log(f"应用开始时间筛选: {col} >= {start_date.strftime('%Y-%m-%d')}")
                        filtered_df = filtered_df[filtered_df[col] >= start_date]
                        self.log(f"筛选后行数: {len(filtered_df)}")
                    except Exception as e:
                        self.log(f"开始日期筛选错误: {e}")

            # 应用结束时间筛选
            for date_rule in rule.get('end_dates', []):
                col = date_rule['column']
                if col in filtered_df:
                    end_date = date_rule['value']
                    try:
                        if not pd.api.types.is_datetime64_any_dtype(filtered_df[col]):
                            self.log(f"转换日期列 '{col}' 为日期类型")
                            filtered_df[col] = pd.to_datetime(filtered_df[col], errors='coerce')
                        self.log(f"应用结束时间筛选: {col} <= {end_date.strftime('%Y-%m-%d')}")
                        filtered_df = filtered_df[filtered_df[col] <= end_date]
                        self.log(f"筛选后行数: {len(filtered_df)}")
                    except Exception as e:
                        self.log(f"结束日期筛选错误: {e}")

            # 应用剔除条件
            for exclude_rule in rule.get('excludes', []):
                col = exclude_rule['column']
                cond_type = exclude_rule['type']
                value = exclude_rule['value']
                self.log(f"应用剔除条件: 列={col}, 类型={cond_type}, 值={value}")
                filtered_df = self._apply_single_condition(filtered_df, col, cond_type, value, exclude=True)
                self.log(f"剔除后行数: {len(filtered_df)}")

            # 应用筛选条件
            for include_rule in rule.get('includes', []):
                col = include_rule['column']
                cond_type = include_rule['type']
                value = include_rule['value']
                self.log(f"应用筛选条件: 列={col}, 类型={cond_type}, 值={value}")
                filtered_df = self._apply_single_condition(filtered_df, col, cond_type, value, exclude=False)
                self.log(f"筛选后行数: {len(filtered_df)}")

            for startswith_rule in rule.get('startswiths', []):
                col = startswith_rule['column']
                cond_type = startswith_rule['type']
                value = startswith_rule['value']
                self.log(f"应用开头为条件: 列={col}, 类型={cond_type}, 值={value}")
                filtered_df = self._apply_single_condition(filtered_df, col, cond_type, value, exclude=False)
                self.log(f"筛选后行数: {len(filtered_df)}")

        self.log(f"所有条件应用完成，最终行数: {len(filtered_df)}")
        return filtered_df

    def _apply_single_condition(self, df, col, cond_type, value, exclude=False):
        """应用单个条件到DataFrame - 支持包含/剔除"""
        if col not in df:
            self.log(f"列 '{col}' 不存在于数据框中，跳过条件")
            return df

        action = "剔除" if exclude else "筛选"
        self.log(f"应用{action}条件: 列={col}, 类型={cond_type}")

        # 处理多个条件
        if cond_type == 'multiple':
            self.log("处理多个条件")
            for cond in value:
                df = self._apply_condition(df, col, cond[0], cond[1], exclude)
        else:
            df = self._apply_condition(df, col, cond_type, value, exclude)

        return df

    def _apply_condition(self, df, col, cond_type, value, exclude):
        """应用单个条件逻辑"""
        action = "剔除" if exclude else "筛选"
        self.log(f"应用{action}条件: 列={col}, 类型={cond_type}, 值={value}")

        if exclude:
            operator = lambda x: ~x
        else:
            operator = lambda x: x

        try:
            if cond_type == 'value':
                self.log(f"应用值条件: 包含 '{value}'")
                condition = df[col].astype(str).str.contains(str(value), na=False)
            elif cond_type == 'list':
                self.log(f"应用列表条件: 包含 {value} 中的任意值")
                pattern = '|'.join(map(re.escape, value))
                condition = df[col].astype(str).str.contains(pattern, na=False)
            elif cond_type == 'exclude_related':
                A, B = value
                self.log(f"应用排除相关条件: 包含 '{A}' 但不包含 '{B}'")
                condition = (df[col].str.contains(A, na=False)) & (~df[col].str.contains(B, na=False))
            elif cond_type == 'equal':
                try:
                    self.log(f"应用数值等于条件: = {value}")
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    value_num = float(value)
                    condition = np.isclose(df[col], value_num, rtol=1e-5, atol=1e-5)
                except Exception:
                    self.log(f"数值转换失败，使用字符串等于条件")
                    condition = df[col].astype(str) == value
            elif cond_type == 'not_equal':
                try:
                    self.log(f"应用数值不等于条件: != {value}")
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    value_num = float(value)
                    condition = ~np.isclose(df[col], value_num, rtol=1e-5, atol=1e-5)
                except Exception:
                    self.log(f"数值转换失败，使用字符串不等于条件")
                    condition = df[col].astype(str) != value
            elif cond_type == 'greater':
                self.log(f"应用大于条件: > {value}")
                df[col] = pd.to_numeric(df[col], errors='coerce')
                value_num = float(value)
                condition = df[col] > value_num
            elif cond_type == 'less':
                self.log(f"应用小于条件: < {value}")
                df[col] = pd.to_numeric(df[col], errors='coerce')
                value_num = float(value)
                condition = df[col] < value_num
            elif cond_type == 'startswith':
                self.log(f"应用开头为条件: 以 '{value}' 开头")
                condition = df[col].astype(str).str.startswith(str(value))
            else:
                self.log(f"未知条件类型: {cond_type}")
                return df

            return df[operator(condition)]
        except Exception as e:
            self.log(f"应用条件错误: {e}")
            return df

    def apply_sampling_method(self, df, method, rule):
        """应用抽样方法 - 增强版"""
        if method is None or df.empty:
            self.log("抽样方法为空或数据框为空，跳过抽样")
            return df

        self.log(f"应用抽样方法类型 {method['type']}")

        # 确定日期列
        date_column = None
        if rule.get('start_dates'):
            date_column = rule['start_dates'][0]['column']
            self.log(f"使用开始日期列: {date_column}")
        elif rule.get('end_dates'):
            date_column = rule['end_dates'][0]['column']
            self.log(f"使用结束日期列: {date_column}")

        # 方法1， #（@A+@B…）取绝对值后每X月取前X
        if method['type'] == 1:
            self.log(f"应用抽样方法1: 列={method['columns']}, 月间隔={method['month_interval']}, TopN={method['top_n']}")
            # 计算绝对值列
            abs_col = '绝对值'
            df[abs_col] = 0
            for col in method['columns']:
                if col in df:
                    self.log(f"处理列: {col}")
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                    df[abs_col] += df[col]

            df[abs_col] = df[abs_col].abs()

            # 提取月份分组 - 使用日期列而不是索引
            month_col = '抽样月份分组'
            if date_column and date_column in df.columns:
                # 确保日期列是日期类型
                if not pd.api.types.is_datetime64_any_dtype(df[date_column]):
                    self.log(f"转换日期列 '{date_column}' 为日期类型")
                    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')

                # 使用日期列创建月份分组
                df[month_col] = df[date_column].apply(
                    lambda x: f"{x.year}-{math.ceil(x.month / method['month_interval'])}" if pd.notnull(x) else "缺失日期"
                )
                self.log(f"创建月份分组列: {month_col}")
            else:
                self.log(f"错误: 日期列 '{date_column}' 不存在")
                return df

            # 按月份分组取TopN
            sampled = df.groupby(month_col, group_keys=False).apply(
                lambda x: x.nlargest(method['top_n'], abs_col)
            )
            self.log(f"抽样完成，抽样行数: {len(sampled)}")

            return sampled.drop(columns=[month_col])

        # 方法2， #（@A+@B…）取绝对值后的前X
        elif method['type'] == 2:
            self.log(f"应用抽样方法2: 列={method['columns']}, TopN={method['top_n']}")
            # 计算绝对值列
            abs_col = '绝对值'
            df[abs_col] = 0
            for col in method['columns']:
                if col in df:
                    self.log(f"处理列: {col}")
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                    df[abs_col] += df[col]

            df[abs_col] = df[abs_col].abs()

            # 取TopN
            sampled = df.nlargest(method['top_n'], abs_col)
            self.log(f"抽样完成，抽样行数: {len(sampled)}")
            return sampled

        # 方法3， #每个@A（@B+@C…）取绝对值后每X月取前X
        elif method['type'] == 3:
            self.log(f"应用抽样方法3: 分组列={method['group_column']}, 列={method['columns']}, "
                  f"月间隔={method['month_interval']}, TopN={method['top_n']}")
            # 计算绝对值列
            abs_col = '绝对值'
            df[abs_col] = 0
            for col in method['columns']:
                if col in df:
                    self.log(f"处理列: {col}")
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                    df[abs_col] += df[col]

            df[abs_col] = df[abs_col].abs()

            # 提取月份分组 - 使用日期列而不是索引
            month_col = '抽样月份分组'
            if date_column and date_column in df.columns:
                # 确保日期列是日期类型
                if not pd.api.types.is_datetime64_any_dtype(df[date_column]):
                    self.log(f"转换日期列 '{date_column}' 为日期类型")
                    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')

                # 使用日期列创建月份分组
                df[month_col] = df[date_column].apply(
                    lambda x: f"{x.year}-{math.ceil(x.month / method['month_interval'])}" if pd.notnull(x) else "缺失日期"
                )
                self.log(f"创建月份分组列: {month_col}")
            else:
                self.log(f"错误: 日期列 '{date_column}' 不存在")
                return df

            # 按分组列和月份分组取TopN
            sampled = df.groupby([method['group_column'], month_col], group_keys=False).apply(
                lambda x: x.nlargest(method['top_n'], abs_col)
            )
            self.log(f"抽样完成，抽样行数: {len(sampled)}")
            return sampled.drop(columns=[month_col])

        # 方法4， #每个@A（@B+@C…）取绝对值后的前X
        elif method['type'] == 4:
            self.log(f"应用抽样方法4: 分组列={method['group_column']}, 列={method['columns']}, TopN={method['top_n']}")
            # 计算绝对值列
            abs_col = '绝对值'
            df[abs_col] = 0
            for col in method['columns']:
                if col in df:
                    self.log(f"处理列: {col}")
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                    df[abs_col] += df[col]

            df[abs_col] = df[abs_col].abs()

            # 按分组列取组内TopN
            sampled = df.groupby(method['group_column'], group_keys=False).apply(
                lambda x: x.nlargest(method['top_n'], abs_col)
            )
            self.log(f"抽样完成，抽样行数: {len(sampled)}")
            return sampled

        # 方法5， #（（@A+@B…）前X的@C）（@A+@B…）取绝对值后每X月取前X
        elif method['type'] == 5:
            group_column = method['group_column']
            self.log(f"应用抽样方法5: 分组列={group_column}, 分组值列={method['group_columns']}, "
                     f"抽样值列={method['columns']}, TopN1={method['top_n1']}, "
                     f"月间隔={method['month_interval']}, TopN2={method['top_n2']}")

            # 1. 计算每个分组的总和
            temp_col = '临时值'
            df[temp_col] = 0
            for col in method['group_columns']:
                if col in df:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                    df[temp_col] += df[col]

            # 计算每个分组的总和（绝对值）
            group_totals = df.groupby(group_column)[temp_col].sum().abs()
            self.log(f"{group_column}总和:\n{group_totals}")

            # 取TopN1的分组
            top_groups = group_totals.nlargest(method['top_n1']).index.tolist()
            self.log(f"Top {method['top_n1']} {group_column}: {top_groups}")

            # 2. 对每个分组单独进行抽样
            sampled_dfs = []  # 存储每个分组的抽样结果

            for group in top_groups:
                self.log(f"\n=== 处理{group_column}: {group} ===")
                # 筛选当前分组的数据
                group_df = df[df[group_column] == group].copy()
                self.log(f"该{group_column}数据行数: {len(group_df)}")

                if group_df.empty:
                    self.log(f"警告: {group} 无数据")
                    continue

                # 计算抽样用的绝对值列
                abs_col = '绝对值'
                group_df[abs_col] = 0
                for col in method['columns']:
                    if col in group_df:
                        group_df[col] = pd.to_numeric(group_df[col], errors='coerce').fillna(0)
                        group_df[abs_col] += group_df[col]

                group_df[abs_col] = group_df[abs_col].abs()
                self.log(f"计算绝对值列完成")

                # 创建月份分组
                month_col = '抽样月份分组'
                if date_column and date_column in group_df.columns:
                    # 确保日期列是日期类型
                    if not pd.api.types.is_datetime64_any_dtype(group_df[date_column]):
                        self.log(f"转换日期列 '{date_column}' 为日期类型")
                        group_df[date_column] = pd.to_datetime(group_df[date_column], errors='coerce')

                    # 创建每X个月的分组
                    group_df[month_col] = group_df[date_column].apply(
                        lambda x: f"{x.year}-{math.ceil(x.month / method['month_interval'])}"
                        if pd.notnull(x) else "缺失日期"
                    )

                    # 记录月份分组分布
                    month_counts = group_df[month_col].value_counts()
                    self.log(f"月份分组分布:\n{month_counts}")
                else:
                    self.log(f"错误: 日期列 '{date_column}' 不存在")
                    continue

                # 按月份分组取TopN2
                sampled_group = group_df.groupby(month_col, group_keys=False).apply(
                    lambda x: x.nlargest(method['top_n2'], abs_col)
                )

                # 添加分组标记（便于追踪）
                # sampled_group['分组标记'] = group
                # self.log(f"抽样到 {len(sampled_group)} 条记录")

                sampled_dfs.append(sampled_group)

            # 合并所有分组的抽样结果
            if sampled_dfs:
                sampled = pd.concat(sampled_dfs, ignore_index=True)
                self.log(f"总抽样行数: {len(sampled)}")

                # 删除临时列
                if temp_col in sampled:
                    sampled = sampled.drop(columns=[temp_col])
                if month_col in sampled:
                    sampled = sampled.drop(columns=[month_col])

                return sampled
            else:
                self.log("警告: 未抽样到任何记录")
                return pd.DataFrame()

        # 方法6， #（（@A+@B…）前X的@C）（@A+@B…）取绝对值后的前X
        elif method['type'] == 6:
            group_column = method['group_column']
            self.log(f"应用抽样方法6: 分组列={method['group_column']}, 分组值列={method['group_columns']}, "
                  f"抽样值列={method['columns']}, TopN1={method['top_n1']}, TopN2={method['top_n2']}")

            # 1. 计算每个分组的总和
            temp_col = '临时值'
            df[temp_col] = 0
            for col in method['group_columns']:
                if col in df:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                    df[temp_col] += df[col]

            # 计算每个分组的总和（绝对值）
            group_totals = df.groupby(group_column)[temp_col].sum().abs()
            self.log(f"{group_column}总和:\n{group_totals}")

            # 取TopN1的分组
            top_groups = group_totals.nlargest(method['top_n1']).index.tolist()
            self.log(f"Top {method['top_n1']} {group_column}: {top_groups}")

            # 2. 对每个分组单独进行抽样
            sampled_dfs = []  # 存储每个分组的抽样结果
            abs_col = '绝对值'  # 用于存储计算后的绝对值

            for group in top_groups:
                self.log(f"\n=== 处理{group_column}: {group} ===")
                # 筛选当前分组的数据
                group_df = df[df[group_column] == group].copy()
                self.log(f"该{group_column}数据行数: {len(group_df)}")

                if group_df.empty:
                    self.log(f"警告: {group} 无数据")
                    continue

                # 计算抽样用的绝对值列
                group_df[abs_col] = 0
                for col in method['columns']:
                    if col in group_df:
                        group_df[col] = pd.to_numeric(group_df[col], errors='coerce').fillna(0)
                        group_df[abs_col] += group_df[col]

                group_df[abs_col] = group_df[abs_col].abs()
                self.log(f"计算绝对值列完成")

                # 直接取分组的前TopN2（不再按月分组）
                sampled_group = group_df.nlargest(method['top_n2'], abs_col)
                self.log(f"抽样到 {len(sampled_group)} 条记录")

                sampled_dfs.append(sampled_group)

            # 合并所有分组的抽样结果
            if sampled_dfs:
                sampled = pd.concat(sampled_dfs, ignore_index=True)
                self.log(f"总抽样行数: {len(sampled)}")

                # 删除临时列
                columns_to_drop = [temp_col, abs_col]
                for col in columns_to_drop:
                    if col in sampled.columns:
                        sampled = sampled.drop(columns=col)

                return sampled
            else:
                self.log("警告: 未抽样到任何记录")
                return pd.DataFrame()

        # 方法7， #（（@A+@B…）前X的@C）整体（@A+@B…）取绝对值后每X月取前X
        elif method['type'] == 7:
            group_column = method['group_column']
            self.log(f"应用抽样方法7: 分组列={group_column}, 分组值列={method['group_columns']}, "
                     f"抽样值列={method['columns']}, TopN={method['top_n']}, "
                     f"月间隔={method['month_interval']}, 样本TopN={method['sample_top_n']}")

            # 1. 计算每个分组的总和
            temp_col = '临时值'
            df[temp_col] = 0
            for col in method['group_columns']:
                if col in df:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                    df[temp_col] += df[col]

            # 计算每个分组的总和（绝对值）
            group_totals = df.groupby(group_column)[temp_col].sum().abs()
            self.log(f"{group_column}总和:\n{group_totals}")

            # 取TopN的分组
            top_groups = group_totals.nlargest(method['top_n']).index.tolist()
            self.log(f"Top {method['top_n']} {group_column}: {top_groups}")

            # 2. 整体抽样：将所有选中的分组数据合并为一个整体
            whole_sample = pd.DataFrame()
            for group in top_groups:
                group_df = df[df[group_column] == group].copy()
                if not group_df.empty:
                    whole_sample = pd.concat([whole_sample, group_df], ignore_index=True)

            self.log(f"整体样本行数: {len(whole_sample)}")

            if whole_sample.empty:
                self.log("警告: 整体样本为空")
                return pd.DataFrame()

            # 3. 在整体样本上进行抽样
            # 计算抽样用的绝对值列
            abs_col = '绝对值'
            whole_sample[abs_col] = 0
            for col in method['columns']:
                if col in whole_sample:
                    whole_sample[col] = pd.to_numeric(whole_sample[col], errors='coerce').fillna(0)
                    whole_sample[abs_col] += whole_sample[col]

            whole_sample[abs_col] = whole_sample[abs_col].abs()
            self.log(f"计算绝对值列完成")

            # 创建月份分组
            month_col = '抽样月份分组'
            if date_column and date_column in whole_sample.columns:
                # 确保日期列是日期类型
                if not pd.api.types.is_datetime64_any_dtype(whole_sample[date_column]):
                    self.log(f"转换日期列 '{date_column}' 为日期类型")
                    whole_sample[date_column] = pd.to_datetime(whole_sample[date_column], errors='coerce')

                # 创建每X个月的分组
                whole_sample[month_col] = whole_sample[date_column].apply(
                    lambda x: f"{x.year}-{math.ceil(x.month / method['month_interval'])}"
                    if pd.notnull(x) else "缺失日期"
                )

                # 记录月份分组分布
                month_counts = whole_sample[month_col].value_counts()
                self.log(f"月份分组分布:\n{month_counts}")
            else:
                self.log(f"错误: 日期列 '{date_column}' 不存在")
                return whole_sample.drop(columns=[temp_col, abs_col], errors='ignore')

            # 按月份分组取TopN
            sampled = whole_sample.groupby(month_col, group_keys=False).apply(
                lambda x: x.nlargest(method['sample_top_n'], abs_col)
            )

            self.log(f"抽样完成，抽样行数: {len(sampled)}")

            # 删除临时列
            columns_to_drop = [temp_col, abs_col, month_col]
            for col in columns_to_drop:
                if col in sampled.columns:
                    sampled = sampled.drop(columns=col)

            return sampled

        # 方法8， #（（@A+@B…）前X的@C）整体（@A+@B…）取绝对值后的前X
        elif method['type'] == 8:
            group_column = method['group_column']
            self.log(f"应用抽样方法8: 分组列={group_column}, 分组值列={method['group_columns']}, "
                     f"抽样值列={method['columns']}, TopN={method['top_n']}, 样本TopN={method['sample_top_n']}")

            # 1. 计算每个分组的总和
            temp_col = '临时值'
            df[temp_col] = 0
            for col in method['group_columns']:
                if col in df:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                    df[temp_col] += df[col]

            # 计算每个分组的总和（绝对值）
            group_totals = df.groupby(group_column)[temp_col].sum().abs()
            self.log(f"{group_column}总和:\n{group_totals}")

            # 取TopN的分组
            top_groups = group_totals.nlargest(method['top_n']).index.tolist()
            self.log(f"Top {method['top_n']} {group_column}: {top_groups}")

            # 2. 整体抽样：将所有选中的分组数据合并为一个整体
            whole_sample = pd.DataFrame()
            for group in top_groups:
                group_df = df[df[group_column] == group].copy()
                if not group_df.empty:
                    whole_sample = pd.concat([whole_sample, group_df], ignore_index=True)

            self.log(f"整体样本行数: {len(whole_sample)}")

            if whole_sample.empty:
                self.log("警告: 整体样本为空")
                return pd.DataFrame()

            # 3. 在整体样本上进行抽样
            # 计算抽样用的绝对值列
            abs_col = '绝对值'
            whole_sample[abs_col] = 0
            for col in method['columns']:
                if col in whole_sample:
                    whole_sample[col] = pd.to_numeric(whole_sample[col], errors='coerce').fillna(0)
                    whole_sample[abs_col] += whole_sample[col]

            whole_sample[abs_col] = whole_sample[abs_col].abs()
            self.log(f"计算绝对值列完成")

            # 取前X
            sampled = whole_sample.nlargest(method['sample_top_n'], abs_col)
            self.log(f"抽样完成，抽样行数: {len(sampled)}")

            # 删除临时列
            columns_to_drop = [temp_col, abs_col]
            for col in columns_to_drop:
                if col in sampled.columns:
                    sampled = sampled.drop(columns=col)

            return sampled

        # 方法9， #（@A+@B…）取绝对值后大于XXX
        elif method['type'] == 9:
            self.log(f"应用抽样方法9: 列={method['columns']}, 阈值={method['threshold']}")
            # 计算绝对值列
            abs_col = '绝对值'
            df[abs_col] = 0
            for col in method['columns']:
                if col in df:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                    df[abs_col] += df[col]

            df[abs_col] = df[abs_col].abs()

            # 筛选大于阈值的记录
            sampled = df[df[abs_col] > method['threshold']]
            self.log(f"抽样完成，抽样行数: {len(sampled)}")
            return sampled

        # 方法10， #每个@A（@B+@C…）取绝对值后大于XXX
        elif method['type'] == 10:
            self.log(
                f"应用抽样方法10: 分组列={method['group_column']}, 列={method['columns']}, 阈值={method['threshold']}")
            # 计算绝对值列
            abs_col = '绝对值'
            df[abs_col] = 0
            for col in method['columns']:
                if col in df:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                    df[abs_col] += df[col]

            df[abs_col] = df[abs_col].abs()

            # 按分组筛选大于阈值的记录
            sampled = df.groupby(method['group_column'], group_keys=False).apply(
                lambda x: x[x[abs_col] > method['threshold']]
            )
            self.log(f"抽样完成，抽样行数: {len(sampled)}")
            return sampled

        # 方法11， #（（@A+@B…）前X的@C）（@A+@B…）取绝对值后大于XXX
        elif method['type'] == 11:
            group_column = method['group_column']
            self.log(f"应用抽样方法11: 分组列={group_column}, 分组值列={method['group_columns']}, "
                     f"抽样值列={method['columns']}, TopN={method['top_n']}, 阈值={method['threshold']}")

            # 1. 计算每个分组的总和
            temp_col = '临时值'
            df[temp_col] = 0
            for col in method['group_columns']:
                if col in df:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                    df[temp_col] += df[col]

            # 计算每个分组的总和（绝对值）
            group_totals = df.groupby(group_column)[temp_col].sum().abs()
            self.log(f"{group_column}总和:\n{group_totals}")

            # 取TopN的分组
            top_groups = group_totals.nlargest(method['top_n']).index.tolist()
            self.log(f"Top {method['top_n']} {group_column}: {top_groups}")

            # 2. 对每个分组筛选大于阈值的记录
            sampled_dfs = []  # 存储每个分组的抽样结果

            for group in top_groups:
                self.log(f"\n=== 处理{group_column}: {group} ===")
                # 筛选当前分组的数据
                group_df = df[df[group_column] == group].copy()
                self.log(f"该{group_column}数据行数: {len(group_df)}")

                if group_df.empty:
                    self.log(f"警告: {group} 无数据")
                    continue

                # 计算抽样用的绝对值列
                abs_col = '绝对值'
                group_df[abs_col] = 0
                for col in method['columns']:
                    if col in group_df:
                        group_df[col] = pd.to_numeric(group_df[col], errors='coerce').fillna(0)
                        group_df[abs_col] += group_df[col]

                group_df[abs_col] = group_df[abs_col].abs()
                self.log(f"计算绝对值列完成")

                # 筛选大于阈值的记录
                sampled_group = group_df[group_df[abs_col] > method['threshold']]
                self.log(f"筛选到 {len(sampled_group)} 条记录")

                sampled_dfs.append(sampled_group)

            # 合并所有分组的抽样结果
            if sampled_dfs:
                sampled = pd.concat(sampled_dfs, ignore_index=True)
                self.log(f"总抽样行数: {len(sampled)}")
                return sampled
            else:
                self.log("警告: 未抽样到任何记录")
                return pd.DataFrame()

        # 方法12， #（@A+@B…）取绝对值后小于XXX
        elif method['type'] == 12:
            self.log(f"应用抽样方法12: 列={method['columns']}, 阈值={method['threshold']}")
            # 计算绝对值列
            abs_col = '绝对值'
            df[abs_col] = 0
            for col in method['columns']:
                if col in df:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                    df[abs_col] += df[col]

            df[abs_col] = df[abs_col].abs()

            # 筛选小于阈值的记录
            sampled = df[df[abs_col] < method['threshold']]
            self.log(f"抽样完成，抽样行数: {len(sampled)}")
            return sampled

        # 方法13， #每个@A（@B+@C…）取绝对值后小于XXX
        elif method['type'] == 13:
            self.log(
                f"应用抽样方法13: 分组列={method['group_column']}, 列={method['columns']}, 阈值={method['threshold']}")
            # 计算绝对值列
            abs_col = '绝对值'
            df[abs_col] = 0
            for col in method['columns']:
                if col in df:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                    df[abs_col] += df[col]

            df[abs_col] = df[abs_col].abs()

            # 按分组筛选小于阈值的记录
            sampled = df.groupby(method['group_column'], group_keys=False).apply(
                lambda x: x[x[abs_col] < method['threshold']]
            )
            self.log(f"抽样完成，抽样行数: {len(sampled)}")
            return sampled

        # 方法14， #（（@A+@B…）前X的@C）（@A+@B…）取绝对值后小于XXX
        elif method['type'] == 14:
            group_column = method['group_column']
            self.log(f"应用抽样方法14: 分组列={group_column}, 分组值列={method['group_columns']}, "
                     f"抽样值列={method['columns']}, TopN={method['top_n']}, 阈值={method['threshold']}")

            # 1. 计算每个分组的总和
            temp_col = '临时值'
            df[temp_col] = 0
            for col in method['group_columns']:
                if col in df:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                    df[temp_col] += df[col]

            # 计算每个分组的总和（绝对值）
            group_totals = df.groupby(group_column)[temp_col].sum().abs()
            self.log(f"{group_column}总和:\n{group_totals}")

            # 取TopN的分组
            top_groups = group_totals.nlargest(method['top_n']).index.tolist()
            self.log(f"Top {method['top_n']} {group_column}: {top_groups}")

            # 2. 对每个分组筛选小于阈值的记录
            sampled_dfs = []  # 存储每个分组的抽样结果

            for group in top_groups:
                self.log(f"\n=== 处理{group_column}: {group} ===")
                # 筛选当前分组的数据
                group_df = df[df[group_column] == group].copy()
                self.log(f"该{group_column}数据行数: {len(group_df)}")

                if group_df.empty:
                    self.log(f"警告: {group} 无数据")
                    continue

                # 计算抽样用的绝对值列
                abs_col = '绝对值'
                group_df[abs_col] = 0
                for col in method['columns']:
                    if col in group_df:
                        group_df[col] = pd.to_numeric(group_df[col], errors='coerce').fillna(0)
                        group_df[abs_col] += group_df[col]

                group_df[abs_col] = group_df[abs_col].abs()
                self.log(f"计算绝对值列完成")

                # 筛选小于阈值的记录
                sampled_group = group_df[group_df[abs_col] < method['threshold']]
                self.log(f"筛选到 {len(sampled_group)} 条记录")

                sampled_dfs.append(sampled_group)

            # 合并所有分组的抽样结果
            if sampled_dfs:
                sampled = pd.concat(sampled_dfs, ignore_index=True)
                self.log(f"总抽样行数: {len(sampled)}")
                return sampled
            else:
                self.log("警告: 未抽样到任何记录")
                return pd.DataFrame()

        # 方法15， #（（@A+@B…）前X的@C）整体（@A+@B…）取绝对值后大于XXX
        elif method['type'] == 15:
            group_column = method['group_column']
            self.log(f"应用抽样方法15: 分组列={group_column}, 分组值列={method['group_columns']}, "
                     f"抽样值列={method['columns']}, TopN={method['top_n']}, 阈值={method['threshold']}")

            # 1. 计算每个分组的总和
            temp_col = '临时值'
            df[temp_col] = 0
            for col in method['group_columns']:
                if col in df:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                    df[temp_col] += df[col]

            # 计算每个分组的总和（绝对值）
            group_totals = df.groupby(group_column)[temp_col].sum().abs()
            self.log(f"{group_column}总和:\n{group_totals}")

            # 取TopN的分组
            top_groups = group_totals.nlargest(method['top_n']).index.tolist()
            self.log(f"Top {method['top_n']} {group_column}: {top_groups}")

            # 2. 整体抽样：将所有选中的分组数据合并为一个整体
            whole_sample = pd.DataFrame()
            for group in top_groups:
                group_df = df[df[group_column] == group].copy()
                if not group_df.empty:
                    whole_sample = pd.concat([whole_sample, group_df], ignore_index=True)

            self.log(f"整体样本行数: {len(whole_sample)}")

            if whole_sample.empty:
                self.log("警告: 整体样本为空")
                return pd.DataFrame()

            # 3. 在整体样本上进行抽样
            # 计算抽样用的绝对值列
            abs_col = '绝对值'
            whole_sample[abs_col] = 0
            for col in method['columns']:
                if col in whole_sample:
                    whole_sample[col] = pd.to_numeric(whole_sample[col], errors='coerce').fillna(0)
                    whole_sample[abs_col] += whole_sample[col]

            whole_sample[abs_col] = whole_sample[abs_col].abs()
            self.log(f"计算绝对值列完成")

            # 筛选大于阈值的记录
            sampled = whole_sample[whole_sample[abs_col] > method['threshold']]
            self.log(f"抽样完成，抽样行数: {len(sampled)}")

            # 删除临时列
            columns_to_drop = [temp_col, abs_col]
            for col in columns_to_drop:
                if col in sampled.columns:
                    sampled = sampled.drop(columns=col)

            return sampled

            # 方法16， #（（@A+@B…）前X的@C）整体（@A+@B…）取绝对值后小于XXX
        elif method['type'] == 16:
            group_column = method['group_column']
            self.log(f"应用抽样方法16: 分组列={group_column}, 分组值列={method['group_columns']}, "
                     f"抽样值列={method['columns']}, TopN={method['top_n']}, 阈值={method['threshold']}")

            # 1. 计算每个分组的总和
            temp_col = '临时值'
            df[temp_col] = 0
            for col in method['group_columns']:
                if col in df:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                    df[temp_col] += df[col]

            # 计算每个分组的总和（绝对值）
            group_totals = df.groupby(group_column)[temp_col].sum().abs()
            self.log(f"{group_column}总和:\n{group_totals}")

            # 取TopN的分组
            top_groups = group_totals.nlargest(method['top_n']).index.tolist()
            self.log(f"Top {method['top_n']} {group_column}: {top_groups}")

            # 2. 整体抽样：将所有选中的分组数据合并为一个整体
            whole_sample = pd.DataFrame()
            for group in top_groups:
                group_df = df[df[group_column] == group].copy()
                if not group_df.empty:
                    whole_sample = pd.concat([whole_sample, group_df], ignore_index=True)

            self.log(f"整体样本行数: {len(whole_sample)}")

            if whole_sample.empty:
                self.log("警告: 整体样本为空")
                return pd.DataFrame()

            # 3. 在整体样本上进行抽样
            # 计算抽样用的绝对值列
            abs_col = '绝对值'
            whole_sample[abs_col] = 0
            for col in method['columns']:
                if col in whole_sample:
                    whole_sample[col] = pd.to_numeric(whole_sample[col], errors='coerce').fillna(0)
                    whole_sample[abs_col] += whole_sample[col]

            whole_sample[abs_col] = whole_sample[abs_col].abs()
            self.log(f"计算绝对值列完成")

            # 筛选小于阈值的记录
            sampled = whole_sample[whole_sample[abs_col] < method['threshold']]
            self.log(f"抽样完成，抽样行数: {len(sampled)}")

            # 删除临时列
            columns_to_drop = [temp_col, abs_col]
            for col in columns_to_drop:
                if col in sampled.columns:
                    sampled = sampled.drop(columns=col)

            return sampled

        # 方法17， #随机抽XX个，随机数种子XXX 或 #随机抽XX个
        elif method['type'] == 17:
            n_samples = method['n_samples']
            random_seed = method['random_seed']

            self.log(f"应用随机抽样方法: 样本数={n_samples}, 种子={random_seed}")
            self.log(f"当前数据行数: {len(df)}")

            # 重要：判断是否需要全抽
            if n_samples >= len(df):
                self.log(f"要求抽样数({n_samples}) >= 可用数据数({len(df)})，返回全部数据")
                return df.copy()

            # 真正的随机抽样
            sampled = df.sample(n=n_samples, random_state=random_seed)
            self.log(f"随机抽样完成，抽样行数: {len(sampled)}")
            return sampled

        self.log(f"未知抽样方法类型: {method['type']}")
        return df

    def process_samples(self):
        """处理所有抽样文件"""
        self.log(f"\n===== 开始处理抽样文件 =====")
        self.log(f"抽样路径: {self.sample_path}")

        if os.path.isfile(self.sample_path):
            self.total_files = 1
            self._process_sample_file(self.sample_path)
        elif os.path.isdir(self.sample_path):
            files = [f for f in os.listdir(self.sample_path) if f.endswith(('.xlsx', '.xls', '.xlsm', '.csv'))]
            self.total_files = len(files)
            self.log(f"在文件夹中找到 {self.total_files} 个抽样文件")
            for file in files:
                file_path = os.path.join(self.sample_path, file)
                self._process_sample_file(file_path)
        else:
            self.log(f"无效的抽样路径: {self.sample_path}")
            return

        self.log(f"\n===== 抽样处理完成 =====")
        self.log(f"共处理 {self.processed_files} 个文件，生成 {len(self.results)} 个结果")

    def _process_sample_file(self, file_path):
        """处理单个抽样文件"""
        self.processed_files += 1
        file_name = os.path.basename(file_path)
        base_name = os.path.splitext(file_name)[0]
        normalized_base_name = self.normalize_symbols(base_name)

        self.log(f"\n===== 处理文件 {self.processed_files}/{self.total_files}: {file_name} =====")
        self.log(f"基础名称: {base_name}, 规范化后: {normalized_base_name}")

        # 查找适用于此文件的规则
        matching_rules = []

        # 第一轮：精确匹配（公司名 == 文件名）
        for company in self.company_rules:
            normalized_company = self.normalize_symbols(company)
            if normalized_base_name == normalized_company:
                self.log(f"精确匹配: {normalized_base_name} == {normalized_company}")
                matching_rules.extend(self.company_rules[company])

        # 第二轮：部分匹配（公司名包含在文件名中）
        if not matching_rules:
            for company in self.company_rules:
                normalized_company = self.normalize_symbols(company)
                if normalized_company in normalized_base_name:
                    self.log(f"部分匹配: {normalized_company} 在 {normalized_base_name} 中")
                    matching_rules.extend(self.company_rules[company])

        if not matching_rules:
            self.log(f"警告: 未找到匹配规则的抽样文件")
            self.log(f"可用公司规则: {list(self.company_rules.keys())}")
            return

        self.log(f"找到 {len(matching_rules)} 条匹配规则")

        # 加载文件
        try:
            self.log(f"加载文件: {file_name}")
            if file_name.endswith('.csv'):
                # 对CSV文件使用编码检测
                df = self.read_csv_with_encoding_detection(file_path)
            else:
                # 支持xlsm文件和工作表名称
                if self.sheet_name:
                    # 如果指定了工作表名称，尝试加载该工作表
                    try:
                        df = pd.read_excel(file_path, sheet_name=self.sheet_name)
                        self.log(f"成功加载工作表 '{self.sheet_name}'")
                    except Exception as e:
                        self.log(f"加载工作表'{self.sheet_name}'失败，尝试加载第一个工作表: {e}")
                        df = pd.read_excel(file_path)
                else:
                    # 如果没有指定工作表名称，加载第一个工作表
                    df = pd.read_excel(file_path)

            self.log(f"原始数据行数: {len(df)}")

            # 处理所有匹配规则
            result_dfs = []

            for idx, rule_set in enumerate(matching_rules):
                self.log(f"\n=== 应用规则 {idx + 1}/{len(matching_rules)} ===")
                self.log(f"规则详情: {rule_set}")
                temp_df = df.copy()
                self.log(f"应用规则前数据行数: {len(temp_df)}")

                # 应用筛选条件
                filtered_df = self.apply_conditions(temp_df, [rule_set])
                self.log(f"应用筛选条件后行数: {len(filtered_df)}")

                # 确定日期列（如果有）
                date_column = None
                if rule_set.get('start_dates'):
                    date_column = rule_set['start_dates'][0]['column']
                    self.log(f"使用开始日期列: {date_column}")
                elif rule_set.get('end_dates'):
                    date_column = rule_set['end_dates'][0]['column']
                    self.log(f"使用结束日期列: {date_column}")

                # 应用抽样方法
                if 'sampling_method' in rule_set:
                    method = rule_set['sampling_method']
                    self.log(f"应用抽样方法: 类型={method['type']}")
                    self.log(f"抽样方法参数: {method}")
                    sampled_df = self.apply_sampling_method(filtered_df, method, rule_set)
                    self.log(f"抽样后行数: {len(sampled_df)}")
                else:
                    self.log("警告: 未找到抽样方法，返回筛选后的全部数据")
                    sampled_df = filtered_df

                # 添加备注列
                for remark in rule_set.get('remarks', []):
                    col_name = remark['column']
                    remark_value = remark['value']
                    # 在最前列添加备注列
                    sampled_df.insert(0, col_name, remark_value)
                    self.log(f"添加备注列: {col_name}, 值={remark_value}")

                # 如果没有添加月份列但需要添加
                if date_column and '月份' not in sampled_df.columns:
                    try:
                        if not pd.api.types.is_datetime64_any_dtype(sampled_df[date_column]):
                            self.log(f"转换日期列 '{date_column}' 为日期类型")
                            sampled_df[date_column] = pd.to_datetime(sampled_df[date_column], errors='coerce')
                        self.log(f"添加月份列")
                        sampled_df['月份'] = sampled_df[date_column].dt.strftime('%Y%m')
                    except Exception as e:
                        self.log(f"添加月份列错误: {e}")

                # 保存这个规则的结果
                result_dfs.append(sampled_df)

            # 合并所有规则的结果
            if result_dfs:
                combined_df = pd.concat(result_dfs, ignore_index=True)
                self.log(f"合并所有规则结果后总行数: {len(combined_df)}")
                # 添加到结果字典
                self.results[file_name] = combined_df
            else:
                self.log("警告: 没有生成任何结果")

        except Exception as e:
            self.log(f"处理文件 {file_name} 时出错: {str(e)}")
            import traceback
            # 获取完整的堆栈跟踪信息
            error_trace = traceback.format_exc()
            # 将堆栈跟踪记录到日志
            self.log(f"错误详情:\n{error_trace}")

    def save_results(self, output_dir=None):
        """保存结果到文件（强制保存为.xlsx格式）"""
        self.log(f"\n===== 开始保存结果 =====")
        if not output_dir:
            output_dir = os.path.join(os.path.dirname(self.rule_file_path), "抽样结果")
            os.makedirs(output_dir, exist_ok=True)
            self.log(f"创建输出目录: {output_dir}")

        self.log(f"保存 {len(self.results)} 个结果文件")
        for file_name, df in self.results.items():
            # 移除原始扩展名，统一添加"_抽样结果.xlsx"后缀
            base_name = os.path.splitext(file_name)[0]  # 直接分割取基本名
            output_path = os.path.join(output_dir, f"{base_name}_抽样结果.xlsx")  # 固定.xlsx后缀

            try:
                # 统一保存为Excel文件
                df.to_excel(output_path, index=False)
                self.log(f"已保存抽样结果: {os.path.basename(output_path)}")
            except Exception as e:
                self.log(f"保存结果出错: {e}")

        self.log(f"所有结果已保存到目录: {output_dir}")
        return output_dir

class SamplingTool:
    """Tkinter 图形界面封装类，负责组织文件选择、执行抽样和去重导出流程。"""

    def __init__(self, root):
        """初始化主窗口与界面控件。"""
        self.root = root
        self.root.title("SampleR - 1.0.6")
        self.root.geometry("800x400")
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

        self.current_version = "1.0.6"

        self.main_frame = ttk.Frame(root)
        self.main_frame.pack(fill='both', expand=True, padx=20, pady=20)

        # 规则文件区域
        rule_frame = ttk.Frame(self.main_frame)
        rule_frame.pack(fill='x', pady=(0, 10))
        ttk.Label(rule_frame, text="抽样规则文件:").pack(side='left', padx=5)
        self.rule_file_var = tk.StringVar()
        ttk.Entry(rule_frame, textvariable=self.rule_file_var, width=50).pack(side='left', padx=5, fill='x',
                                                                              expand=True)
        ttk.Button(rule_frame, text="浏览", command=self.browse_rule_file).pack(side='left', padx=5)
        ttk.Button(rule_frame, text="生成模板", command=self.generate_template).pack(side='left', padx=5)

        # 抽样文件区域
        sample_frame = ttk.Frame(self.main_frame)
        sample_frame.pack(fill='x', pady=10)
        ttk.Label(sample_frame, text="抽样文件或文件夹:").pack(side='left', padx=5)
        self.sample_file_var = tk.StringVar()
        ttk.Entry(sample_frame, textvariable=self.sample_file_var, width=50).pack(side='left', padx=5, fill='x',
                                                                                  expand=True)
        ttk.Button(sample_frame, text="浏览文件", command=self.browse_sample_file).pack(side='left', padx=5)
        ttk.Button(sample_frame, text="浏览文件夹", command=self.browse_sample_folder).pack(side='left', padx=5)

        # 字典文件区域
        dict_frame = ttk.Frame(self.main_frame)
        dict_frame.pack(fill='x', pady=10)
        ttk.Label(dict_frame, text="字典文件或文件夹:").pack(side='left', padx=5)
        self.dict_file_var = tk.StringVar()
        ttk.Entry(dict_frame, textvariable=self.dict_file_var, width=50).pack(side='left', padx=5, fill='x',
                                                                              expand=True)
        ttk.Button(dict_frame, text="浏览文件", command=self.browse_dict_file).pack(side='left', padx=5)
        ttk.Button(dict_frame, text="浏览文件夹", command=self.browse_dict_folder).pack(side='left', padx=5)

        # Excel工作表名称区域
        sheet_frame = ttk.Frame(self.main_frame)
        sheet_frame.pack(fill='x', pady=10)
        ttk.Label(sheet_frame, text="Excel工作表名称:").pack(side='left', padx=5)
        self.sheet_name_var = tk.StringVar()
        ttk.Entry(sheet_frame, textvariable=self.sheet_name_var, width=50).pack(side='left', padx=5, fill='x',
                                                                                expand=True)

        # 注释提示
        comment_frame = ttk.Frame(self.main_frame)
        comment_frame.pack(fill='x', pady=(0, 10))
        ttk.Label(comment_frame, text="注1：Excel工作表名称不填则使用第一个工作表（含隐藏工作表）。", foreground="gray").pack(anchor='w',
                                                                                                         padx=5)
        ttk.Label(comment_frame, text="注2：如果抽样规则文件不包含字典，可以不上传字典文件。", foreground="gray").pack(
            anchor='w', padx=5)
        ttk.Label(comment_frame, text="注3：如果文件有修改，请重新上传。", foreground="gray").pack(anchor='w',
                                                                                                         padx=5)

        # 进度显示区域
        progress_frame = ttk.Frame(self.main_frame)
        progress_frame.pack(fill='x', pady=(15, 5))
        self.progress_var = tk.StringVar(value="就绪")
        self.progress_label = ttk.Label(progress_frame, textvariable=self.progress_var, foreground="blue")
        self.progress_label.pack()

        # 按钮区域
        button_frame = ttk.Frame(self.main_frame)
        button_frame.pack(fill='x', pady=(20, 10))
        ttk.Button(button_frame, text="开始处理", command=self.execute_sampling, width=15).pack(side='left',
                                                                                                expand=True)
        ttk.Button(button_frame, text="筛选去重", command=self.open_deduplicate_window, width=15).pack(side='left',
                                                                                                       expand=True)

        # 初始化数据
        self.rule_df = None
        self.sample_df = None
        self.result_df = None
        self.dict_data = {}
        self.dedupe_results = None

        # 预留运行状态字段，便于后续扩展任务线程或状态同步。
        self.stop_requested = False

    def on_close(self):
        """窗口关闭时执行必要的资源释放并退出应用。"""
        self.stop_requested = True
        self.root.destroy()

    def log(self, message):
        """记录日志到控制台（如果需要也可以记录到文件）"""
        print(message)

    def update_progress(self, message):
        """更新进度信息"""
        self.progress_var.set(message)
        self.root.update()

    def browse_rule_file(self):
        """浏览规则文件"""
        file_path = filedialog.askopenfilename(
            title="选择抽样规则文件",
            filetypes=[("Excel文件", "*.xlsx *.xls"), ("所有文件", "*.*")]
        )
        if file_path:
            self.rule_file_var.set(file_path)
            try:
                # 读取Excel文件，指定所有列的数据类型为字符串
                self.rule_df = pd.read_excel(file_path, dtype=str)
                # 将NaN值替换为空字符串
                self.rule_df = self.rule_df.fillna('')
                self.update_progress(f"已加载规则文件: {os.path.basename(file_path)}")
            except Exception:
                messagebox.showerror("错误", "无法读取规则文件")

    def generate_template(self):
        """生成规则模板"""
        template_path = filedialog.asksaveasfilename(
            title="保存规则模板",
            defaultextension=".xlsx",
            filetypes=[("Excel文件", "*.xlsx")]
        )
        if not template_path:
            return
        try:
            workbook = xlsxwriter.Workbook(template_path)
            worksheet = workbook.add_worksheet("抽样规则")
            worksheet.set_column('A:I', 20)

            headers = [
                "#文件名", "#开始时间@日期", "#结束时间@日期", "#剔除@科目名称",
                "#开头为@科目名称", "#筛选@科目名称", "#筛选@借方", "#筛选@贷方",
                "#抽样方式", "#备注：抽样规则"
            ]

            data = [
                ["A公司", "2023/1/1", "2023/12/31", "#字典%工资奖金福利费、折旧、摊销", "", "利息收入", "#大于0", "",
                 "#（@借方+@贷方）取绝对值后每1月取前1", "规则1"],
                ["B公司", "yyyy-mm-dd", "", "", "", "", "#大于0", "", "#（@借方+@贷方）取绝对值后的前5", "规则2"],
                ["C公司", "yyyy-m-d", "", "", "", "", "#等于0", "", "#每个@科目名称（@借方+@贷方）取绝对值后每2月取前1",
                 "规则3"],
                ["D公司", "yyyy/mm/dd", "", "", "", "", "#不等于0", "", "#每个@科目名称（@借方+@贷方）取绝对值后的前10",
                 "规则4"],
                ["#字典%重要组成部分", "yyyy/m/d", "", "", "", "", "#不等于0", "",
                 "#（（@借方+@贷方）前5的@供应商）（@借方+@贷方）取绝对值后每2月取前1", "规则5"],
                ["#字典%非重要组成部分", "yyyy年m月d日", "", "", "", "", "", "",
                 "#（（@借方+@贷方）前5的@客户）（@借方+@贷方）取绝对值后的前3", "规则6"],
                ["A公司", "20241001", "20241231", "", "", "", "", "", "#（@借方+@贷方）取绝对值后大于10000", "规则7"],
                ["B公司", "yyyy-mm-dd", "", "", "", "", "", "", "#每个@科目名称（@借方+@贷方）取绝对值后大于20000",
                 "规则8"],
                ["C公司", "yyyy-m-d", "", "", "", "", "", "",
                 "#（（@借方+@贷方）前3的@辅助核算）（@借方+@贷方）取绝对值后大于15000", "规则9"],
                ["D公司", "yyyy/mm/dd", "", "", "", "", "", "", "#（@借方+@贷方）取绝对值后小于10000", "规则10"],
                ["#字典%重要组成部分", "yyyy/m/d", "", "", "", "", "", "",
                 "#每个@科目名称（@借方+@贷方）取绝对值后小于20000", "规则11"],
                ["#字典%非重要组成部分", "yyyy年m月d日", "", "", "", "", "", "",
                 "#（（@借方+@贷方）前3的@辅助核算）（@借方+@贷方）取绝对值后小于15000", "规则12"],
                ["#字典%重要组成部分", "yyyy年m月d日", "", "", "", "", "", "",
                "#随机抽10个，随机数种子10", "规则13"],
                ["#字典%非重要组成部分", "yyyy年m月d日", "", "", "", "", "", "",
                 "#随机抽20个", "规则13"]
            ]

            header_format = workbook.add_format({
                'bold': True,
                'bg_color': '#D9E1F2',
                'border': 1,
                'align': 'center',
                'valign': 'vcenter'
            })

            data_format = workbook.add_format({
                'text_wrap': True,
                'valign': 'vcenter',
                'border': 1
            })

            for col, header in enumerate(headers):
                worksheet.write(0, col, header, header_format)

            for row, row_data in enumerate(data):
                for col, cell_data in enumerate(row_data):
                    worksheet.write(row + 1, col, cell_data, data_format)

            workbook.close()
            self.update_progress(f"规则模板已生成: {os.path.basename(template_path)}")
            messagebox.showinfo("成功", f"规则模板已生成:\n{template_path}")
        except Exception:
            messagebox.showerror("错误", "生成模板失败")

    def browse_sample_file(self):
        """浏览抽样文件"""
        file_path = filedialog.askopenfilename(
            title="选择抽样文件",
            filetypes=[
                ("Excel文件", "*.xlsx *.xls *.xlsm"),
                ("CSV文件", "*.csv"),
                ("所有文件", "*.*")
            ]
        )
        if file_path:
            self.sample_file_var.set(file_path)
            self.update_progress(f"已选择抽样文件: {os.path.basename(file_path)}")

    def browse_sample_folder(self):
        """浏览抽样文件夹"""
        folder_path = filedialog.askdirectory(title="选择抽样文件所在文件夹")
        if folder_path:
            self.sample_file_var.set(folder_path)
            self.update_progress(f"已选择抽样文件夹: {os.path.basename(folder_path)}")

    def browse_dict_file(self):
        """浏览字典文件"""
        file_path = filedialog.askopenfilename(
            title="选择字典文件",
            filetypes=[("文本文件", "*.txt"), ("所有文件", "*.*")]
        )
        if file_path:
            self.dict_file_var.set(file_path)
            self.update_progress(f"已选择字典文件: {os.path.basename(file_path)}")

    def browse_dict_folder(self):
        """浏览字典文件夹"""
        folder_path = filedialog.askdirectory(title="选择字典文件所在文件夹")
        if folder_path:
            self.dict_file_var.set(folder_path)
            self.update_progress(f"已选择字典文件夹: {os.path.basename(folder_path)}")

    def execute_sampling(self):
        """执行抽样 - 多线程版本"""
        if not self.rule_file_var.get():
            messagebox.showwarning("警告", "请上传抽样规则文件！")
            return
        if not self.sample_file_var.get():
            messagebox.showwarning("警告", "请选择抽样文件或文件夹！")
            return

        sample_path = self.sample_file_var.get()
        dict_path = self.dict_file_var.get() if self.dict_file_var.get() else None
        sheet_name = self.sheet_name_var.get() if self.sheet_name_var.get() else None

        # 获取当前时间戳用于文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # 先让用户选择保存位置
        output_path = None
        result_dir = None

        if os.path.isfile(sample_path):
            # 单个文件处理
            base_name = os.path.splitext(os.path.basename(sample_path))[0]
            default_filename = f"{base_name}_{timestamp}_抽样结果.xlsx"

            # 弹出保存对话框
            output_path = filedialog.asksaveasfilename(
                title="保存抽样结果",
                initialfile=default_filename,
                defaultextension=".xlsx",
                filetypes=[("Excel文件", "*.xlsx")]
            )

            if not output_path:
                self.update_progress("用户取消了操作")
                return
        elif os.path.isdir(sample_path):
            # 文件夹处理
            output_dir = filedialog.askdirectory(title="选择保存结果的文件夹")
            if not output_dir:
                self.update_progress("用户取消了操作")
                return

            # 创建结果文件夹
            result_dir = os.path.join(output_dir, f"{timestamp}_抽样结果")
            os.makedirs(result_dir, exist_ok=True)
        else:
            messagebox.showerror("错误", "无效的抽样路径")
            self.update_progress("错误: 无效的抽样路径")
            return

        # 创建线程执行抽样
        self.update_progress("开始加载规则和字典...")
        threading.Thread(
            target=self._execute_sampling_thread,
            args=(self.rule_file_var.get(), sample_path, dict_path, sheet_name, output_path, result_dir),
            daemon=True
        ).start()

    def _execute_sampling_thread(self, rule_path, sample_path, dict_path, sheet_name, output_path, result_dir):
        """在后台线程中执行抽样处理"""
        log_file = None
        try:
            # 创建日志文件
            log_file = self._create_log_file(sample_path, output_path, result_dir)

            # 确保传递正确的参数给 SamplingProcessor
            processor = SamplingProcessor(
                rule_file_path=rule_path,
                sample_path=sample_path,
                dict_path=dict_path,
                sheet_name=sheet_name,
                log_file=log_file
            )

            self.update_progress("处理抽样文件中...")
            processor.process_samples()

            # 保存结果
            if os.path.isfile(sample_path):
                if processor.results:
                    # 获取第一个结果（应该是唯一的结果）
                    df = next(iter(processor.results.values()))
                    df.to_excel(output_path, index=False)
                    self.update_progress(f"结果已保存: {os.path.basename(output_path)}")
                    messagebox.showinfo("完成", f"抽样处理完成！结果已保存到:\n{output_path}")
                else:
                    messagebox.showwarning("警告", "没有生成任何抽样结果")
            else:  # 文件夹处理
                # 保存所有结果
                for file_name, df in processor.results.items():
                    # 移除原始扩展名，添加.xlsx扩展名
                    base_name = os.path.splitext(file_name)[0]
                    output_file_name = f"{base_name}.xlsx"
                    file_output_path = os.path.join(result_dir, output_file_name)

                    df.to_excel(file_output_path, index=False)
                    self.update_progress(f"已保存: {output_file_name}")

                self.update_progress(f"结果已保存到文件夹: {os.path.basename(result_dir)}")
                messagebox.showinfo("完成", f"抽样处理完成！结果已保存到:\n{result_dir}")

        except Exception as e:
            messagebox.showerror("错误", f"抽样处理出错: {str(e)}")
            self.update_progress(f"错误: {str(e)}")
            # 记录详细的错误信息到日志
            if log_file:
                error_trace = traceback.format_exc()
                log_file.write(f"错误详情:\n{error_trace}")
        finally:
            # 关闭日志文件
            if log_file:
                try:
                    log_file.close()
                    self.log(f"日志文件已关闭")
                except Exception as e:
                    self.log(f"关闭日志文件时出错: {str(e)}")

    def _create_log_file(self, sample_path, output_path, result_dir):
        """创建日志文件"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if os.path.isfile(sample_path):
            # 单个文件处理
            base_name = os.path.splitext(os.path.basename(sample_path))[0]
            log_filename = f"{base_name}_{timestamp}_抽样日志.log"

            # 如果output_path是文件路径，则日志文件放在同一目录
            if output_path:
                log_dir = os.path.dirname(output_path)
            else:
                log_dir = os.path.dirname(sample_path)

            log_path = os.path.join(log_dir, log_filename)
        else:
            # 文件夹处理
            log_filename = f"{timestamp}_抽样日志.log"
            log_path = os.path.join(result_dir, log_filename)

        try:
            log_file = open(log_path, 'w', encoding='utf-8')
            self.log(f"创建日志文件: {log_path}")
            return log_file
        except Exception as e:
            self.log(f"创建日志文件失败: {str(e)}")
            return None

    def open_deduplicate_window(self):
        """打开去重窗口"""
        dedupe_window = tk.Toplevel(self.root)
        dedupe_window.title("抽样去重")
        dedupe_window.geometry("650x250")

        # 基准文件选择
        base_frame = ttk.Frame(dedupe_window)
        base_frame.pack(fill='x', padx=20, pady=10)
        ttk.Label(base_frame, text="基准文件(A):").pack(side='left', padx=5)
        self.base_file_var = tk.StringVar()
        ttk.Entry(base_frame, textvariable=self.base_file_var, width=40).pack(side='left', padx=5, fill='x',
                                                                              expand=True)
        btn_frame = ttk.Frame(base_frame)
        btn_frame.pack(side='left', padx=5)
        ttk.Button(btn_frame, text="浏览文件", command=lambda: self.browse_file(self.base_file_var)).pack(side='left',
                                                                                                          padx=2)
        ttk.Button(btn_frame, text="浏览文件夹", command=lambda: self.browse_folder(self.base_file_var)).pack(
            side='left', padx=2)

        # 被筛选文件选择
        target_frame = ttk.Frame(dedupe_window)
        target_frame.pack(fill='x', padx=20, pady=10)
        ttk.Label(target_frame, text="被筛选文件(B):").pack(side='left', padx=5)
        self.target_file_var = tk.StringVar()
        ttk.Entry(target_frame, textvariable=self.target_file_var, width=40).pack(side='left', padx=5, fill='x',
                                                                                  expand=True)
        btn_frame = ttk.Frame(target_frame)
        btn_frame.pack(side='left', padx=5)
        ttk.Button(btn_frame, text="浏览文件", command=lambda: self.browse_file(self.target_file_var)).pack(side='left',
                                                                                                            padx=2)
        ttk.Button(btn_frame, text="浏览文件夹", command=lambda: self.browse_folder(self.target_file_var)).pack(
            side='left', padx=2)

        # 提示信息
        ttk.Label(dedupe_window, text="注：文件夹匹配需要文件名完全一致", foreground="gray").pack(anchor='w', padx=25,
                                                                                                 pady=(0, 10))

        # 进度显示区域
        progress_frame = ttk.Frame(dedupe_window)
        progress_frame.pack(fill='x', padx=20, pady=(5, 10))
        self.dedupe_progress_var = tk.StringVar(value="就绪")
        self.dedupe_progress_label = ttk.Label(progress_frame, textvariable=self.dedupe_progress_var, foreground="blue")
        self.dedupe_progress_label.pack()

        # 按钮区域
        btn_frame = ttk.Frame(dedupe_window)
        btn_frame.pack(pady=10)
        ttk.Button(btn_frame, text="开始处理", command=self.execute_and_save_deduplicate, width=15).pack(pady=10)

        self.dedupe_results = None

    def browse_file(self, var):
        """浏览文件"""
        file_path = filedialog.askopenfilename(
            title="选择文件",
            filetypes=[("Excel文件", "*.xlsx *.xls *.xlsm"), ("CSV文件", "*.csv"), ("所有文件", "*.*")]
        )
        if file_path:
            var.set(file_path)

    def browse_folder(self, var):
        """浏览文件夹"""
        folder_path = filedialog.askdirectory(title="选择文件夹")
        if folder_path:
            var.set(folder_path)

    def execute_and_save_deduplicate(self):
        """执行去重并保存结果 - 多线程版本"""
        if not self.base_file_var.get() or not self.target_file_var.get():
            messagebox.showwarning("警告", "请选择基准文件(A)和被筛选文件(B)!")
            return

        base_path = self.base_file_var.get()
        target_path = self.target_file_var.get()

        is_base_file = os.path.isfile(base_path)
        is_target_file = os.path.isfile(target_path)
        is_base_dir = os.path.isdir(base_path)
        is_target_dir = os.path.isdir(target_path)

        # 获取当前时间戳
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if (is_base_file and is_target_file) or (is_base_dir and is_target_dir):
            # 创建线程执行去重
            threading.Thread(
                target=self._execute_deduplicate_thread,
                args=(base_path, target_path, timestamp),
                daemon=True
            ).start()
        else:
            messagebox.showwarning("输入类型错误", "基准文件(A)和被筛选文件(B)必须是相同的类型（都是文件或都是文件夹）")
            self.dedupe_progress_var.set("输入类型错误，请选择相同类型的输入")

    def _execute_deduplicate_thread(self, base_path, target_path, timestamp):
        """在后台线程中执行去重处理"""
        try:
            self.execute_deduplicate_logic(base_path, target_path)

            if os.path.isfile(base_path) and os.path.isfile(target_path):
                self.save_as_file(timestamp)
            elif os.path.isdir(base_path) and os.path.isdir(target_path):
                self.save_as_folder(timestamp)
        except Exception as e:
            messagebox.showerror("错误", f"去重处理出错: {str(e)}")
            self.dedupe_progress_var.set(f"错误: {str(e)}")

    def execute_deduplicate_logic(self, base_path, target_path):
        """执行去重逻辑 - 修改版"""
        self.dedupe_results = {}

        if os.path.isdir(base_path) and os.path.isdir(target_path):
            # 文件夹处理模式
            base_files = [f for f in os.listdir(base_path) if f.endswith(('.xlsx', '.xls', '.xlsm', '.csv'))]
            target_files = [f for f in os.listdir(target_path) if f.endswith(('.xlsx', '.xls', '.xlsm', '.csv'))]

            # 找出两个文件夹中同名的文件
            common_files = set(base_files) & set(target_files)
            total_files = len(common_files)

            if total_files == 0:
                self.dedupe_progress_var.set("没有找到同名文件，无法进行去重")
                return

            self.dedupe_progress_var.set(f"开始处理 {total_files} 对文件...")
            self.root.update()

            for i, filename in enumerate(common_files):
                base_file_path = os.path.join(base_path, filename)
                target_file_path = os.path.join(target_path, filename)

                self.dedupe_progress_var.set(f"处理文件 {i + 1}/{total_files}: {filename}")
                self.root.update()

                try:
                    # 读取两个文件
                    base_df = self.read_file(base_file_path)
                    target_df = self.read_file(target_file_path)

                    # 执行去重
                    deduped_df = self.deduplicate_dataframes(base_df, target_df)

                    # 保存结果
                    self.dedupe_results[filename] = deduped_df

                except Exception as e:
                    print(f"处理文件 {filename} 时出错: {str(e)}")
                    self.dedupe_results[filename] = None

            self.dedupe_progress_var.set(f"去重操作完成！处理了 {len(self.dedupe_results)} 个文件")

        elif os.path.isfile(base_path) and os.path.isfile(target_path):
            # 单个文件处理模式
            filename = os.path.basename(target_path)
            self.dedupe_progress_var.set(f"开始处理文件: {filename}")
            self.root.update()

            try:
                # 读取两个文件
                base_df = self.read_file(base_path)
                target_df = self.read_file(target_path)

                # 执行去重
                deduped_df = self.deduplicate_dataframes(base_df, target_df)

                # 保存结果
                self.dedupe_results[filename] = deduped_df

                self.dedupe_progress_var.set(f"去重操作完成！")

            except Exception as e:
                print(f"处理文件 {filename} 时出错: {str(e)}")
                self.dedupe_results[filename] = None
                self.dedupe_progress_var.set(f"处理文件出错: {str(e)}")
        else:
            messagebox.showwarning("输入类型错误", "基准文件(A)和被筛选文件(B)必须是相同的类型（都是文件或都是文件夹）")
            self.dedupe_progress_var.set("输入类型错误，请选择相同类型的输入")

    def read_file(self, file_path):
        """读取文件为DataFrame，自动处理编码问题"""
        try:
            return read_file_with_encoding_detection(file_path)
        except Exception as e:
            # 如果自动检测失败，尝试使用原始方法
            if file_path.endswith('.csv'):
                return pd.read_csv(file_path)
            else:
                return pd.read_excel(file_path)

    def read_file_with_encoding_detection(file_path):
        """读取文件并自动检测编码（主要用于CSV文件）"""
        if file_path.endswith('.csv'):
            # 对于CSV文件，先检测编码
            try:
                # 首先尝试UTF-8编码（最常见的编码）
                return pd.read_csv(file_path, encoding='utf-8')
            except UnicodeDecodeError:
                try:
                    # 尝试UTF-8 with BOM（某些Windows应用程序使用的编码）
                    return pd.read_csv(file_path, encoding='utf-8-sig')
                except UnicodeDecodeError:
                    try:
                        # 尝试GBK（中文Windows常用编码）
                        return pd.read_csv(file_path, encoding='gbk')
                    except UnicodeDecodeError:
                        try:
                            # 尝试GB2312（GBK的前身）
                            return pd.read_csv(file_path, encoding='gb2312')
                        except UnicodeDecodeError:
                            # 如果常见编码都失败，使用chardet自动检测编码
                            try:
                                # 读取文件的一部分来检测编码
                                with open(file_path, 'rb') as f:
                                    raw_data = f.read(10000)  # 读取前10000字节用于检测
                                    result = chardet.detect(raw_data)
                                    encoding = result['encoding']

                                # 使用检测到的编码读取文件
                                return pd.read_csv(file_path, encoding=encoding)
                            except Exception:
                                # 最后尝试使用错误忽略策略
                                return pd.read_csv(file_path, encoding='utf-8', errors='replace')
        else:
            # 对于非CSV文件（如Excel），直接使用pandas读取
            return pd.read_excel(file_path)

    def deduplicate_dataframes(self, base_df, target_df):
        """执行去重操作"""
        # 确保两个DataFrame有相同的列
        if not set(base_df.columns) == set(target_df.columns):
            # 尝试重新排列列
            target_df = target_df[base_df.columns]

        # 创建一个副本用于比较
        base_df_clean = base_df.copy()
        target_df_clean = target_df.copy()

        # 重置索引以确保所有行都是唯一的
        base_df_clean.reset_index(drop=True, inplace=True)
        target_df_clean.reset_index(drop=True, inplace=True)

        # 添加临时索引列用于标识原始行
        base_df_clean['__temp_index'] = base_df_clean.index
        target_df_clean['__temp_index'] = target_df_clean.index

        # 找出在base_df中存在的行
        duplicates = pd.merge(target_df_clean, base_df_clean, how='inner', on=list(base_df.columns))

        # 获取需要删除的行索引
        duplicate_indices = duplicates['__temp_index_x'].unique()

        # 删除重复行
        deduped_df = target_df[~target_df.index.isin(duplicate_indices)]

        return deduped_df

    def save_as_file(self, timestamp):
        """保存为单个文件 - 添加时间戳"""
        if not self.dedupe_results or len(self.dedupe_results) == 0:
            messagebox.showwarning("无结果", "没有找到匹配文件，无法保存")
            return

        # 只有一个文件结果
        filename, deduped_df = next(iter(self.dedupe_results.items()))

        if deduped_df is None:
            messagebox.showwarning("错误", "去重操作失败，无法保存")
            return

        # 创建默认文件名
        base_name = os.path.splitext(filename)[0]
        default_filename = f"{base_name}_{timestamp}_去重后.xlsx"

        # 弹出保存对话框
        file_path = filedialog.asksaveasfilename(
            title="保存去重结果",
            initialfile=default_filename,
            defaultextension=".xlsx",
            filetypes=[("Excel文件", "*.xlsx")]
        )

        if not file_path:
            self.dedupe_progress_var.set("用户取消了操作")
            return

        try:
            deduped_df.to_excel(file_path, index=False)
            self.dedupe_progress_var.set(f"结果已保存: {os.path.basename(file_path)}")
            messagebox.showinfo("成功", f"去重结果已保存到:\n{file_path}")
        except Exception as e:
            messagebox.showerror("保存错误", f"保存文件时出错: {str(e)}")
            self.dedupe_progress_var.set(f"保存失败: {str(e)}")

    def save_as_folder(self, timestamp):
        """保存为文件夹（多个文件） - 添加时间戳"""
        if not self.dedupe_results or len(self.dedupe_results) == 0:
            messagebox.showwarning("无结果", "没有生成任何去重结果")
            return

        # 选择输出文件夹
        output_dir = filedialog.askdirectory(title="选择保存结果的文件夹")
        if not output_dir:
            self.dedupe_progress_var.set("用户取消了操作")
            return

        # 创建结果文件夹
        result_dir = os.path.join(output_dir, f"{timestamp}_去重结果")
        os.makedirs(result_dir, exist_ok=True)

        total_files = len(self.dedupe_results)
        success_count = 0

        self.dedupe_progress_var.set(f"开始保存 {total_files} 个结果文件...")
        self.root.update()

        for i, (filename, deduped_df) in enumerate(self.dedupe_results.items()):
            if deduped_df is None:
                continue

            try:
                # 创建新文件名
                base_name = os.path.splitext(filename)[0]
                new_filename = f"{base_name}_去重后.xlsx"
                output_path = os.path.join(result_dir, new_filename)

                # 保存文件
                deduped_df.to_excel(output_path, index=False)
                success_count += 1

                self.dedupe_progress_var.set(f"已保存: {new_filename} ({i + 1}/{total_files})")
                self.root.update()
            except Exception as e:
                print(f"保存文件 {filename} 时出错: {str(e)}")

        self.dedupe_progress_var.set(f"所有结果已保存到文件夹: {os.path.basename(result_dir)}")
        messagebox.showinfo("成功", f"共处理 {total_files} 个文件，成功保存 {success_count} 个去重结果到:\n{result_dir}")

if __name__ == "__main__":
    root = tk.Tk()
    app = SamplingTool(root)
    root.mainloop()
