# -*- coding: utf-8 -*-
"""
data_simulator.py - 实时数据模拟器 v2.2
通过 Socket 或 Kafka 向 Flink 发送模拟用户行为数据

功能特性:
- 自动重连（Socket模式, 断开后等待并重试）
- Kafka 模式（--kafka, 直接发送到 Kafka Topic）
- 速率控制（普通模式: 1-500条/秒, Turbo模式: ~5000条/秒）
- 支持 CSV 和 XLSX 格式
- 自动跳过 header 行
- 实时统计打印 + 预计剩余时间

使用方法:
    python data_simulator.py                                         # Socket: data\\test.csv, localhost:9999, 200条/秒
    python data_simulator.py --turbo                                  # Socket Turbo: ~5000条/秒
    python data_simulator.py data\\test.csv localhost 9999 100        # Socket 自定义速率
    python data_simulator.py data\\test.csv 192.168.88.161 9999 30   # Socket 远程Flink
    python data_simulator.py --kafka                                  # Kafka: 发送到 node1:9092
    python data_simulator.py --kafka --turbo                          # Kafka Turbo模式
"""

import sys
import os
import socket
import time
import pandas as pd

try:
    from kafka import KafkaProducer
    HAS_KAFKA = True
except ImportError:
    HAS_KAFKA = False

# 获取脚本所在目录
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# ==================== 配置参数 ====================
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 9999
DEFAULT_RATE = 200  # 默认发送速率: 200条/秒（发送151k数据约需12分钟）
TURBO_RATE = 5000   # Turbo模式: 5000条/秒（发送151k数据约需30秒）
DEFAULT_FILE = "split_data/test.csv"  # 默认使用 split_data/test.csv
MAX_RETRIES = 5                # 最大重连次数
RETRY_DELAY_SECONDS = 3        # 重连间隔（秒）

# Kafka 配置
KAFKA_BOOTSTRAP_SERVERS = "node1.itcast.cn:9092"
KAFKA_TOPIC = "user-behavior-topic"

# ==================== 数据模拟器类 ====================

class DataSimulator:
    def __init__(self, host: str, port: int, rate: int):
        self.host = host
        self.port = port
        self.rate = rate  # 条/秒
        self.interval = 1.0 / rate  # 发送间隔（秒）
        self.socket = None
        self.total_sent = 0

    def connect(self, retry: bool = True):
        """建立Socket连接，支持自动重连

        Args:
            retry: 是否启用自动重连

        Returns:
            bool: 是否连接成功
        """
        attempts = 0
        max_attempts = MAX_RETRIES if retry else 1

        while attempts < max_attempts:
            attempts += 1
            try:
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.socket.settimeout(5.0)  # 5秒超时
                self.socket.connect((self.host, self.port))
                print(f"[连接成功] 已连接到 {self.host}:{self.port}")
                return True
            except (ConnectionRefusedError, TimeoutError, OSError) as e:
                if retry and attempts < max_attempts:
                    print(f"[重连] 第 {attempts}/{max_attempts} 次失败: {e}")
                    print(f"[重连] {RETRY_DELAY_SECONDS} 秒后重试...")
                    self.close()
                    time.sleep(RETRY_DELAY_SECONDS)
                else:
                    print(f"[连接失败] 无法连接到 {self.host}:{self.port}")
                    print(f"[提示] 请确保 Flink 流处理任务已启动并监听 {self.port} 端口")
                    return False
        return False

    def reconnect(self):
        """断线重连"""
        print("[断线] 连接已断开，尝试重连...")
        self.close()
        return self.connect(retry=True)

    def send_data(self, csv_path: str):
        """发送数据，支持 Socket 和 Kafka 两种模式"""
        # Kafka 模式直接走 Kafka 发送
        if hasattr(self, 'kafka_mode') and self.kafka_mode:
            self._send_to_kafka(csv_path)
            return
        # Socket 模式：读取文件并通过Socket逐行发送数据
        # 1. 读取文件（自动识别格式）
        try:
            abs_path = csv_path if os.path.isabs(csv_path) else os.path.join(SCRIPT_DIR, csv_path)

            if not os.path.exists(abs_path):
                print(f"[错误] 文件不存在: {abs_path}")
                return

            if abs_path.endswith('.xlsx') or abs_path.endswith('.xls'):
                df = pd.read_excel(abs_path)
            else:
                # CSV文件：尝试自动检测是否有header
                with open(abs_path, 'r', encoding='utf-8') as f:
                    first_line = f.readline().strip().lower()
                if first_line and ('user_id' in first_line or 'user' in first_line):
                    df = pd.read_csv(abs_path, encoding='utf-8')
                else:
                    df = pd.read_csv(abs_path, header=None, encoding='utf-8')
                    df.columns = ['user_id', 'item_id', 'category_id', 'behavior_type', 'timestamp']

            # 确保必要字段存在
            required_cols = ['user_id', 'item_id', 'behavior_type']
            for col in required_cols:
                if col not in df.columns:
                    print(f"[错误] 数据缺少必要字段: {col}")
                    return

            total_records = len(df)
            estimated_seconds = total_records / self.rate if self.rate > 0 else 0
            print(f"[数据加载] 共 {total_records:,} 条记录，字段: {list(df.columns)}")
            if estimated_seconds > 0:
                print(f"[预计耗时] 约 {estimated_seconds:.0f} 秒 ({estimated_seconds/60:.1f} 分钟) @ {self.rate}条/秒")

            # 统计用户/商品数
            unique_users = df['user_id'].nunique()
            unique_items = df['item_id'].nunique()
            print(f"[数据概览] {unique_users:,} 个用户 × {unique_items:,} 个商品")

        except Exception as e:
            print(f"[错误] 读取文件失败: {e}")
            return

        # 2. 逐行发送
        print(f"[开始发送] 速率: {self.rate:,}条/秒 (间隔: {self.interval:.4f}秒)")
        print(f"按 Ctrl+C 可提前停止")
        print("-" * 50)

        start_time = time.time()
        last_log_time = start_time

        try:
            for index, row in df.iterrows():
                # 构造发送格式: user_id,item_id,category_id,behavior_type,timestamp
                user_id = int(row['user_id'])
                item_id = int(row['item_id'])
                category_id = int(row.get('category_id', 0))
                behavior_type = str(row['behavior_type']).strip().lower()
                timestamp = int(row.get('timestamp', time.time()))

                message = f"{user_id},{item_id},{category_id},{behavior_type},{timestamp}\n"

                try:
                    self.socket.sendall(message.encode('utf-8'))
                    self.total_sent += 1

                    # 每100条打印进度和速率
                    if self.total_sent % 100 == 0:
                        current_time = time.time()
                        elapsed = current_time - last_log_time
                        actual_rate = 100.0 / elapsed if elapsed > 0 else 0
                        total_elapsed = current_time - start_time
                        progress = self.total_sent / total_records * 100
                        remaining = (total_records - self.total_sent) / actual_rate if actual_rate > 0 else 0
                        print(f"[进度] {self.total_sent:,}/{total_records:,} ({progress:.1f}%) "
                              f"实时速率: {actual_rate:.0f}条/秒 "
                              f"剩余: {remaining:.0f}秒")
                        last_log_time = current_time

                except (BrokenPipeError, ConnectionResetError, OSError):
                    print("[断线] Socket连接异常，尝试重连...")
                    if not self.reconnect():
                        print("[错误] 重连失败，停止发送")
                        break
                    # 重连成功后重新发送本条数据
                    continue

                # 控制发送速率（turbo模式不做限制）
                if self.interval < 0.001:
                    # 高速模式：不做sleep
                    pass
                elif self.interval > 0:
                    time.sleep(self.interval)

        except KeyboardInterrupt:
            print("\n[停止] 用户手动中断")

        finally:
            self.print_summary(start_time)

    def _send_to_kafka(self, csv_path: str):
        """通过 Kafka 发送数据"""
        abs_path = csv_path if os.path.isabs(csv_path) else os.path.join(SCRIPT_DIR, csv_path)
        if not os.path.exists(abs_path):
            print(f"[错误] 文件不存在: {abs_path}")
            return

        # 读取文件
        if abs_path.endswith('.xlsx') or abs_path.endswith('.xls'):
            df = pd.read_excel(abs_path)
        else:
            with open(abs_path, 'r', encoding='utf-8') as f:
                first_line = f.readline().strip().lower()
            if first_line and ('user_id' in first_line or 'user' in first_line):
                df = pd.read_csv(abs_path, encoding='utf-8')
            else:
                df = pd.read_csv(abs_path, header=None, encoding='utf-8')
                df.columns = ['user_id', 'item_id', 'category_id', 'behavior_type', 'timestamp']

        total_records = len(df)
        estimated_seconds = total_records / self.rate if self.rate > 0 else 0
        print(f"[数据加载] 共 {total_records:,} 条记录")
        if estimated_seconds > 0:
            print(f"[预计耗时] 约 {estimated_seconds:.0f} 秒 @ {self.rate}条/秒")

        start_time = time.time()
        last_log_time = start_time

        try:
            for index, row in df.iterrows():
                message = f"{int(row['user_id'])},{int(row['item_id'])},{int(row.get('category_id', 0))}," \
                          f"{str(row['behavior_type']).strip().lower()},{int(row.get('timestamp', time.time()))}"
                self.producer.send(KAFKA_TOPIC, message.encode('utf-8'))
                self.total_sent += 1

                if self.total_sent % 100 == 0:
                    current_time = time.time()
                    elapsed = current_time - last_log_time
                    actual_rate = 100.0 / elapsed if elapsed > 0 else 0
                    remaining = (total_records - self.total_sent) / actual_rate if actual_rate > 0 else 0
                    progress = self.total_sent / total_records * 100
                    print(f"[Kafka进度] {self.total_sent:,}/{total_records:,} ({progress:.1f}%) "
                          f"速率: {actual_rate:.0f}条/秒 剩余: {remaining:.0f}秒")
                    last_log_time = current_time

                # 速率控制
                if self.rate < 1000:
                    time.sleep(self.interval)

            self.producer.flush()
        except KeyboardInterrupt:
            print("\n[停止] 用户手动中断")
        finally:
            self.print_summary(start_time)

    def print_summary(self, start_time: float = None):
        """打印发送统计"""
        total_elapsed = time.time() - start_time if start_time else 0
        print("-" * 50)
        print(f"[统计] 共发送 {self.total_sent} 条记录")
        if total_elapsed > 0:
            avg_rate = self.total_sent / total_elapsed
            print(f"[统计] 总耗时 {total_elapsed:.1f} 秒 ({total_elapsed/60:.1f} 分钟)")
            print(f"[统计] 平均速率 {avg_rate:.1f} 条/秒")
        print("[关闭] 模拟器已停止")

    def close(self):
        """关闭Socket连接"""
        if self.socket:
            try:
                self.socket.close()
            except Exception:
                pass
            self.socket = None


# ==================== 主程序入口 ====================

def main():
    # 解析命令行参数
    kafka_mode = '--kafka' in sys.argv or '-k' in sys.argv
    turbo_mode = '--turbo' in sys.argv or '-t' in sys.argv

    # 移除特殊标记避免影响其他参数解析
    for flag in ['--kafka', '-k', '--turbo', '-t']:
        sys.argv = [a for a in sys.argv if a != flag]

    # 获取文件路径
    if len(sys.argv) < 2:
        csv_path = DEFAULT_FILE
        print(f"[提示] 使用默认数据文件: {csv_path}")
    else:
        csv_path = sys.argv[1]

    # 检查 Kafka 依赖
    if kafka_mode and not HAS_KAFKA:
        print("[错误] 使用 --kafka 模式需要安装 kafka-python: pip install kafka-python")
        return

    # 获取其他参数
    host = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_HOST
    port = int(sys.argv[3]) if len(sys.argv) > 3 else DEFAULT_PORT
    rate = int(sys.argv[4]) if len(sys.argv) > 4 else DEFAULT_RATE

    if turbo_mode:
        rate = TURBO_RATE
        print(f"[Turbo模式] 以 ~{TURBO_RATE}条/秒 高速发送")
    else:
        # 限制速率范围
        rate = max(1, min(rate, 500))

    mode_str = "Kafka" if kafka_mode else "Socket"
    target_str = f"{KAFKA_BOOTSTRAP_SERVERS}/{KAFKA_TOPIC}" if kafka_mode else f"{host}:{port}"

    print("=" * 50)
    print("  Flink 数据模拟器 v2.2")
    print(f"  模式: {mode_str}")
    print("=" * 50)
    print(f"  数据文件: {csv_path}")
    print(f"  目标: {target_str}")
    print(f"  目标速率: {rate:,}条/秒")
    print("=" * 50)
    if turbo_mode:
        print(f"  [Turbo] 以 ~{TURBO_RATE}条/秒 极速发送")
    print(f"  使用 --kafka 切换 Kafka 模式")
    print()

    if kafka_mode:
        # Kafka 模式：直接创建 producer 发送
        print(f"[Kafka] 连接到 {KAFKA_BOOTSTRAP_SERVERS} ...")
        producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        simulator = DataSimulator(host, port, rate)
        simulator.producer = producer
        simulator.kafka_mode = True
        simulator.send_data(csv_path)
        producer.close()
    else:
        # Socket 模式
        simulator = DataSimulator(host, port, rate)
        if simulator.connect(retry=True):
            simulator.send_data(csv_path)
        simulator.close()


if __name__ == "__main__":
    main()
