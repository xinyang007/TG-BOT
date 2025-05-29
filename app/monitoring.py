import time
import asyncio
import psutil
import threading
from typing import Dict, Any, List, Optional, Callable
from collections import defaultdict, deque
from dataclasses import dataclass, field
from functools import wraps
from contextlib import asynccontextmanager
import statistics

from .logging_config import get_logger

logger = get_logger("app.monitoring")


@dataclass
class MetricValue:
    """指标值"""
    value: float
    timestamp: float = field(default_factory=time.time)
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class TimingMetric:
    """计时指标"""
    name: str
    duration: float
    timestamp: float = field(default_factory=time.time)
    labels: Dict[str, str] = field(default_factory=dict)
    success: bool = True


class Counter:
    """计数器"""

    def __init__(self, name: str, description: str = ""):
        self.name = name
        self.description = description
        self._value = 0
        self._lock = threading.Lock()

    def increment(self, amount: int = 1, labels: Dict[str, str] = None):
        """增加计数"""
        with self._lock:
            self._value += amount
        logger.debug(f"Counter {self.name} incremented by {amount} to {self._value}")

    def get_value(self) -> int:
        """获取当前值"""
        with self._lock:
            return self._value

    def reset(self):
        """重置计数器"""
        with self._lock:
            old_value = self._value
            self._value = 0
        logger.debug(f"Counter {self.name} reset from {old_value} to 0")


class Histogram:
    """直方图（用于计时统计）"""

    def __init__(self, name: str, description: str = "", max_samples: int = 1000):
        self.name = name
        self.description = description
        self.max_samples = max_samples
        self._samples = deque(maxlen=max_samples)
        self._lock = threading.Lock()

    def observe(self, value: float, labels: Dict[str, str] = None):
        """记录观测值"""
        with self._lock:
            self._samples.append(MetricValue(value, labels=labels or {}))
        logger.debug(f"Histogram {self.name} observed value {value}")

    def get_stats(self) -> Dict[str, float]:
        """获取统计信息"""
        with self._lock:
            if not self._samples:
                return {"count": 0}

            values = [sample.value for sample in self._samples]
            return {
                "count": len(values),
                "sum": sum(values),
                "min": min(values),
                "max": max(values),
                "mean": statistics.mean(values),
                "median": statistics.median(values),
                "p95": self._percentile(values, 0.95),
                "p99": self._percentile(values, 0.99)
            }

    def _percentile(self, values: List[float], percentile: float) -> float:
        """计算百分位数"""
        sorted_values = sorted(values)
        index = int(len(sorted_values) * percentile)
        return sorted_values[min(index, len(sorted_values) - 1)]


class Gauge:
    """仪表（当前值）"""

    def __init__(self, name: str, description: str = ""):
        self.name = name
        self.description = description
        self._value = 0.0
        self._lock = threading.Lock()

    def set(self, value: float, labels: Dict[str, str] = None):
        """设置值"""
        with self._lock:
            self._value = value
        logger.debug(f"Gauge {self.name} set to {value}")

    def increment(self, amount: float = 1.0):
        """增加值"""
        with self._lock:
            self._value += amount
        logger.debug(f"Gauge {self.name} incremented by {amount} to {self._value}")

    def decrement(self, amount: float = 1.0):
        """减少值"""
        self.increment(-amount)

    def get_value(self) -> float:
        """获取当前值"""
        with self._lock:
            return self._value


class MetricsCollector:
    """指标收集器"""

    def __init__(self):
        self.counters: Dict[str, Counter] = {}
        self.histograms: Dict[str, Histogram] = {}
        self.gauges: Dict[str, Gauge] = {}
        self.timing_records: deque = deque(maxlen=10000)
        self._lock = threading.Lock()
        self.logger = get_logger("app.monitoring.collector")
        self._background_tasks: List[asyncio.Task] = []

        # 初始化系统指标
        self._init_system_metrics()

    def _init_system_metrics(self):
        """初始化系统指标"""
        self.gauges["system_cpu_percent"] = Gauge("system_cpu_percent", "System CPU usage percentage")
        self.gauges["system_memory_percent"] = Gauge("system_memory_percent", "System memory usage percentage")
        self.gauges["system_memory_available"] = Gauge("system_memory_available", "Available system memory in bytes")
        self.gauges["process_memory_rss"] = Gauge("process_memory_rss", "Process RSS memory in bytes")
        self.gauges["process_memory_vms"] = Gauge("process_memory_vms", "Process VMS memory in bytes")

        # 应用指标
        self.counters["http_requests_total"] = Counter("http_requests_total", "Total HTTP requests")
        self.counters["telegram_api_calls_total"] = Counter("telegram_api_calls_total", "Total Telegram API calls")
        self.counters["database_operations_total"] = Counter("database_operations_total", "Total database operations")
        self.counters["cache_operations_total"] = Counter("cache_operations_total", "Total cache operations")
        self.counters["message_processing_total"] = Counter("message_processing_total", "Total messages processed")
        self.counters["errors_total"] = Counter("errors_total", "Total errors")

        self.histograms["http_request_duration"] = Histogram("http_request_duration",
                                                             "HTTP request duration in seconds")
        self.histograms["telegram_api_duration"] = Histogram("telegram_api_duration",
                                                             "Telegram API call duration in seconds")
        self.histograms["database_operation_duration"] = Histogram("database_operation_duration",
                                                                   "Database operation duration in seconds")
        self.histograms["message_processing_duration"] = Histogram("message_processing_duration",
                                                                   "Message processing duration in seconds")

        self.gauges["active_conversations"] = Gauge("active_conversations", "Number of active conversations")
        self.gauges["cached_items"] = Gauge("cached_items", "Number of cached items")

    def counter(self, name: str, description: str = "") -> Counter:
        """获取或创建计数器"""
        with self._lock:
            if name not in self.counters:
                self.counters[name] = Counter(name, description)
            return self.counters[name]

    def histogram(self, name: str, description: str = "") -> Histogram:
        """获取或创建直方图"""
        with self._lock:
            if name not in self.histograms:
                self.histograms[name] = Histogram(name, description)
            return self.histograms[name]

    def gauge(self, name: str, description: str = "") -> Gauge:
        """获取或创建仪表"""
        with self._lock:
            if name not in self.gauges:
                self.gauges[name] = Gauge(name, description)
            return self.gauges[name]

    def record_timing(self, name: str, duration: float, labels: Dict[str, str] = None, success: bool = True):
        """记录计时信息"""
        timing = TimingMetric(name, duration, labels=labels or {}, success=success)
        self.timing_records.append(timing)

        # 也记录到对应的直方图
        if name in self.histograms:
            self.histograms[name].observe(duration, labels)

        self.logger.debug(f"Recorded timing for {name}: {duration:.3f}s")

    @asynccontextmanager
    async def time_operation(self, operation_name: str, labels: Dict[str, str] = None):
        """计时上下文管理器"""
        start_time = time.time()
        success = True
        try:
            yield
        except Exception as e:
            success = False
            self.counter("errors_total").increment()
            raise
        finally:
            duration = time.time() - start_time
            self.record_timing(operation_name, duration, labels, success)

    def timing_decorator(self, operation_name: str, labels: Dict[str, str] = None):
        """计时装饰器"""

        def decorator(func):
            if asyncio.iscoroutinefunction(func):
                @wraps(func)
                async def async_wrapper(*args, **kwargs):
                    async with self.time_operation(operation_name, labels):
                        return await func(*args, **kwargs)

                return async_wrapper
            else:
                @wraps(func)
                def sync_wrapper(*args, **kwargs):
                    start_time = time.time()
                    success = True
                    try:
                        result = func(*args, **kwargs)
                        return result
                    except Exception as e:
                        success = False
                        self.counter("errors_total").increment()
                        raise
                    finally:
                        duration = time.time() - start_time
                        self.record_timing(operation_name, duration, labels, success)

                return sync_wrapper

        return decorator

    def get_all_metrics(self) -> Dict[str, Any]:
        """获取所有指标"""
        with self._lock:
            metrics = {
                "counters": {name: counter.get_value() for name, counter in self.counters.items()},
                "gauges": {name: gauge.get_value() for name, gauge in self.gauges.items()},
                "histograms": {name: hist.get_stats() for name, hist in self.histograms.items()}
            }

        # 添加最近的计时记录统计
        recent_timings = list(self.timing_records)[-100:]  # 最近100条
        if recent_timings:
            timing_stats = {}
            by_operation = defaultdict(list)

            for timing in recent_timings:
                by_operation[timing.name].append(timing.duration)

            for operation, durations in by_operation.items():
                timing_stats[operation] = {
                    "count": len(durations),
                    "avg_duration": statistics.mean(durations),
                    "max_duration": max(durations),
                    "min_duration": min(durations)
                }

            metrics["recent_timings"] = timing_stats

        return metrics

    async def update_system_metrics(self):
        """更新系统指标"""
        try:
            # CPU使用率
            cpu_percent = psutil.cpu_percent(interval=1)
            self.gauges["system_cpu_percent"].set(cpu_percent)

            # 内存使用情况
            memory = psutil.virtual_memory()
            self.gauges["system_memory_percent"].set(memory.percent)
            self.gauges["system_memory_available"].set(memory.available)

            # 进程内存使用情况
            process = psutil.Process()
            memory_info = process.memory_info()
            self.gauges["process_memory_rss"].set(memory_info.rss)
            self.gauges["process_memory_vms"].set(memory_info.vms)

            self.logger.debug(f"Updated system metrics: CPU {cpu_percent}%, Memory {memory.percent}%")

        except Exception as e:
            self.logger.error(f"Error updating system metrics: {e}", exc_info=True)

    async def _system_metrics_task(self):
        """系统指标更新任务"""
        while True:
            try:
                await self.update_system_metrics()
                await asyncio.sleep(30)  # 每30秒更新一次
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in system metrics task: {e}", exc_info=True)
                await asyncio.sleep(60)  # 错误时等待更长时间

    def start_background_tasks(self):
        """启动后台任务"""
        if not self._background_tasks:
            task = asyncio.create_task(self._system_metrics_task())
            self._background_tasks.append(task)
            self.logger.info("Started background metrics collection tasks")

    async def stop_background_tasks(self):
        """停止后台任务"""
        for task in self._background_tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        self._background_tasks.clear()
        self.logger.info("Stopped background metrics collection tasks")

    def get_performance_summary(self) -> Dict[str, Any]:
        """获取性能摘要"""
        metrics = self.get_all_metrics()

        # 计算一些关键性能指标
        total_requests = metrics["counters"].get("http_requests_total", 0)
        total_errors = metrics["counters"].get("errors_total", 0)
        error_rate = (total_errors / total_requests * 100) if total_requests > 0 else 0

        http_duration_stats = metrics["histograms"].get("http_request_duration", {})
        avg_response_time = http_duration_stats.get("mean", 0)

        return {
            "total_requests": total_requests,
            "total_errors": total_errors,
            "error_rate_percent": round(error_rate, 2),
            "avg_response_time_seconds": round(avg_response_time, 3),
            "system_cpu_percent": metrics["gauges"].get("system_cpu_percent", 0),
            "system_memory_percent": metrics["gauges"].get("system_memory_percent", 0),
            "active_conversations": metrics["gauges"].get("active_conversations", 0),
            "cached_items": metrics["gauges"].get("cached_items", 0)
        }


# 全局指标收集器实例
_metrics_collector: Optional[MetricsCollector] = None


def get_metrics_collector() -> MetricsCollector:
    """获取全局指标收集器"""
    global _metrics_collector
    if _metrics_collector is None:
        _metrics_collector = MetricsCollector()
    return _metrics_collector


# 便利装饰器
def monitor_performance(operation_name: str, labels: Dict[str, str] = None):
    """性能监控装饰器"""
    return get_metrics_collector().timing_decorator(operation_name, labels)


@asynccontextmanager
async def monitor_async_operation(operation_name: str, labels: Dict[str, str] = None):
    """异步操作监控上下文管理器"""
    async with get_metrics_collector().time_operation(operation_name, labels):
        yield


# 特定操作的便利函数
def record_http_request(method: str, path: str, status_code: int, duration: float):
    """记录HTTP请求"""
    collector = get_metrics_collector()
    labels = {"method": method, "path": path, "status": str(status_code)}

    collector.counter("http_requests_total").increment()
    collector.histogram("http_request_duration").observe(duration, labels)

    if status_code >= 400:
        collector.counter("errors_total").increment()


def record_telegram_api_call(method: str, duration: float, success: bool = True):
    """记录Telegram API调用"""
    collector = get_metrics_collector()
    labels = {"method": method, "success": str(success)}

    collector.counter("telegram_api_calls_total").increment()
    collector.histogram("telegram_api_duration").observe(duration, labels)

    if not success:
        collector.counter("errors_total").increment()


def record_database_operation(operation: str, duration: float, success: bool = True):
    """记录数据库操作"""
    collector = get_metrics_collector()
    labels = {"operation": operation, "success": str(success)}

    collector.counter("database_operations_total").increment()
    collector.histogram("database_operation_duration").observe(duration, labels)

    if not success:
        collector.counter("errors_total").increment()


def record_message_processing(message_type: str, duration: float, success: bool = True):
    """记录消息处理"""
    collector = get_metrics_collector()
    labels = {"type": message_type, "success": str(success)}

    collector.counter("message_processing_total").increment()
    collector.histogram("message_processing_duration").observe(duration, labels)

    if not success:
        collector.counter("errors_total").increment()


def update_active_conversations(count: int):
    """更新活跃对话数量"""
    get_metrics_collector().gauge("active_conversations").set(count)


def update_cached_items(count: int):
    """更新缓存项数量"""
    get_metrics_collector().gauge("cached_items").set(count)