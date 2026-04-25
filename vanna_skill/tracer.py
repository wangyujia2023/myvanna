"""
调用链追踪器 - 完整记录每次问数的链路信息
每次 generate_sql() 调用产生一条 RequestTrace，包含所有中间步骤
"""
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Deque, Dict, List, Optional


# ── 单个步骤 ─────────────────────────────────────────────────────────────────
@dataclass
class Step:
    name: str           # 步骤名称
    status: str = "running"          # running / ok / error / cached
    start_ms: float = field(default_factory=lambda: time.time() * 1000)
    end_ms: float = 0.0
    inputs: Dict[str, Any] = field(default_factory=dict)
    outputs: Dict[str, Any] = field(default_factory=dict)
    note: str = ""      # 补充说明（如 CACHED、fallback model 等）
    error: str = ""

    def finish(self, status: str = "ok", outputs: dict = None,
               note: str = "", error: str = ""):
        self.end_ms = time.time() * 1000
        self.status = status
        if outputs:
            self.outputs = outputs
        self.note = note
        self.error = error

    @property
    def duration_ms(self) -> float:
        end = self.end_ms if self.end_ms else time.time() * 1000
        return end - self.start_ms

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "status": self.status,
            "duration_ms": round(self.duration_ms, 1),
            "inputs": self.inputs,
            "outputs": self.outputs,
            "note": self.note,
            "error": self.error,
        }


# ── 一次完整请求的追踪 ────────────────────────────────────────────────────────
@dataclass
class RequestTrace:
    trace_id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    question: str = ""
    start_ms: float = field(default_factory=lambda: time.time() * 1000)
    steps: List[Step] = field(default_factory=list)
    final_sql: str = ""
    status: str = "running"   # running / ok / error
    error: str = ""
    end_ms: float = 0.0
    model_used: str = ""

    # ── 步骤管理 ──────────────────────────────────────────────────────────────
    def begin_step(self, name: str, inputs: dict = None) -> Step:
        s = Step(name=name, inputs=inputs or {})
        self.steps.append(s)
        return s

    def finish(self, sql: str = "", error: str = ""):
        self.end_ms = time.time() * 1000
        self.final_sql = sql
        self.status = "error" if error else "ok"
        self.error = error

    @property
    def total_ms(self) -> float:
        end = self.end_ms if self.end_ms else time.time() * 1000
        return end - self.start_ms

    @property
    def created_at(self) -> str:
        return datetime.fromtimestamp(self.start_ms / 1000).strftime("%H:%M:%S")

    # ── 序列化为适合前端展示的树形结构 ──────────────────────────────────────
    def to_dict(self) -> dict:
        return {
            "trace_id": self.trace_id,
            "question": self.question,
            "created_at": self.created_at,
            "status": self.status,
            "total_ms": round(self.total_ms, 1),
            "model_used": self.model_used,
            "final_sql": self.final_sql,
            "error": self.error,
            "steps": [s.to_dict() for s in self.steps],
        }

    # ── 纯文本日志（适合写日志文件）─────────────────────────────────────────
    def to_log_lines(self) -> List[str]:
        icon = {"ok": "✓", "error": "✗", "running": "…"}.get(self.status, "?")
        lines = [
            f"[{self.created_at}] Trace {self.trace_id} {icon}  "
            f"({self.total_ms:.0f}ms)  Q: {self.question[:60]}"
        ]
        for i, s in enumerate(self.steps):
            st_icon = {"ok": "✓", "error": "✗", "cached": "○",
                       "running": "…"}.get(s.status, "?")
            note = f"  [{s.note}]" if s.note else ""
            err = f"  ERR:{s.error}" if s.error else ""
            lines.append(
                f"  {i+1}. {st_icon} {s.name:<28} {s.duration_ms:>6.0f}ms{note}{err}"
            )
        if self.final_sql:
            lines.append(f"  SQL: {self.final_sql[:120]}")
        return lines


# ── 全局追踪管理器 ────────────────────────────────────────────────────────────
class TraceManager:
    """
    线程安全的追踪管理器：
    - 保留最近 N 条 trace（内存）
    - 提供统计摘要
    """

    def __init__(self, maxlen: int = 500):
        self._traces: Deque[RequestTrace] = deque(maxlen=maxlen)
        self._index: Dict[str, RequestTrace] = {}
        self._lock = threading.Lock()

    def start(self, question: str) -> RequestTrace:
        t = RequestTrace(question=question)
        with self._lock:
            self._traces.append(t)
            self._index[t.trace_id] = t
        return t

    def get(self, trace_id: str) -> Optional[RequestTrace]:
        return self._index.get(trace_id)

    def recent(self, n: int = 50) -> List[RequestTrace]:
        with self._lock:
            lst = list(self._traces)
        return list(reversed(lst))[:n]

    def stats(self) -> dict:
        with self._lock:
            lst = list(self._traces)
        if not lst:
            return {"total": 0}
        ok = sum(1 for t in lst if t.status == "ok")
        err = sum(1 for t in lst if t.status == "error")
        durations = [t.total_ms for t in lst if t.end_ms > 0]
        avg_ms = sum(durations) / len(durations) if durations else 0
        return {
            "total": len(lst),
            "ok": ok,
            "error": err,
            "success_rate": f"{ok/(ok+err)*100:.1f}%" if (ok + err) > 0 else "N/A",
            "avg_ms": round(avg_ms, 0),
            "p95_ms": round(sorted(durations)[int(len(durations) * 0.95)]
                            if len(durations) > 1 else 0, 0),
        }


# 全局单例
tracer = TraceManager()
