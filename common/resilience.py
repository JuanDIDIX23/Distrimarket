"""Patrones de tolerancia a fallos: Circuit Breaker, reintentos con backoff exponencial y jitter."""
import asyncio
import random
import time
from dataclasses import dataclass, field


class CircuitOpenError(Exception):
    """Se lanza cuando el circuito está abierto y la llamada se rechaza sin intentar."""


@dataclass
class CircuitBreaker:
    """Circuit breaker de tres estados (CLOSED -> OPEN -> HALF_OPEN).

    - CLOSED: las llamadas pasan; se cuentan fallos consecutivos.
    - OPEN: tras `failure_threshold` fallos, se rechazan llamadas durante `reset_timeout` s.
    - HALF_OPEN: pasado el tiempo, se permite una llamada de prueba; si tiene éxito se cierra.
    """
    name: str
    failure_threshold: int = 5
    reset_timeout: float = 10.0
    state: str = "CLOSED"
    failures: int = 0
    opened_at: float = 0.0
    total_rejected: int = 0
    transitions: list = field(default_factory=list)

    def _transition(self, new_state: str) -> None:
        if new_state != self.state:
            self.transitions.append((round(time.time(), 3), self.state, new_state))
            self.transitions = self.transitions[-20:]
            self.state = new_state

    def allow(self) -> bool:
        if self.state == "OPEN":
            if time.monotonic() - self.opened_at >= self.reset_timeout:
                self._transition("HALF_OPEN")
                return True
            self.total_rejected += 1
            return False
        return True

    def on_success(self) -> None:
        self.failures = 0
        self._transition("CLOSED")

    def on_failure(self) -> None:
        self.failures += 1
        if self.state == "HALF_OPEN" or self.failures >= self.failure_threshold:
            self.opened_at = time.monotonic()
            self._transition("OPEN")

    def snapshot(self) -> dict:
        return {
            "name": self.name,
            "state": self.state,
            "consecutive_failures": self.failures,
            "rejected_calls": self.total_rejected,
            "recent_transitions": self.transitions[-5:],
        }


async def retry_async(fn, *, attempts: int = 3, base_delay: float = 0.1,
                      max_delay: float = 1.0, retry_on: tuple = (Exception,)):
    """Ejecuta `fn` con reintentos y backoff exponencial con jitter completo."""
    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            return await fn()
        except retry_on as exc:  # noqa: PERF203
            last_exc = exc
            if attempt == attempts:
                break
            delay = min(max_delay, base_delay * 2 ** (attempt - 1))
            await asyncio.sleep(random.uniform(0, delay))
    raise last_exc
