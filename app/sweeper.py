"""撤销事件过期在飞投递的独立后台清理（API 进程）。

撤销（repository.cancel_event）在撤销事务内只终结 pending 与撤销时刻租约
已过期的 in_flight 投递；撤销时租约尚未过期的在飞投递保留不动（撤销前已
认领的请求允许飞完，成功仍记 succeeded），待租约到期后由
repository.sweep_expired_cancelled_in_flight 终结为 cancelled 并清空租约。

该清理同时由两条互不影响的路径驱动：

1. Worker 每次认领事务开头（claim_delivery）；
2. 本模块在 API 进程内的周期巡检（cancel_sweep_loop）。

只有路径 1 时，Worker 全部停止（API 仍存活、无人查询）便再无任何驱动，
撤销事件的“后到期”在飞投递会永远滞留 in_flight（旧租约残留），
GET /api/v1/events/{id}/deliveries 长期显示 in_flight 与旧租约。路径 2
独立于 Worker 运行：只要 API 进程存活，到期投递最终都会转为 cancelled。

清理的并发与幂等语义全部在仓储层保证（见 repository 文件头“事件撤销”）：

- 只翻转 lease_expires_at < now() 的行：租约未过期的在飞请求绝不被提前
  取消，其成功结算仍记 succeeded；
- 与 record_attempt / settle_delivery 的交错由租约栅栏裁决：清理先提交
  则旧租约令牌的回写读到 leased_by = NULL 即作废（不新增尝试、不覆盖
  cancelled 终态）；回写先提交则尝试历史保留，清理随后仍把该过期行翻转；
- 幂等：不插入 delivery_attempts、不改 attempts_made、不产生新租约、
  不翻回 pending；重复执行不再命中；
- 不经过认领的候选选择（候选选择会过滤暂停/停用订阅），因此暂停/停用
  订阅下的滞留行同样被清理，其他事件的严格顺序不受影响。
"""
from __future__ import annotations

import asyncio
import logging

import asyncpg

from app import repository as repo
from app.config import settings

log = logging.getLogger("webhook.cancel_sweep")


async def sweep_until_idle(
    conn: asyncpg.Connection, *, limit: int
) -> int:
    """一轮清理：把“事件已撤销 + 租约已过期”的 in_flight 投递终结为 cancelled。

    返回本轮终结的总行数。批处理与续批策略：

    - 每批一个事务（sweep_expired_cancelled_in_flight 内部提交），批与批
      之间不持有任何锁，不会形成长事务；
    - 满批（== limit）说明剩余集合可能未扫完，立即同轮续批，直到短批
      （< limit）才结束本轮——剩余集合只减不增（cancelled 是终态，已撤销
      事件不会再产生新租约），循环必然终止；
    - 被并发 record_attempt/settle/认领持锁而 SKIP LOCKED 跳过的行仍在
      剩余集合中（清理不消费游标，每批都重新查询剩余集合），本轮跳过、
      由下一轮巡检（或锁释放后的下一批）回访终结。
    """
    total = 0
    while True:
        n = await repo.sweep_expired_cancelled_in_flight(conn, limit=limit)
        total += n
        if n < limit:
            return total


async def cancel_sweep_loop() -> None:
    """API 进程后台任务：按固定间隔巡检清理撤销事件的过期在飞投递。

    与 Worker 认领事务内的清理是同一仓储函数的两路驱动，互为冗余：
    Worker 存活时认领路径通常先清理；Worker 全部停止时本任务保证到期
    投递仍最终转为 cancelled 并清空租约。单轮失败只记录日志，下一轮继续。
    """
    from app.db import get_pool

    try:
        pool = get_pool()
    except RuntimeError:
        return
    interval = settings.cancel_sweep_interval_seconds
    batch = settings.cancel_sweep_batch_size
    log.info(
        "撤销事件过期在飞投递清理任务已启动（间隔 %ss，批大小 %s）",
        interval,
        batch,
    )
    while True:
        await asyncio.sleep(interval)
        try:
            async with pool.acquire() as conn:
                n = await sweep_until_idle(conn, limit=batch)
            if n:
                log.info("后台清理终结了 %s 条撤销事件的过期在飞投递", n)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - 后台巡检不能因单轮失败退出
            log.exception("过期在飞投递清理轮询失败")
