import asyncio
import red.scheduler
import red.core
import time
from random import randint as rint

@red.core.task
def item_elab(step):
    time.sleep(16)
    return f'1<-{step}'

@red.core.task
def item_opt(elab_out):
    time.sleep(rint(1, 3))
    return f'2.1<-{elab_out}'

@red.core.task
def item_inv(elab_out):
    time.sleep(16)
    return f'2.2<-{elab_out}'

@red.core.task
def unconstrained_sim(elab_out, inv_out, opt_out):
    time.sleep(rint(1, 12))
    return f'3<-{elab_out}+{inv_out}+{opt_out}'

async def main(loop = None):
    scheduler = await red.scheduler.init_scheduler(loop = loop)
    await pipeline(scheduler, loop)

async def pipeline(scheduler, loop):

    items = list()

    for item in range(1, 300):
        items.append(middlemile(scheduler, item))

    await asyncio.wait(items, return_when = asyncio.ALL_COMPLETED)


async def middlemile(scheduler, item):

    elab = item_elab.spawn(scheduler, "0")
    inv = item_inv.spawn(scheduler, await elab)
    policy = item_opt.spawn(scheduler, await elab)

    metrics = unconstrained_sim.spawn(
        scheduler,
        await elab,
        await inv,
        await policy
    )

    print(await metrics)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main(loop))
    finally:
        loop.close()
