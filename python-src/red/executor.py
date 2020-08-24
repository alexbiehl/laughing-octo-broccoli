import asyncio
import core

# Initialize a Redpy instance for doing executors work
async def init_executor(loop = None):
    redpy = core.Redpy("executor", loop = loop)
    await redpy.initialize()
    return redpy

async def run_executor(loop = None):
    redpy = await init_executor(loop = loop)
    await redpy.block_until_done()
    return

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(run_executor(loop))
    finally:
        loop.close()
