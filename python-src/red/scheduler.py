import core

# Initialize a Redpy instance for doing executors work
async def init_scheduler(loop = None):
    redpy = core.Redpy("scheduler", loop = loop)
    await redpy.initialize()
    return redpy
