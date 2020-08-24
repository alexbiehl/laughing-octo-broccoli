import asyncio
import os
import cloudpickle
import functools
import atexit
import protocol.redpy_pb2
from concurrent.futures import ThreadPoolExecutor as TPE

import logging
lh = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(message)s')
lh.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(lh)

class Invocation:

    def __init__(self, function, args = None, kwargs = None):
        self._function = function
        self._args = [] if args is None else args
        self._kwargs = {} if kwargs is None else kwargs
        return

    def invoke(self):
        logger.debug(f'Inside invoke...About to return function...')
        logger.info(f'this is self...f={self._function}, args={self._args}, kwargs={self._kwargs}...')
        f = self._function(*self._args, **self._kwargs)
        logger.debug(f'Inside invoke...Returning function...')
        return f



class TaskFunction:

    def __init__(self, function):
        self._function = function
        self.function = function
        self._function_name = (self._function.__module__ + "." + self._function.__name__)
        return

    def __call__(self, *args, **kwargs):
        raise TypeError("Tasks cannot be called directly. Instead "
                        "of running '{}()', try '{}.spawn()'.".format(
                            self._function_name, self._function_name))

    def spawn(self, redpy, *args, **kwargs):
        return redpy.submit_task(self._function, args = args, kwargs = kwargs)


# Decorator for python functions that can be run using red.
def task(func):
    return TaskFunction(func)


class RedpyProtocol(asyncio.SubprocessProtocol):

    def __init__(self, done_signal, loop):

        # Python seems to be very picky with its EventLoops.
        # Make sure to schedule everything happening in Redpy
        # onto the same loop.
        self._loop = loop
        self._loop.set_default_executor(TPE())
        # Transport is giving us STDIN and STDOUT of RedPy Rust
        # process. It's being set by connection_made callback.
        # Always assert this is present.
        self._transport = None

        # Once we are done we can shutdown the whole shebang.
        self._done_signal = done_signal

        # We read from the Pyred Rust process's STDOUT in an event driven
        # manner. This buffer captures the received bytes for the next message(s).
        self._receive_buf = bytearray()

        # We assign task_ids when scheduling tasks
        self._next_task_id = 0

        # A mapping from ongoing tasks to futures blocking on the availability
        # of the result
        self._submitted_tasks = {}

        super().__init__()

    # Take a task and submit it to Redpy
    def submit_task(self, task):
        assert(self._transport is not None)

        result_future = self._loop.create_future()

        # TODO Check for overflow
        task_id = self._next_task_id
        self._next_task_id = self._next_task_id + 1
        self._submitted_tasks[task_id] = result_future

        input = protocol.redpy_pb2.Input()
        input.spawn_task.task_id = task_id
        input.spawn_task.serialized_task = cloudpickle.dumps(task)

        encoded_message = input.SerializeToString()
        message_prefix = len(encoded_message).to_bytes(8, byteorder = 'big', signed = False)

        stdin = self._transport.get_pipe_transport(0)
        logger.debug(f'Submitting task with {task_id}...')
        stdin.write(message_prefix)
        stdin.write(encoded_message)

        return result_future

    # Take a task result and submit it to Redpy
    async def emit_result(self, task_id, result):
        assert(self._transport is not None)

        input = protocol.redpy_pb2.Input()
        input.task_result.task_id = task_id
        input.task_result.serialized_result = cloudpickle.dumps(result)

        encoded_message = input.SerializeToString()
        message_prefix = len(encoded_message).to_bytes(8, byteorder = 'big', signed = False)

        stdin = self._transport.get_pipe_transport(0)
        #logger.info(f'Emitting task results for task_id={task_id}...')
        #logger.info(f'Task results prefix is {message_prefix} and message is {encoded_message}...')
        stdin.write(message_prefix)
        stdin.write(encoded_message)

    def connection_made(self, transport):
        self._transport = transport
        super().connection_made(transport)

    def pipe_data_received(self, fd, data):
        if fd == 1: # STDOUT
            self._receive_buf += data
            while len(self._receive_buf) > 8:
                # Splice the first 8 bytes and interpret them
                # as message length
                message_prefix = self._receive_buf[0:8]
                message_size = int.from_bytes(message_prefix, byteorder = 'big')

                if len(self._receive_buf) - 8 >= message_size:

                    # Cool, we have a complete message in our buffer! Mind
                    # the length prefix and extract the payload. Once done,
                    # remove from receive buffer
                    message_bytes = self._receive_buf[8:message_size + 8]
                    self._receive_buf[0:message_size + 8] = []

                    decoded = protocol.redpy_pb2.Output()
                    decoded.ParseFromString(message_bytes)

                    # Let's not block decoder with task execution
                    #logger.info(f'Inside pipe_data_received -- calling create_task with {decoded}...')
                    self._loop.create_task(self.handle_output(decoded))
                else:
                    # Break out the loop, we have no enough bytes for
                    # the next message yet
                    break

    def process_exited(self):
        logger.info(f'PROCESS EXITED!')
        self._transport.close()
        self._done_signal.set_result(None)

    # Whenever we receive an output from Redpy we call this function
    async def handle_output(self, output):
        logger.debug(f'output is {output}...')
        if output.WhichOneof("output") == "task_result":
            logger.debug(f'Inside handle_output -- output == task_result...')

            task_id = output.task_result.task_id
            serialized_result = output.task_result.serialized_result
            result = cloudpickle.loads(serialized_result)

            result_future = self._submitted_tasks[task_id]
            del self._submitted_tasks[task_id]
            #logger.info(f'Got result for task_id = {task_id}...')
            #logger.info(f'Result is -- {result}')
            result_future.set_result(result)
            return

        elif output.WhichOneof("output") == "spawn_task":
            logger.debug(f'Inside handle_output -- output == spawn_task...')

            task_id = output.spawn_task.task_id
            serialized_task = output.spawn_task.serialized_task

            invocation = cloudpickle.loads(serialized_task)
            logger.info(f'Got invocation -- {type(invocation)}')
            logger.debug(f'Inside handle_output -- calling invoke for task_id = {task_id}...')

            result = await self._loop.run_in_executor(None, invocation.invoke)
            await self.emit_result(task_id, result)

            logger.debug(f'result is {result}')
        else:
            raise "Unknown Zhopa"

class Redpy:

    def __init__(self, mode, loop = asyncio.get_event_loop()):
        done_fut = asyncio.Future(loop = loop)
        self._done_fut = done_fut
        self._mode = mode
        self._loop = loop
        self._proc = None
        self._transport = None
        self._protocol = RedpyProtocol(done_fut, loop)

    async def initialize(self):
        if self._proc is None:
            self._proc = self._loop.subprocess_exec(
                lambda: self._protocol,
                "target/debug/pyred",
                self._mode,
                stdin = asyncio.subprocess.PIPE,
                stdout = asyncio.subprocess.PIPE,
                stderr = None # Inherit
            )

            # Wait for the process to come up.
            transport, _protocol = await self._proc
            self._transport = transport

        return

    async def block_until_done(self):
        await self._done_fut
        return

    def __del__(self):
        if self._proc is not None:
            self._transport.close()

    def submit_task(self, function, args = None, kwargs = None):
        return self._protocol.submit_task(
            Invocation(function, args = args, kwargs = kwargs)
        )
