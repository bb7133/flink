################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import random
import sys
from org.apache.flink.api.common.functions import MapFunction
from org.apache.flink.streaming.api.collector.selector import OutputSelector
from org.apache.flink.streaming.api.functions.source import SourceFunction
from org.apache.flink.api.java.utils import ParameterTool
from org.apache.flink.python.api.jython import PythonStreamExecutionEnvironment

TARGET_VAL = 100
MAX_INT_START = 50


class Generator(SourceFunction):
    def __init__(self, num_iters=1000):
        self._running = True
        self._num_iters = num_iters

    def run(self, ctx):
        counter = 0
        while self._running and counter < self._num_iters:
            self.do(ctx)
            counter += 1

    def do(self, ctx):
        tup = (random.randrange(1, MAX_INT_START), random.randrange(1, MAX_INT_START))
        ctx.collect(tup)

    def cancel(self):
        self._running = False


class Fib(MapFunction):
    def map(self, value):
        return (value[0], value[1], value[3], value[2] + value[3], value[4] + 1)


class InPut(MapFunction):
    def map(self, value):
        return (value[0], value[1], value[0], value[1], 0)


class OutPut(MapFunction):
    def map(self, value):
        return ((value[0], value[1]), value[4])


class StreamSelector(OutputSelector):
    def select(self, value):
        return ["iterate"] if value[3] < TARGET_VAL else ["output"]


class Main(object):
    def __init__(self):
        super(Main, self).__init__()

    def run(self, argv):
        _params = ParameterTool.fromArgs(argv)
        env = PythonStreamExecutionEnvironment.create_local_execution_environment(_params.getConfiguration())

        # create input stream of integer pairs
        if "--input" in argv:
            try:
                file_path = _params.get("input")
                input_stream = env.read_text_file(file_path)
            except Exception as e:
                print(e)
                print ("Error in reading input file. Exiting...")
                sys.exit(5)
        else:
            input_stream = env.create_python_source(Generator(num_iters=1000))

        # create an iterative data stream from the input with 5 second timeout
        it = input_stream.map(InPut()).iterate(5000)

        # apply the step function to get the next Fibonacci number
        # increment the counter and split the output with the output selector
        step = it.map(Fib()).split(StreamSelector())
        it.close_with(step.select("iterate"))

        # to produce the final output select the tuples directed to the
        # 'output' channel then get the input pairs that have the greatest iteration counter
        # on a 1 second sliding window
        output = step.select("output")
        parsed_output = output.map(OutPut())
        if "--output" in argv:
            try:
                file_path = _params.get("output")
                parsed_output.write_as_text(file_path)
            except Execption as e:
                print (e)
                print ("Error in writing to output file. will print instead.")
                parsed_output.print()
        else:
            parsed_output.print()
        result = env.execute("Fibonacci Example (py)")
        print("Fibonacci job completed, job_id={}".format(result.jobID))


if __name__ == '__main__':
    Main().run(sys.argv[1:])
