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
from utils import constants
from utils.python_test_base import TestBase
from utils.pygeneratorbase import PyGeneratorBase
from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction
from org.apache.flink.api.java.functions import KeySelector
from org.apache.flink.streaming.api.windowing.time.Time import milliseconds


class Generator(PyGeneratorBase):
    def __init__(self, msg, num_iters):
        super(Generator, self).__init__(num_iters)
        self._msg = msg
    def do(self, ctx):
        ctx.collect(self._msg)


class Tokenizer(FlatMapFunction):
    def flatMap(self, value, collector):
        collector.collect((1, value))


class Sum(ReduceFunction):
    def reduce(self, input1, input2):
        count1, val1 = input1
        count2, val2 = input2
        return (count1 + count2, val1)


class Selector(KeySelector):
    def getKey(self, input):
        return 1

class Main(TestBase):
    def __init__(self):
        super(Main, self).__init__()

    def run(self):
        env = self._get_execution_environment()
        seq1 = env.create_python_source(Generator(msg='Hello', num_iters=constants.NUM_ITERATIONS_IN_TEST))
        seq2 = env.create_python_source(Generator(msg='World', num_iters=constants.NUM_ITERATIONS_IN_TEST))
        seq3 = env.create_python_source(Generator(msg='Happy', num_iters=constants.NUM_ITERATIONS_IN_TEST))

        seq1.union(seq2, seq3) \
            .flat_map(Tokenizer()) \
            .key_by(Selector()) \
            .time_window(milliseconds(10)) \
            .reduce(Sum()) \
            .print()

        result = env.execute("My python union stream test", True)
        print("Job completed, job_id={}".format(result.jobID))


if __name__ == '__main__':
    Main().run()
