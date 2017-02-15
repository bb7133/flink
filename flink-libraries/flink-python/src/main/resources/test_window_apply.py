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
import sys
from pygeneratorbase import PyGeneratorBase
from org.apache.flink.streaming.api.functions.windowing import WindowFunction
from org.apache.flink.api.java.functions import KeySelector
from org.apache.flink.python.api.jython import PythonStreamExecutionEnvironment
from org.apache.flink.streaming.api.windowing.time.Time import seconds
from org.apache.flink.api.java.utils import ParameterTool


class Generator(PyGeneratorBase):
    def __init__(self, num_iters):
        super(Generator, self).__init__(num_iters)
        self._alternator = True

    def do(self, ctx):
        if self._alternator:
            ctx.collect(('key1', 'Any specific text here'))
        else:
            ctx.collect(('key2', 'Any other specific text here'))
        self._alternator = not self._alternator


class Selector(KeySelector):
    def getKey(self, input):
        return input[0]


class WindowSum(WindowFunction):
    def apply(self, key, window, values, collector):
        collector.collect((key, len(values)))


def main():
    params = ParameterTool.fromArgs(sys.argv[1:])
    env = PythonStreamExecutionEnvironment.create_local_execution_environment(params.getConfiguration())

    env.create_python_source(Generator(num_iters=7000)) \
        .key_by(Selector()) \
        .time_window(seconds(1)) \
        .apply(WindowSum()) \
        .print()

    env.execute()


if __name__ == '__main__':
    main()
    print("Job completed ({})\n".format(sys.argv))
