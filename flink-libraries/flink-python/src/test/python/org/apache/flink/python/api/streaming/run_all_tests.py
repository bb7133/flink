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
from os.path import dirname, join, basename
from glob import glob
import exclude
from org.apache.flink.runtime.client import JobExecutionException

tests = []
pwd = dirname(sys.argv[0])

if exclude.test_module_names:
    print("Excluded tests: {}\n".format(exclude.test_module_names))

for x in glob(join(pwd, 'test_*.py')):
    if not x.startswith('__'):
        test_module_name = basename(x)[:-3]
        if test_module_name not in exclude.test_module_names:
            tests.append(__import__(test_module_name, globals(), locals()))

failed_tests = []
for test in tests:
    print("Submitting job ... '{}'".format(test.__name__))
    try:
        test.Main().run()
        print("Job completed ('{}')\n".format(test.__name__))
    except JobExecutionException as ex:
        failed_tests.append(test.__name__)
        print("\n{}\n{}\n{}\n".format('#'*len(ex.message), ex.message, '#'*len(ex.message)))
    except:
        failed_tests.append(test.__name__)
        ex_type = sys.exc_info()[0]
        print("\n{}\n{}\n{}\n".format('#'*len(ex_type), ex_type, '#'*len(ex_type)))


if failed_tests:
    print("\nThe following tests were failed:")
    for failed_test in failed_tests:
        print("\t* " + failed_test)
    raise Exception("\nFailed test(s): {}".format(failed_tests))
else:
    print("\n*** All tests passed successfully ***")
