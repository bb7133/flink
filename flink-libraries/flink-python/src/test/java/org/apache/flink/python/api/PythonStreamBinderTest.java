/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.python.api;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.util.StreamingProgramTestBase;
import org.apache.flink.python.api.jython.PythonStreamBinder;

import java.io.File;
import java.io.FileNotFoundException;

public class PythonStreamBinderTest extends StreamingProgramTestBase {
	final private static String defaultPythonScriptName = "run_all_tests.py";
	final private static String pathToStreamingTests = "flink-libraries/flink-python/src/test/python/org/apache/flink/python/api/streaming";

	public PythonStreamBinderTest() {
	}

	public static void main(String[] args) throws Exception {
		File script = getPythonScriptPath(args);
		if (args.length == 0 || isFirstArgAnOption(args)) {
			args = prepend(args, script.getAbsolutePath());  // First argument in sys.argv is the script full path
		}
		PythonStreamBinder.main(args);
	}

	@Override
	public void testProgram() throws Exception {
		FileSystem fs = FileSystem.getLocalFileSystem();
		String path_to_test = fs.getWorkingDirectory().getPath()
								+ "/src/test/python/org/apache/flink/python/api/streaming/run_all_tests.py";
		String[] args = {path_to_test};
		this.main(args);

	}

	private static File getPythonScriptPath(String[] args) throws Exception {
		File script;
		if (args.length > 0) {
			if (isFirstArgAnOption(args)) {
				final ParameterTool params = ParameterTool.fromArgs(args);
				String scriptResource = params.get("script", defaultPythonScriptName);
				script = findStreamTestFile(scriptResource);
			} else {
				script = findStreamTestFile(args[0]);
			}
		} else {
			script = findStreamTestFile(defaultPythonScriptName);
		}
		return script;
	}

	private static File findStreamTestFile(String name) throws Exception {
		if (new File(name).exists()) {
			return new File(name);
		}
		FileSystem fs = FileSystem.getLocalFileSystem();
		FileStatus[] status = fs.listStatus(
			new Path(fs.getWorkingDirectory().toString()
				+ File.separator + pathToStreamingTests));
		for (FileStatus f : status) {
			String file_name = f.getPath().getName();
			if (file_name.equals(name)) {
				return new File(f.getPath().getPath());
			}
		}
		throw new FileNotFoundException();
	}

	private static boolean isFirstArgAnOption(String[] args) {
		return args.length > 0 && args[0].startsWith("-");
	}

	private static String[] prepend(String[] a, String prepended) {
		String[] c = new String[a.length + 1];
		c[0] = prepended;
		System.arraycopy(a, 0, c, 1, a.length);
		return c;
	}
}
