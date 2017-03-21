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
package org.apache.flink.python.api.jython;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.python.core.Py;
import org.python.core.PyObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.python.core.PySystemState;
import org.python.util.PythonInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Properties;


public class UtilityFunctions {
	private static boolean jythonInitialized = false;
	private static PythonInterpreter pythonInterpreter;
	private static final Logger LOG = LoggerFactory.getLogger(UtilityFunctions.class);

	private UtilityFunctions() {
	}

	public static class SerializerMap<IN> implements MapFunction<IN, PyObject> {
		private static final long serialVersionUID = 1582769662549499373L;

		@Override
		public PyObject map(IN value) throws Exception {
			return Py.java2py(value);
		}
	}

	public static PyObject adapt(Object o) {
		if (o instanceof PyObject) {
			return (PyObject)o;
		}
		return  Py.java2py(o);
	}

	public static synchronized Object smartFunctionDeserialization(RuntimeContext runtimeCtx, byte[] serFun) throws IOException, ClassNotFoundException, InterruptedException {
		try {
			return SerializationUtils.deserializeObject(serFun);
		} catch (Exception e) {
			String path = runtimeCtx.getDistributedCache().getFile(PythonEnvironmentConfig.FLINK_PYTHON_DC_ID).getAbsolutePath();

			initPythonInterpreter(path, new String[]{""});

			PySystemState pySysStat = Py.getSystemState();
			pySysStat.path.add(0, path);

			String scriptFullPath = path + File.separator + PythonEnvironmentConfig.FLINK_PYTHON_PLAN_NAME;
			LOG.debug("Execute python script, path=" + scriptFullPath);
			pythonInterpreter.execfile(scriptFullPath);

			pySysStat.path.remove(path);

			return SerializationUtils.deserializeObject(serFun);
		}
	}

	public static void initAndExecPythonScript(File scriptFullPath, String[] args) {
		initPythonInterpreter(scriptFullPath.getParent(), args);
		pythonInterpreter.execfile(scriptFullPath.getAbsolutePath());
	}

	private static synchronized void initPythonInterpreter(String pythonPath, String[] args) {
		if (!jythonInitialized) {
			LOG.debug("Init python interpreter, path=" + pythonPath);
			Properties postProperties = new Properties();
			postProperties.put("python.path", pythonPath);
			PythonInterpreter.initialize(System.getProperties(), postProperties, args);

			pythonInterpreter = new PythonInterpreter();
			pythonInterpreter.setErr(System.err);
			pythonInterpreter.setOut(System.out);

			jythonInitialized = true;
		}
	}
}
