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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.python.core.PyObject;

import java.io.IOException;

public class PythonApplyFunction extends RichWindowFunction<PyObject, PyObject, PyKey, Window> {
	private static final long serialVersionUID = 577032239468987781L;

	private final byte[] serFun;
	private transient WindowFunction<PyObject, Object, Object, Window> fun;
	private transient PythonCollector collector;

	public PythonApplyFunction(WindowFunction<PyObject, Object, Object, Window> fun) throws IOException {
		this.serFun = SerializationUtils.serializeObject(fun);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		this.fun = (WindowFunction<PyObject, Object, Object, Window>) UtilityFunctions.smartFunctionDeserialization(
			getRuntimeContext(), this.serFun);
		if (this.fun instanceof RichWindowFunction) {
			final RichWindowFunction winFun = (RichWindowFunction)this.fun;
			winFun.setRuntimeContext(getRuntimeContext());
			winFun.open(parameters);
		}
	}

	@Override
	public void close() throws Exception {
		if (this.fun instanceof RichWindowFunction) {
			((RichWindowFunction)this.fun).close();
		}
	}

	@Override
	public void apply(PyKey key, Window window, Iterable<PyObject> values, Collector<PyObject> out) throws Exception {
		if (this.fun == null) {
			this.fun = (WindowFunction<PyObject, Object, Object, Window>) SerializationUtils.deserializeObject(this.serFun);
			this.collector = new PythonCollector();
		}
		this.collector.setCollector(out);
		this.fun.apply(key.getData(), window, values, this.collector);
	}
}