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
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Iterator;
import java.io.IOException;

public class PythonIteratorFunction extends RichSourceFunction<Object> {
	private static final long serialVersionUID = 6741748297048588334L;

	private final byte[] serFun;
	private transient Iterator<Object> fun;
	private volatile boolean isRunning = true;

	public PythonIteratorFunction(Iterator<Object> fun) throws IOException {
		this.serFun = SerializationUtils.serializeObject(fun);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		this.fun = (Iterator<Object>) UtilityFunctions.smartFunctionDeserialization(getRuntimeContext(), this.serFun);
	}

	@Override
	public void close() throws Exception {
	}

	@Override
	public void run(SourceContext<Object> ctx) throws Exception {
		while (isRunning && this.fun.hasNext()) {
			ctx.collect(this.fun.next());
		}
	}

	@Override
	public void cancel() {
		isRunning = false;
	}
}
