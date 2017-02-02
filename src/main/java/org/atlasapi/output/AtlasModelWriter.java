/* Copyright 2009 British Broadcasting Corporation
 
Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. */

package org.atlasapi.output;

import java.io.IOException;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.v3.ApplicationConfiguration;


public interface AtlasModelWriter<T> {
	
	public void writeTo(HttpServletRequest request, HttpServletResponse response, T graph, Set<Annotation> annotations, ApplicationConfiguration config) throws IOException;
	
	public void writeError(HttpServletRequest request, HttpServletResponse response, AtlasErrorSummary exception) throws IOException;
	
}
