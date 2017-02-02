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

package org.atlasapi.output.rdf;

import static com.hp.hpl.jena.ontology.OntModelSpec.OWL_MEM;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.output.Annotation;
import org.springframework.beans.factory.InitializingBean;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import com.hp.hpl.jena.ontology.OntDocumentManager;
import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;

public abstract class JenaRdfTranslator<EntityType extends Iterable<?>> extends AbstractRdfTranslator<EntityType, OntModel, Resource, Property, Resource, String> implements InitializingBean {

	private OntDocumentManager documentMgr;

	/**
	 * Define a particular output rendering of the Jena model to a stream.
	 */
	protected abstract void writeOut(OntModel rdf, OutputStream stream);

	public JenaRdfTranslator(TypeMap typeMap) {
		this.typeMap = typeMap;
	}

	public void initOntologies() {
		for (Map.Entry<String, String> ontology : ontologyMap.entrySet()) {
			URL ontologyUrl;
			try {
				ontologyUrl = Resources.getResource(ontology.getValue());
			} catch (Exception e) {
				throw new IllegalStateException("Failed to load ontology file " + ontology.getValue(), e);
			}

			documentMgr.addAltEntry(ontology.getKey(), ontologyUrl.toString());
		}
	}
    
    public void loadOntologies(OntModel model) {
        for (String ontology : ontologyMap.keySet()) {
            documentMgr.loadImport(model, ontology);
        }
    }

    public void loadPrefixes(OntModel model) {
        for (Map.Entry<String, String> nsPrefix : nsPrefixes.entrySet()) {
            model.setNsPrefix(nsPrefix.getValue(), nsPrefix.getKey());
        }
    }

	@Override
    public void writeTo(HttpServletRequest request, HttpServletResponse response, EntityType graph, Set<Annotation> annotations, ApplicationConfiguration config) throws IOException {
		OntModel rdf = ModelFactory.createOntologyModel(OWL_MEM);

		loadOntologies(rdf);
	    loadPrefixes(rdf);
		   
		Map<Object, Resource> references = Maps.newHashMap();

		List<Object> workList = Lists.<Object>newArrayList(graph);
		for (int i = 0; i < workList.size(); i++) {
		    Object bean = workList.get(i);
		    
			addBean(rdf, workList, references, bean);
		}

		writeOut(rdf, response.getOutputStream());
	}

	public void afterPropertiesSet() throws Exception {
		documentMgr = OntDocumentManager.getInstance();
		documentMgr.setProcessImports(false);
		initOntologies();
	}

    @Override
    protected Property createTypedProperty(OntModel model, Resource resource, String ns, String uri) {
        return model.createProperty(ns + uri);
    }

    @Override
    protected Resource createTypedReference(OntModel model, String uri) {
        return model.createResource(uri);
    }

    @Override
    protected Resource createTypedResource(OntModel model, String uri) {
        return model.createResource(uri);
    }

    @Override
    protected void createReferenceStatement(
            OntModel model, 
            Resource subject,
            Property predicate, 
            Resource object) {
        subject.addProperty(predicate, object);   
    }

    @Override
    protected void createLiteralStatement(
            OntModel model, 
            Resource subject,
            Property predicate, 
            String literal) {
        subject.addProperty(predicate, literal);   
    }

    @Override
    protected Resource createTypedResource(OntModel model, Resource ref) {
        return ref;
    }

    @Override
    protected String createTypedLiteral(OntModel model, Object value) {
        return value.toString();
    }
}
