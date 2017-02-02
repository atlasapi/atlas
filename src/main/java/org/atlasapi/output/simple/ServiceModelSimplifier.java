package org.atlasapi.output.simple;

import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.Service;
import org.atlasapi.output.Annotation;

import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;


public class ServiceModelSimplifier extends DescribedModelSimplifier<Service, org.atlasapi.media.entity.simple.Service> {

    public ServiceModelSimplifier(ImageSimplifier imageSimplifier) {
        super(imageSimplifier, SubstitutionTableNumberCodec.lowerCaseOnly(), null);
    }

    @Override
    public org.atlasapi.media.entity.simple.Service simplify(Service model,
            Set<Annotation> annotations, ApplicationConfiguration config) {
        org.atlasapi.media.entity.simple.Service simpleModel = new org.atlasapi.media.entity.simple.Service();
        copyBasicDescribedAttributes(model, simpleModel, annotations);
        
        return simpleModel;
    }

}
