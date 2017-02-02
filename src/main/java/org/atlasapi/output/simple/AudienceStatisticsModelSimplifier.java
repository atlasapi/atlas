package org.atlasapi.output.simple;

import java.util.List;
import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.simple.AudienceStatistics;
import org.atlasapi.media.entity.simple.Demographic;
import org.atlasapi.output.Annotation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;


public class AudienceStatisticsModelSimplifier implements ModelSimplifier<org.atlasapi.media.entity.AudienceStatistics, AudienceStatistics> {

    private final DemographicsModelSimplifier demographicsModelSimplifier = new DemographicsModelSimplifier();
    
    @Override
    public AudienceStatistics simplify(org.atlasapi.media.entity.AudienceStatistics model,
            Set<Annotation> annotations, ApplicationConfiguration config) {
        if (model == null) {
            return null;
        }
        AudienceStatistics audienceStats = new AudienceStatistics();
        audienceStats.setDemographics(toDemographics(model.getDemographics(), annotations, config));
        audienceStats.setTotalViewers(model.getTotalViewers());
        audienceStats.setViewingShare(model.getViewingShare());
        return audienceStats;
    }
    
    private List<Demographic> toDemographics(Iterable<org.atlasapi.media.entity.Demographic> demographics, 
            Set<Annotation> annotations, ApplicationConfiguration config) {
        Builder<Demographic> simple = ImmutableList.builder();
        for(org.atlasapi.media.entity.Demographic demographic : demographics) {
            simple.add(demographicsModelSimplifier.simplify(demographic, annotations, config));
        }
        return simple.build();
    } 

}
