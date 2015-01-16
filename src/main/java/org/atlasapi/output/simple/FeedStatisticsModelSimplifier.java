package org.atlasapi.output.simple;

import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.feeds.youview.statistics.FeedStatistics;
import org.atlasapi.output.Annotation;


public class FeedStatisticsModelSimplifier implements ModelSimplifier<FeedStatistics, org.atlasapi.feeds.youview.statistics.simple.FeedStatistics> {
    

    public FeedStatisticsModelSimplifier() {
    }

    @Override
    public org.atlasapi.feeds.youview.statistics.simple.FeedStatistics simplify(FeedStatistics model,
            Set<Annotation> annotations, ApplicationConfiguration config) {
        org.atlasapi.feeds.youview.statistics.simple.FeedStatistics feedStats = new org.atlasapi.feeds.youview.statistics.simple.FeedStatistics();
        
        feedStats.setPublisher(model.publisher());
        feedStats.setQueueSize(model.queueSize());
        feedStats.setUpdateLatency(model.updateLatency());
        
        return feedStats;
    }
}
