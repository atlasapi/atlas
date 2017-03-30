package org.atlasapi.output.simple;

import java.util.Set;

import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.feeds.youview.statistics.FeedStatistics;
import org.atlasapi.output.Annotation;


public class FeedStatisticsModelSimplifier implements ModelSimplifier<FeedStatistics, org.atlasapi.feeds.youview.statistics.simple.FeedStatistics> {
    

    public FeedStatisticsModelSimplifier() {
    }

    @Override
    public org.atlasapi.feeds.youview.statistics.simple.FeedStatistics simplify(
            FeedStatistics model,
            Set<Annotation> annotations,
            Application application
    ) {
        org.atlasapi.feeds.youview.statistics.simple.FeedStatistics feedStats =
                new org.atlasapi.feeds.youview.statistics.simple.FeedStatistics();
        
        feedStats.setPublisher(model.publisher());
        feedStats.setQueueSize(model.queueSize());
        feedStats.setUpdateLatency(model.updateLatency());
        feedStats.setSuccessfulTasks(model.successfulTasks());
        feedStats.setUnsuccessfulTasks(model.unsuccessfulTasks());
        
        return feedStats;
    }
}
