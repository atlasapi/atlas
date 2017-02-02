package org.atlasapi.output;

import static com.google.api.client.util.Preconditions.checkNotNull;

import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.feeds.youview.statistics.FeedStatistics;
import org.atlasapi.feeds.youview.statistics.FeedStatisticsQueryResult;
import org.atlasapi.output.simple.ModelSimplifier;

public class SimpleFeedStatisticsModelWriter extends TransformingModelWriter<Iterable<FeedStatistics>, FeedStatisticsQueryResult> {

    private final ModelSimplifier<FeedStatistics, org.atlasapi.feeds.youview.statistics.simple.FeedStatistics> feedStatsSimplifier;

    public SimpleFeedStatisticsModelWriter(AtlasModelWriter<FeedStatisticsQueryResult> delegate, 
            ModelSimplifier<FeedStatistics, org.atlasapi.feeds.youview.statistics.simple.FeedStatistics> feedStatsSimplifier) {
        super(delegate);
        this.feedStatsSimplifier = checkNotNull(feedStatsSimplifier);
    }
    
    @Override
    protected FeedStatisticsQueryResult transform(Iterable<FeedStatistics> feedStats, Set<Annotation> annotations, ApplicationConfiguration config) {
        FeedStatisticsQueryResult result = new FeedStatisticsQueryResult();
        for (FeedStatistics stats : feedStats) {
            result.add(feedStatsSimplifier.simplify(stats, annotations, config));
        }
        return result;
    }

}
