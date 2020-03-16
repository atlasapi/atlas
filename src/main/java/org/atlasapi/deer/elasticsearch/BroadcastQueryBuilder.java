package org.atlasapi.deer.elasticsearch;

import com.metabroadcast.common.time.DateTimeZones;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.MultiValueMode;
import org.joda.time.DateTime;

import java.util.Date;

public class BroadcastQueryBuilder {

    public static QueryBuilder build(QueryBuilder childQuery, Float timeBoost) {
        Date minusThirtyDays = DateTime.now().minusDays(30).toDateTime(DateTimeZones.UTC).toDate();
        long minusThirtyDaysMillis = minusThirtyDays.toInstant().getEpochSecond() * 1000;
        long nowMilis = new Date().toInstant().getEpochSecond() * 1000;

        return QueryBuilders.functionScoreQuery(
                childQuery
        ).add(ScoreFunctionBuilders.gaussDecayFunction(
                EsBroadcast.TRANSMISSION_TIME_IN_MILLIS,
                nowMilis,
                minusThirtyDaysMillis
                ).setMultiValueMode(MultiValueMode.MIN)
        ).boost(timeBoost);
    }
}
