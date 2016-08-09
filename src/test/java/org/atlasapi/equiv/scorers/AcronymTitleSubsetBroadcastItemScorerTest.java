package org.atlasapi.equiv.scorers;

import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.scorers.proposed.AcronymTitleSubsetBroadcastItemScorer;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.testing.StubContentResolver;

/**
 * Created by adam on 08/08/2016.
 */
public class AcronymTitleSubsetBroadcastItemScorerTest extends TitleSubsetBroadcastItemScorerTest {

    private static final ContentResolver resolver = new StubContentResolver();

    public AcronymTitleSubsetBroadcastItemScorerTest() {
        super(new AcronymTitleSubsetBroadcastItemScorer(resolver, Score.nullScore(), 80));
    }


}