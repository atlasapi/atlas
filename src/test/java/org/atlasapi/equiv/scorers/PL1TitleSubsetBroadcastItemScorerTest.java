package org.atlasapi.equiv.scorers;

import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.testing.StubContentResolver;

import static org.junit.Assert.*;

/**
 * Created by adam on 29/07/2016.
 */
public class PL1TitleSubsetBroadcastItemScorerTest extends TitleSubsetBroadcastItemScorerTest {

    private static final ContentResolver resolver = new StubContentResolver();

    public PL1TitleSubsetBroadcastItemScorerTest() {
        super(new PL1TitleSubsetBroadcastItemScorer(resolver, Score.nullScore(), 80));
    }



}