package org.atlasapi.equiv.generators;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentResolver;

/** C4 PMLSD films are ingested as a brand with a single episode. This is because the C4 PMLSD feed
 * does not have a way for us to identify film content as films. We want these to actually equiv
 * to films from PA, so this should do the following:
 * - if you are sole child and duration is over 60 minutes, then you can equiv to top level items
 * - use title and year
 *
 */
public class C4FilmEquivalenceGenerator implements EquivalenceGenerator<Item> {

    private final ContentResolver resolver;

    public C4FilmEquivalenceGenerator(ContentResolver resolver) {
        this.resolver = resolver;
    }

    @Override
    public ScoredCandidates<Item> generate(Item subject, ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult) {



        return null;
    }
}
