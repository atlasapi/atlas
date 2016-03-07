package org.atlasapi.remotesite.pa.deletes;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.remotesite.pa.deletes.PaContentDeactivationPredicate;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class PaContentDeactivationPredicateTest {

    private PaContentDeactivationPredicate predicate;

    @Before
    public void setUp() throws Exception {
        SetMultimap<String, String> namespaceToAlias = MultimapBuilder.hashKeys().hashSetValues().build();
        predicate = new PaContentDeactivationPredicate(DateTime.now(), namespaceToAlias);

        namespaceToAlias.put("pa:episodes", "activeItem1");
        namespaceToAlias.put("pa:episodes", "genericEpisode");
    }

    @Test
    public void testDoesNotDeactivateActiveContent() throws Exception {
        Item active = new Item("activeItem1", "", Publisher.PA);
        active.setId(10L);
        active.setLastUpdated(DateTime.now().minusDays(1));
        assertThat(predicate.apply(active), is(false));
    }

    @Test
    public void testDoesDeactivateInactiveContent() throws Exception {
        Item active = new Item("http://pressassociation.com/episodes/103123", "", Publisher.PA);
        active.setId(20L);
        active.setLastUpdated(DateTime.now().minusDays(1));
        assertThat(predicate.apply(active), is(true));
    }

    @Test
    public void testDoesNotDeactivatePaFilm() throws Exception {
        Film film = new Film("http://pressassociation.com/films/12312", "", Publisher.PA);
        film.setAliasUrls(ImmutableList.of("http://pressassociation.com/films/12312"));
        film.setId(30L);
        film.setLastUpdated(DateTime.now().minusDays(1));
        assertThat(predicate.apply(film), is(false));

        Film film2 = new Film("http://pressassociation.com/films/123123123123", "", Publisher.PA);
        film2.setId(31L);
        film2.setAliasUrls(ImmutableList.of("http://pressassociation.com/films/12312"));
        film2.setLastUpdated(DateTime.now().minusDays(1));
        assertThat(predicate.apply(film2), is(false));
    }

    @Test
    public void testDoesNotDeactivateGenericDescriptionContent() throws Exception {
        Episode genericEpisode1 = new Episode("genericEpisode", "", Publisher.PA);
        genericEpisode1.setGenericDescription(true);
        genericEpisode1.setLastUpdated(DateTime.now().minusDays(1));
        assertThat(predicate.apply(genericEpisode1), is(false));

        Episode genericEpisode2 = new Episode("http://pressassociation.com/episodes/100000001", "", Publisher.PA);
        genericEpisode2.setLastUpdated(DateTime.now().minusDays(1));
        assertThat(predicate.apply(genericEpisode2), is(false));

    }

    @Test
    public void testDoesDeactivateEmptyContainers() throws Exception {
        Brand emptyBrand = new Brand("emptyBrand1", "", Publisher.PA);
        emptyBrand.setId(40L);
        emptyBrand.setGenericDescription(false);
        emptyBrand.setLastUpdated(DateTime.now().minusDays(1));
        assertThat(predicate.apply(emptyBrand), is(true));

        Brand emptyBrand2 = new Brand("emptyBrand2", "", Publisher.PA);
        emptyBrand2.setId(41L);
        emptyBrand2.setGenericDescription(true);
        emptyBrand2.setLastUpdated(DateTime.now().minusDays(1));
        assertThat(predicate.apply(emptyBrand2), is(true));

        Series emptySeries1 = new Series("emptySeries1", "", Publisher.PA);
        emptySeries1.setId(42L);
        emptySeries1.setLastFetched(DateTime.now().minusDays(1));
        assertThat(predicate.apply(emptySeries1), is(true));

        Series nonEmptySeries = new Series("emptySeries1", "", Publisher.PA);
        nonEmptySeries.setId(42L);
        nonEmptySeries.setLastUpdated(DateTime.now().minusDays(1));
        nonEmptySeries.setChildRefs(ImmutableSet.of(new ChildRef(1l, "", "", DateTime.now(), EntityType.EPISODE)));
        assertThat(predicate.apply(nonEmptySeries), is(false));
    }

    @Test
    public void testDoesNotDeactivateContentIngestedAfterArchiveFileGenerated() {
        Episode newEpisode = new Episode("newEpisode", "", Publisher.PA);
        newEpisode.setLastUpdated(DateTime.now());

        assertThat(predicate.apply(newEpisode), is(false));
    }
}