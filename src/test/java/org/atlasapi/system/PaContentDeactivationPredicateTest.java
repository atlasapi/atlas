package org.atlasapi.system;

import com.google.common.collect.ImmutableSet;
import org.atlasapi.media.entity.*;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class PaContentDeactivationPredicateTest {

    private PaContentDeactivationPredicate predicate;

    @Before
    public void setUp() throws Exception {
        predicate = new PaContentDeactivationPredicate(
                ImmutableSet.of(10L)
        );
    }

    @Test
    public void testDoesNotDeactivateActiveContent() throws Exception {
        Item active = new Item("activeItem1", "", Publisher.PA);
        active.setId(10L);
        assertThat(predicate.apply(active), is(false));
    }

    @Test
    public void testDoesDeactivateInactiveContent() throws Exception {
        Item active = new Item("inactiveItem1", "", Publisher.PA);
        active.setId(20L);
        assertThat(predicate.apply(active), is(true));
    }

    @Test
    public void testDoesNotDeactivatePaFilm() throws Exception {
        Film film = new Film("paFilm1", "", Publisher.PA);
        film.setId(30L);
        assertThat(predicate.apply(film), is(true));

        Film film2 = new Film("paFilm2", "", Publisher.PA);
        film2.setId(31L);
        assertThat(predicate.apply(film2), is(true));
    }

    @Test
    public void testDoesNotDeactivateGenericDescriptionContent() throws Exception {
        Episode episode = new Episode("genericEpisode", "", Publisher.PA);
        episode.setGenericDescription(true);

        assertThat(predicate.apply(episode), is(false));
    }

    @Test
    public void testDoesDeactivateEmptyContainers() throws Exception {
        Brand emptyBrand = new Brand("emptyBrand1", "", Publisher.PA);
        emptyBrand.setId(40L);
        emptyBrand.setGenericDescription(false);
        assertThat(predicate.apply(emptyBrand), is(true));

        Brand emptyBrand2 = new Brand("emptyBrand2", "", Publisher.PA);
        emptyBrand2.setId(41L);
        emptyBrand2.setGenericDescription(true);
        assertThat(predicate.apply(emptyBrand2), is(true));

        Series emptySeries1 = new Series("emptySeries1", "", Publisher.PA);
        emptySeries1.setId(42L);
        assertThat(predicate.apply(emptySeries1), is(true));

        Series nonEmptySeries = new Series("emptySeries1", "", Publisher.PA);
        nonEmptySeries.setId(42L);
        nonEmptySeries.setChildRefs(ImmutableSet.of(new ChildRef(1l, "", "", DateTime.now(), EntityType.EPISODE)));
        assertThat(predicate.apply(nonEmptySeries), is(false));
    }
}