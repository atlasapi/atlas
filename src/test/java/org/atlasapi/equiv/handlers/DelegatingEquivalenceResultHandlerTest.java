package org.atlasapi.equiv.handlers;

import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.media.entity.Item;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DelegatingEquivalenceResultHandlerTest {

    @Mock private EquivalenceResultHandler<Item> firstDelegate;
    @Mock private EquivalenceResultHandler<Item> secondDelegate;
    @Mock private EquivalenceResult<Item> result;

    private DelegatingEquivalenceResultHandler<Item> handler;

    @Before
    public void setUp() throws Exception {
        handler = new DelegatingEquivalenceResultHandler<>(ImmutableList.of(
                firstDelegate, secondDelegate
        ));
    }

    @Test
    public void returnsTrueIfBothDelegatesReturnTrue() throws Exception {
        when(firstDelegate.handle(result)).thenReturn(true);
        when(secondDelegate.handle(result)).thenReturn(true);

        assertThat(
                handler.handle(result),
                is(true)
        );
    }

    @Test
    public void returnsTrueIfOneDelegateReturnsFalse() throws Exception {
        when(firstDelegate.handle(result)).thenReturn(true);
        when(secondDelegate.handle(result)).thenReturn(false);

        assertThat(
                handler.handle(result),
                is(true)
        );
    }

    @Test
    public void returnsFalseIfBothDelegatesReturnFalse() throws Exception {
        when(firstDelegate.handle(result)).thenReturn(false);
        when(secondDelegate.handle(result)).thenReturn(false);

        assertThat(
                handler.handle(result),
                is(false)
        );
    }
}
