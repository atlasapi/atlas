package org.atlasapi.input;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.simple.Alias;
import org.atlasapi.media.entity.simple.Description;
import org.atlasapi.media.entity.simple.SameAs;
import org.joda.time.DateTime;

import com.metabroadcast.common.time.Clock;

public abstract class IdentifiedModelTransformer<F extends Description, T extends Identified>
        implements ModelTransformer<F, T> {

    private final Clock clock;

    public IdentifiedModelTransformer(Clock clock) {
        this.clock = checkNotNull(clock);
    }

    @Override
    public final T transform(F simple) {
        DateTime now = clock.now();
        T output = createIdentifiedOutput(simple, now);
        output.setLastUpdated(now);
        return setIdentifiedFields(output, simple);
    }

    private T setIdentifiedFields(T output, F simple) {
        output.setCanonicalUri(simple.getUri());
        output.setCurie(simple.getCurie());
        setEquivalents(output, simple);
        output.setAliases(
                transformV4Aliases(simple.getV4Aliases())
        );
        return output;
    }

    private void setEquivalents(T output, F simple) {
        if (simple.getEquivalents().isEmpty() && !simple.getSameAs().isEmpty()) {
            output.setEquivalentTo(resolveEquivalents(simple.getSameAs()));
        } else if (!simple.getEquivalents().isEmpty()){
            output.setEquivalentTo(resolveSameAs(simple.getEquivalents()));
        }
    }

    private Iterable<org.atlasapi.media.entity.Alias> transformV4Aliases(Collection<Alias> v4Aliases) {
        if (v4Aliases == null) {
            return null;
        }
        return Collections2.transform(v4Aliases, new Function<Alias, org.atlasapi.media.entity.Alias>() {
            @Override
            public org.atlasapi.media.entity.Alias apply(Alias input) {
                return new org.atlasapi.media.entity.Alias(
                        input.getNamespace(),
                        input.getValue()
                );
            }
        });
    }

    protected abstract Set<LookupRef> resolveSameAs(Set<SameAs> equivalents);

    protected abstract Set<LookupRef> resolveEquivalents(Set<String> sameAs);

    protected abstract T createIdentifiedOutput(F simple, DateTime now);

}
