package org.atlasapi.equiv.handlers;

import org.atlasapi.media.entity.Item;

public class ContainerSummaryRequiredException extends RuntimeException {

    public ContainerSummaryRequiredException(String message) {
        super(message);
    }

    public ContainerSummaryRequiredException(Item item) {
        super("Container summary not found so equivalence cannot continue for item "
                + "with id: " + item.getId());
    }
}
