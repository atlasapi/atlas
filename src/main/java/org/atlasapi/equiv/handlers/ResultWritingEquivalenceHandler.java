package org.atlasapi.equiv.handlers;

import java.io.IOException;

import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.results.persistence.EquivalenceResultStore;
import org.atlasapi.media.entity.Content;

import com.google.common.base.Throwables;
import org.codehaus.jackson.map.ObjectMapper;

public class ResultWritingEquivalenceHandler<T extends Content>
        implements EquivalenceResultHandler<T> {

    private EquivalenceResultStore store;

    public ResultWritingEquivalenceHandler(EquivalenceResultStore store) {
        this.store = store;
    }

    @Override
    public boolean handle(EquivalenceResult<T> result) {

        //CloseableHttpClient client = HttpClients.createDefault();
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            System.out.println(objectMapper.writeValueAsString(result));
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }

        store.store(result);
        return false;
    }
}
