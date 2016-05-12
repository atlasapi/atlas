package org.atlasapi.remotesite.bbc.nitro;

import com.google.common.base.Throwables;
import com.metabroadcast.atlas.glycerin.Glycerin;
import com.metabroadcast.atlas.glycerin.GlycerinException;
import com.metabroadcast.atlas.glycerin.GlycerinResponse;
import com.metabroadcast.atlas.glycerin.model.Programme;
import com.metabroadcast.atlas.glycerin.queries.ProgrammesQuery;

import java.util.Iterator;

/**
 * Used to paginate over Nitro Programmes to reduce the heap overhead.
 *
 * Takes an iterable of {@link ProgrammesQuery} which is used to get
 * the individual Nitro Programmes.
 * Used as part of {@link OffScheduleContentIngestTask}
 */
public class PaginatedProgrammeRequest implements Iterable<Programme> {

    private final Iterable<ProgrammesQuery> programmeQueries;
    private final Glycerin client;

    public PaginatedProgrammeRequest(Glycerin client, Iterable<ProgrammesQuery> queries) throws GlycerinException {
        this.client = client;
        this.programmeQueries = queries;
    }

    @Override
    public Iterator<Programme> iterator() {
        return new ProgrammeIterator(client, programmeQueries);
    }

    private static class ProgrammeIterator implements Iterator<Programme> {

        private final Glycerin client;
        private final Iterator<ProgrammesQuery> programmeQueries;
        private GlycerinResponse<Programme> currentResponse;
        private Iterator<Programme> currentProgrammes;

        public ProgrammeIterator(Glycerin client, Iterable<ProgrammesQuery> programmeQueries) {
            this.client = client;
            this.programmeQueries = programmeQueries.iterator();
        }

        /**
         * Checks if current programme is present so that next method can be used to retrieve the Programme object.
         * At first will get the first page of the Program, then iterate over Programme pages, if there are any.
         * If not will get next Programme object and repeat previous steps.
         * This has been done to decrease the heap overhead when querying Nitro.
         * @return returns true if the current Programme is present, else returns false.
         */
        @Override
        public boolean hasNext() {
            if (currentProgrammes != null && currentProgrammes.hasNext()) {
                return true;
            }

            if (!programmeQueries.hasNext()) {
                return false;
            }

            try {
                if (currentProgrammes == null) { // Getting the first page.
                    currentResponse = client.execute(programmeQueries.next());
                    currentProgrammes = currentResponse.getResults().iterator();
                } else if (currentResponse.hasNext()) { // Getting the next page.
                    currentProgrammes = currentResponse.getNext().getResults().iterator();
                } else if (!currentResponse.hasNext()) { // Getting the next Programme response.
                    currentResponse = client.execute(programmeQueries.next());
                    currentProgrammes = currentResponse.getResults().iterator();
                }
                return currentProgrammes.hasNext();
            } catch (GlycerinException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public Programme next() {
            return currentProgrammes.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
