package org.atlasapi.equiv.results.persistence;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

public class StoredEquivalenceResults implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String id;
    private final String aid;
    private final String title;
    private final List<StoredEquivalenceResultTable> resultTables;
    private final DateTime resultTime;
    private final List<Object> desc;

    public StoredEquivalenceResults(
            String targetId,
            String aid,
            String targetTitle,
            Collection<StoredEquivalenceResultTable> resultTables,
            DateTime resultTime,
            List<Object> desc
    ) {
        this.id = targetId;
        this.aid = aid;
        this.title = targetTitle;
        this.resultTables = ImmutableList.copyOf(resultTables);
        this.resultTime = resultTime;
        this.desc = desc;
    }

    /**
     * Convert the original StoredEquivalenceResult object to the new class
     * @param result
     */
    public StoredEquivalenceResults(StoredEquivalenceResult result) {
        this(
                result.id(),
                "",
                result.title(),
                ImmutableList.of(new StoredEquivalenceResultTable(result.sourceResults(), result.combinedResults(), result.description())),
                result.resultTime(),
                ImmutableList.of()
        );
    }

    public String id() {
        return id;
    }

    public String getAid() {
        return aid;
    }

    public String title() {
        return title;
    }

    public List<StoredEquivalenceResultTable> getResultTables() {
        return resultTables;
    }

    public DateTime resultTime() {
        return resultTime;
    }

    public List<Object> description() {
        return desc;
    }
    
    @Override
    public String toString() {
        return String.format("Result for %s %s", title, id);
    }
    
    @Override
    public boolean equals(Object that) {
        if(this == that) {
            return true;
        }
        if(that instanceof StoredEquivalenceResults) {
            StoredEquivalenceResults other = (StoredEquivalenceResults) that;
            return id.equals(other.id) && title.equals(other.title) && resultTables.equals(other.resultTables) && resultTime.equals(other.resultTime);
        }
        return super.equals(that);
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(id, title, resultTables, resultTime);
    }

}
