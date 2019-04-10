package org.atlasapi.equiv.results.persistence;

import com.google.common.base.Objects;
import com.google.common.collect.Table;

import java.io.Serializable;
import java.util.List;

public class StoredEquivalenceResultTable implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Table<String, String, Double> results;
    private final List<CombinedEquivalenceScore> totals;
    private final List<Object> desc;

    public StoredEquivalenceResultTable(
            Table<String, String, Double> results,
            List<CombinedEquivalenceScore> totals,
            List<Object> desc
    ) {
        this.results = results;
        this.totals = totals;
        this.desc = desc;
    }

    public Table<String, String, Double> sourceResults() {
        return results;
    }

    public List<CombinedEquivalenceScore> combinedResults() {
        return totals;
    }

    public List<Object> description() {
        return desc;
    }

    @Override
    public boolean equals(Object that) {
        if(this == that) {
            return true;
        }
        if(that instanceof StoredEquivalenceResultTable) {
            StoredEquivalenceResultTable other = (StoredEquivalenceResultTable) that;
            return results.equals(other.results) && totals.equals(other.totals);
        }
        return super.equals(that);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(results, totals);
    }

}
