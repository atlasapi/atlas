package org.atlasapi.equiv.results.probe;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.model.SimpleModel;
import com.metabroadcast.common.model.SimpleModelList;
import org.atlasapi.equiv.results.persistence.EquivalenceResultStore;
import org.atlasapi.equiv.results.persistence.StoredEquivalenceResult;
import org.atlasapi.equiv.results.persistence.StoredEquivalenceResultTable;
import org.atlasapi.equiv.results.persistence.StoredEquivalenceResults;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.atlasapi.equiv.results.probe.EquivalenceResultProbe.equivalenceResultProbeFor;

@Controller
public class EquivalenceResultProbeController {

    private final EquivalenceResultStore resultStore;
    private final EquivalenceProbeStore probeStore;
    
    private final Splitter csv = Splitter.on(",").omitEmptyStrings().trimResults();
    private final EquivalenceProbeModelBuilder probeModelBuilder = new EquivalenceProbeModelBuilder();

    public EquivalenceResultProbeController(EquivalenceResultStore resultStore, EquivalenceProbeStore probeStore) {
        this.resultStore = resultStore;
        this.probeStore = probeStore;
    }
    
    @RequestMapping("system/equivalence/probes")
    public String showAllProbes(Map<String,Object> model, HttpServletResponse response, @RequestParam(value="uris",required=false,defaultValue="all") String uris) {
        
        Iterable<EquivalenceResultProbe> probes = "all".equals(uris) ? probeStore.probes() : probesIn(csv.split(uris));
        model.put("probes", modelsFor(probes).asListOfMaps());
        
        return "equivalence.probes";
    }

    private List<EquivalenceResultProbe> probesIn(Iterable<String> uris) {
        return ImmutableList.copyOf(Iterables.transform(uris, new Function<String, EquivalenceResultProbe>() {
            @Override
            public EquivalenceResultProbe apply(String input) {
                return probeStore.probeFor(input);
            }
        }));
    }

    private SimpleModelList modelsFor(Iterable<EquivalenceResultProbe> probes) {
        return new SimpleModelList(Iterables.transform(probes, new Function<EquivalenceResultProbe, SimpleModel>() {
            @Override
            public SimpleModel apply(EquivalenceResultProbe input) {
                StoredEquivalenceResults results = resultStore.forId(input.target());
                return probeModelBuilder.build(input, storedEquivalenceResultFromResults(results));
            }
        }));
    }

    @RequestMapping(value="system/equivalence/probes/update",method=RequestMethod.POST)
    public String updateProbe(
            Map<String,Object> model,
            HttpServletResponse response,
            @RequestParam("uri") String target,
            @RequestParam(value="expect",required=false,defaultValue="") String expect,
            @RequestParam(value="notExpect",required=false,defaultValue="") String notExpect) {
        
        EquivalenceResultProbe probe = equivalenceResultProbeFor(target).isEquivalentTo(csv.split(expect)).isNotEquivalentTo(csv.split(notExpect)).build();
        
        probeStore.store(probe);

        StoredEquivalenceResults results = resultStore.forId(probe.target());
        model.put("probe", probeModelBuilder.build(probe, storedEquivalenceResultFromResults(results)));
        
        return "equivalence.widgets.probe";
    }
    
    @RequestMapping(value = "system/equivalence/probes/update", method = RequestMethod.GET)
    public String configureProbe(Map<String, Object> model, HttpServletResponse response, @RequestParam(value = "uri", required = false) String target) throws IOException {

        target = Strings.nullToEmpty(target);
        
        SimpleModel probeModel = new SimpleModel().put("target", target);
        
        EquivalenceResultProbe probe = probeStore.probeFor(target);

        probeModel.put("expectedEquivalent", probe == null ? "" : Joiner.on(",\n").join(probe.expectedEquivalent()));
        probeModel.put("expectedNotEquivalent", probe == null ? "" : Joiner.on(",\n").join(probe.expectedNotEquivalent()));
        model.put("probe", probeModel);

        return "equivalence.probeUpdate";
    }

    //TODO (?): rework probes to work with the new StoredEquivalenceResults object which now contains multiple tables of results
    /**
     * This is a method that simply takes the first result table from the StoredEquivalenceResults object
     * to construct an old StoredEquivalenceResult object.
     * Since the endpoints in this class no longer appear to be used (and possibly are not configured correctly) this
     * is just to return something that should be compatible with existing code
     */
    private StoredEquivalenceResult storedEquivalenceResultFromResults(StoredEquivalenceResults results) {
        Optional<StoredEquivalenceResultTable> resultTable = results.getResultTables().isEmpty()
                ? Optional.empty()
                : Optional.of(results.getResultTables().get(0));
        return new StoredEquivalenceResult(
                results.id(),
                results.title(),
                resultTable.map(StoredEquivalenceResultTable::sourceResults).orElse(HashBasedTable.create()),
                resultTable.map(StoredEquivalenceResultTable::combinedResults).orElse(ImmutableList.of()),
                results.resultTime(),
                resultTable.map(StoredEquivalenceResultTable::description).orElse(results.description())
        );
    }
}
