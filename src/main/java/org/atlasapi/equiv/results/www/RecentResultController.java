package org.atlasapi.equiv.results.www;

import com.google.common.collect.Lists;
import com.metabroadcast.common.model.SimpleModelList;
import org.atlasapi.equiv.results.persistence.RecentEquivalenceResultStore;
import org.atlasapi.equiv.results.persistence.StoredEquivalenceResults;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.List;
import java.util.Map;

@Controller
public class RecentResultController {

    private final RecentEquivalenceResultStore resultStore;
    private final RestoredEquivalenceResultModelBuilder resultModelBuilder;

    public RecentResultController(RecentEquivalenceResultStore resultStore) {
        this.resultStore = resultStore;
        this.resultModelBuilder = new RestoredEquivalenceResultModelBuilder();
    }

    @RequestMapping("/system/equivalence/results/recent")
    public String showRecent(Map<String,Object> model) {
        model.put("itemResults", compileResults(resultStore.latestItemResults()));
        model.put("containerResults", compileResults(resultStore.latestContainerResults()));
        return "equivalence.recent";
    }

    @RequestMapping("/system/equivalence/results/recent/items")
    public String showRecentItems(Map<String,Object> model) {
        model.put("itemResults", compileResults(resultStore.latestItemResults()));
        return "equivalence.recentItems";
    }

    @RequestMapping("/system/equivalence/results/recent/containers")
    public String showRecentContainers(Map<String,Object> model) {
        model.put("containerResults", compileResults(resultStore.latestContainerResults()));
        return "equivalence.recentContainers";
    }
    
    private List<Map<String, ?>> compileResults(List<StoredEquivalenceResults> latestResults) {
        SimpleModelList resultsList = new SimpleModelList();
        
        for (StoredEquivalenceResults result : Lists.reverse(latestResults)) {
            resultsList.add(resultModelBuilder.build(result, null));
        }
        
        return resultsList.asListOfMaps();
    }
    
}
