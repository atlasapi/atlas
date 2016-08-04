package org.atlasapi.equiv.analytics;

import java.io.FileWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.atlasapi.equiv.results.persistence.CombinedEquivalenceScore;
import org.atlasapi.equiv.results.persistence.StoredEquivalenceResult;

import com.google.common.collect.Table;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

public class EquivResultsReader {

    StoredEquivalenceResult result;
    String titleAddOn;

    public EquivResultsReader(StoredEquivalenceResult result, String titleAddOn) {
        this.result = result;
        this.titleAddOn = titleAddOn;
    }

    public void visualiseData() throws Exception {
        String filename = "output/" + titleAddOn + "_" +
                result.id().replaceAll("[^a-zA-Z0-9.-]", "") + ".txt";
        CSVFormat csvFileFormat = CSVFormat.DEFAULT.withRecordSeparator("\n");

        FileWriter writer = new FileWriter(filename);
        CSVPrinter printer = new CSVPrinter(writer, csvFileFormat);

        List<String> resultInfo = new LinkedList<>();
        resultInfo.add("ID: " + result.id());
        resultInfo.add("title" + result.title());
        printer.printRecord(resultInfo);



        Table<String, String, Double> table = result.sourceResults();
        printer.printRecord(table.toString());
        for (String tableKey: table.columnKeySet()) {
            Map<String, Double> row = table.row(tableKey);
            for (String rowKey: row.keySet()) {
                List<String> rowList = new LinkedList<>();
                rowList.add("row key: " + rowKey);
                rowList.add("row Contents: " + row.get(rowKey));
                rowList.add("table list: " + tableKey);
                printer.printRecord(rowList);
            }
        }

        for (CombinedEquivalenceScore combinedEquivalenceScore: result.combinedResults()) {
            List<String> record = new LinkedList<>();
            record.add("title: " + combinedEquivalenceScore.title());
            record.add("score: " + combinedEquivalenceScore.score());
            record.add("publisher: " + combinedEquivalenceScore.publisher());
            record.add("result title: " + result.title());
            record.add("result description" + result.description());

            printer.printRecord(record);
        }

        writer.flush();
        writer.close();
        printer.close();
    }
}
