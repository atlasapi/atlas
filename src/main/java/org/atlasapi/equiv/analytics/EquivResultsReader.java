package org.atlasapi.equiv.analytics;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.persistence.CombinedEquivalenceScore;
import org.atlasapi.equiv.results.persistence.StoredEquivalenceResult;

import com.google.common.collect.ImmutableSet;
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
        String filename = "output/" +
                result.id().replaceAll("[^a-zA-Z0-9]", "") + "_" + titleAddOn + ".csv";
        CSVFormat csvFileFormat = CSVFormat.DEFAULT.withRecordSeparator("\n");

        FileWriter writer = new FileWriter(filename);
        CSVPrinter printer = new CSVPrinter(writer, csvFileFormat);

        List<CombinedEquivalenceScore> candidateList = new LinkedList<>();
        int strong = 0;
        for (CombinedEquivalenceScore combinedEquivalenceScore: result.combinedResults()) {
            candidateList.add(combinedEquivalenceScore);
            if (combinedEquivalenceScore.strong()) {
                strong++;
            }
        }
        printer.printRecord("candidates:" + candidateList.size());
        printer.printRecord("strong:" + strong);



        List<String> resultInfo = new LinkedList<>();
        resultInfo.add("ID: " + result.id());
        resultInfo.add("Title: " + result.title());
        printer.printRecord(resultInfo);



        Table<String, String, Double> table = result.sourceResults();
//        printer.printRecord(table.toString());

        printer.printRecord("---------------");
        printer.printRecord("result.sourceResults() table: ");
        printer.printRecord("---------------");
        List<String> columnIndex = new LinkedList<>();

        for (String rowKey: table.rowKeySet()) {
            columnIndex.add("rowkey: " + rowKey);
        }
        printer.printRecord(columnIndex);
        for (String columnKey: table.columnKeySet()) {
            List<String> cellList = new LinkedList<>();
            for (String rowKey: table.rowKeySet()) {
                if (table.get(rowKey, columnKey) != null) {
                    cellList.add(table.get(rowKey, columnKey).toString());
                }


            }
            printer.printRecord(cellList);
        }
        printer.printRecord("---------------");
        for (String columnKey: table.columnKeySet()) {
            printer.printRecord(columnKey);
        }

//        for (String tableKey: table.columnKeySet()) {
//            Map<String, Double> row = table.row(tableKey);
//            for (String rowKey: row.keySet()) {
//                List<String> rowList = new LinkedList<>();
//                rowList.add("row key: " + rowKey);
//                rowList.add("row Contents: " + row.get(rowKey));
//                rowList.add("table list: " + tableKey);
//                printer.printRecord(rowList);
//            }
//        }

        printer.printRecord("---------------");
        printer.printRecord("result.combinedResults() :");
        printer.printRecord("---------------");
        printer.printRecord(Arrays.asList("title", "combined score", "publisher", "strong", "id"));

        for (CombinedEquivalenceScore combinedEquivalenceScore: result.combinedResults()) {
            List<String> record = new LinkedList<>();
            record.add(combinedEquivalenceScore.title());
            record.add(combinedEquivalenceScore.score().toString());
            record.add(combinedEquivalenceScore.publisher());
            record.add(combinedEquivalenceScore.strong()?"true":"false");
            record.add(combinedEquivalenceScore.id());


            printer.printRecord(record);
        }
        printer.printRecord("---------------");
        printer.printRecord("Description matching results");
        printer.printRecord("---------------");

        // I had to use object here because I'm dealing with messy code
        Object o = (result.description().get(3));

        for (Object object: (List)o) {
            if (object instanceof String) {
                printer.printRecord(object);
            } else if (object instanceof LinkedList) {
                for (String string: (LinkedList<String>)object) {
                    printer.printRecord(string);
                }
            }
        }


        writer.flush();
        writer.close();
        printer.close();
    }
}
