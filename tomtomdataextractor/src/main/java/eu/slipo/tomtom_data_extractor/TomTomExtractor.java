package eu.slipo.tomtom_data_extractor;

import com.google.common.base.Charsets;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TomTomExtractor {

    public void processTomTomData(String inputFile, String outputFile, String categoriesFile) {
        Model model = ModelFactory.createDefaultModel();
        model.read(inputFile, "NT");

        Iterable<String> categories = readNonEmptyLines(categoriesFile);
        //queryTomTomPOICategories(model, categories);
        queryNoOfTomTomPOIs(model, categories);

       // List<List<String>> poiRecords = queryTomTomPOIs(model, categories);
       // writeToCSV(outputFile, poiRecords);
    }

    private Iterable<String> readNonEmptyLines(String filename) {
        try {
            List<String> lines = Files.readLines(new File(filename), Charsets.UTF_8);
            Iterable<String> nonEmptyLines = Iterables.filter(lines, line -> line != null && !line.isEmpty());
            return nonEmptyLines;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Iterable<String[]> readCSVFile(String file, String separator) {
        Iterable<String> nonEmptyLines = readNonEmptyLines(file);
        return Iterables.transform(nonEmptyLines, line -> line.split(separator));
    }

    private List<List<String>> queryTomTomPOIs(Model model, Iterable<String> categories) {
        List<List<String>> poiRecords = new ArrayList<>();

        String queryString = "" + //
                " prefix ogc: <http://www.opengis.net/ont/geosparql#> " + //
                " prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " + //
                " prefix sld: <http://slipo.eu/def#> " + //
                " prefix slid: <http://slipo.eu/id/> " + //

                " SELECT DISTINCT ?poi ?nameValue ?termValue ?coordinates WHERE {  " + //
                "  ?poi rdf:type sld:POI  . " + //

                "  ?poi sld:name ?name . " + //
                "  ?name sld:nameValue ?nameValue . " + //

                "  ?poi ogc:hasGeometry ?geometry . " + //
                "  ?geometry ogc:asWKT ?coordinates . " + //

                "  ?poi sld:category ?category . " + //
                "  ?category sld:termValue ?termValue . " + //

                "  FILTER ( ?category IN (" + String.join(", ", categories) + ") )" +
                "}";

        Query query = QueryFactory.create(queryString);

        try (QueryExecution qexec = QueryExecutionFactory.create(query, model)) {
            ResultSet results = qexec.execSelect();

            for (; results.hasNext(); ) {
                QuerySolution soln = results.nextSolution();
                List<String> poiRecord = new ArrayList<>();

                String poiID = soln.get("poi").toString();
                String name = StringUtils.substringBefore(soln.getLiteral("nameValue").getString(), "@");
                String category = StringUtils.substringBefore(soln.getLiteral("termValue").getString(), "@");
                String[] coordinates = parseCoordinates(soln.getLiteral("coordinates").getString());

                System.out.println(poiID + "  " + name + "  " + category + "  " + coordinates[1] + " " + coordinates[0]);

                poiRecord.add(poiID);
                poiRecord.add(name);
                poiRecord.add(category);
                poiRecord.add(coordinates[1]);
                poiRecord.add(coordinates[0]);

                poiRecords.add(poiRecord);
            }

            return poiRecords;
        }
    }


    private String[] parseCoordinates(String coordinates) {
        Pattern pattern = Pattern.compile("\\((.*?)\\)");
        Matcher matcher = pattern.matcher(coordinates);

        return matcher.find() ? matcher.group(1).split(" ") : null;
    }

    private void writeToCSV(String outputFile, List<List<String>> poiRecords) {
        File file = new File(outputFile);

        // if file doesnt exists, then create it
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        try (FileWriter fw = new FileWriter(file.getAbsoluteFile());
             BufferedWriter bw = new BufferedWriter(fw)) {

            for (List<String> poiRecord : poiRecords) {
                bw.write(String.join(";", poiRecord));
                bw.newLine();
            }

            System.out.println("Done");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void queryTomTomPOICategories(Model model, Iterable<String> categories) {

        String queryString = "" + //
                " prefix ogc: <http://www.opengis.net/ont/geosparql#> " + //
                " prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " + //
                " prefix sld: <http://slipo.eu/def#> " + //
                " prefix slid: <http://slipo.eu/id/> " + //

                " SELECT DISTINCT ?category ?termValue WHERE {  " + //
                "  ?poi rdf:type sld:POI  . " + //

                "  ?poi sld:category ?category . " + //
                "  ?category sld:termValue ?termValue . " + //

                "  FILTER ( ?category IN (" + String.join(", ", categories) + ") )" +
                "}";

        Query query = QueryFactory.create(queryString);

        try (QueryExecution qexec = QueryExecutionFactory.create(query, model)) {
            ResultSet results = qexec.execSelect();

            for (; results.hasNext(); ) {
                QuerySolution soln = results.nextSolution();

                String categoryID = soln.get("category").toString();
                String categoryName = StringUtils.substringBefore(soln.getLiteral("termValue").getString(), "@");

                System.out.println(categoryID + "  " + categoryName );
            }

        }
    }

    private void queryNoOfTomTomPOIs(Model model, Iterable<String> categories) {

        String queryString = "" + //
                " prefix ogc: <http://www.opengis.net/ont/geosparql#> " + //
                " prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " + //
                " prefix sld: <http://slipo.eu/def#> " + //
                " prefix slid: <http://slipo.eu/id/> " + //

                " SELECT (COUNT(DISTINCT ?poi) as ?pcount) WHERE {  " + //
                "  ?poi rdf:type sld:POI  . " + //
                "}";

        Query query = QueryFactory.create(queryString);

        try (QueryExecution qexec = QueryExecutionFactory.create(query, model)) {
            ResultSet results = qexec.execSelect();

            for (; results.hasNext(); ) {
                QuerySolution soln = results.nextSolution();

                String count = soln.get("pcount").toString();

                System.out.println("POI count  " + count );
            }

        }
    }
}
