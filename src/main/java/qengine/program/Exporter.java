package qengine.program;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class Exporter {
    private String path = "";
    private String dataFile = "";
    private String queryFile = "";
    private Integer numberOfTriplets = 0;
    private Integer numberOfQueries = 0;
    private long dataReadTime = 0;
    private long queryReadTime = 0;
    private long dictionaryCreationTime = 0;
    private Integer numberOfIndexes = 0;
    private long indexesCreationTime = 0;
    private long workloadEvaluationTime = 0;
    private long totalTimeElapsed = 0;
    private List<List<String>> results = null;
    private List<String> queries = null;

    public Exporter(String path,
                    String dataFile,
                    String queryFile,
                    Integer numberOfTriplets,
                    Integer numberOfQueries,
                    long dataReadTime,
                    long queryReadTime,
                    long dictionaryCreationTime,
                    Integer numberOfIndexes,
                    long indexesCreationTime,
                    long workloadEvaluationTime,
                    long totalTimeElapsed,
                    List<List<String>> results,
                    List<String> queries
    ) {
        setPath(path);
        setDataFile(dataFile);
        setQueryFile(queryFile);
        setNumberOfTriplets(numberOfTriplets);
        setNumberOfQueries(numberOfQueries);
        setDataReadTime(dataReadTime);
        setQueryReadTime(queryReadTime);
        setDictionaryCreationTime(dictionaryCreationTime);
        setNumberOfIndexes(numberOfIndexes);
        setIndexesCreationTime(indexesCreationTime);
        setWorkloadEvaluationTime(workloadEvaluationTime);
        setTotalTimeElapsed(totalTimeElapsed);
        setResults(results);
        setQueries(queries);
    }

    public List<List<String>> getResults() {
        return results;
    }

    public void setResults(List<List<String>> results) {
        this.results = results;
    }

    public void handleResults(boolean exportToCSV) {
        // Exportation to CSV file
        try {
            // Create CSV file content

            String csvContent = "Data File,Query File,Triple Count,Query Count,Data Read Time (ms),Query Read Time (ms)," +
                    "Dictionary Creation Time (ms),Index Count,Index Creation Time (ms)," +
                    "Total Workload Evaluation Time (ms),Total Time (ms)\n" +
                    getDataFile() + "," + getQueryFile() + "," + getNumberOfTriplets() +
                    "," + getNumberOfQueries() + "," +
                    Math.max(1, getDataReadTime()) +
                    "," + Math.max(1, getQueryReadTime()) + "," +
                    Math.max(1, getDictionaryCreationTime()) +
                    "," + getNumberOfIndexes() + "," +
                    Math.max(1, getIndexesCreationTime()) +
                    "," + getWorkloadEvaluationTime() + "," +
                    getTotalTimeElapsed() +
                    "\n";
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy_MM_dd_HH:mm:ss");
            LocalDateTime now = LocalDateTime.now();

            if (exportToCSV) {
                String outputResultsPath = getPath() + "/results" + dtf.format(now) + ".csv";
                StringBuilder resultContent = new StringBuilder();
                resultContent.append("query,results\n");
                for (int i = 0; i < results.size(); i += 1) {
                    resultContent.append('"').append(getQueries().get(i)).append('"').append(',');
                    resultContent.append('"');
                    resultContent.append(getResults().get(i));
                    resultContent.append('"');
                    resultContent.append('\n');
                }

                Files.write(Paths.get(outputResultsPath), resultContent.toString().getBytes());
            }

            Files.write(Paths.get(getPath() + "/output" + dtf.format(now) + ".csv"), csvContent.getBytes());
            System.out.println("Results exported to CSV: " + getPath() + "/output" + dtf.format(now) + ".csv");

            System.out.printf("Data Read Time (ms): %d\n", Math.max(1, getDataReadTime()));
            System.out.printf("Query Read Time (ms): %d\n", Math.max(1, getQueryReadTime()));
            System.out.printf("Dictionary Creation Time (ms): %d\n",
                    Math.max(1, getDictionaryCreationTime()));
            System.out.printf("Index Creation Time (ms): %d\n", Math.max(1, getIndexesCreationTime()));
            System.out.printf("Total Workload Evaluation Time (ms): %d\n",
                    getWorkloadEvaluationTime());
            System.out.printf("Total Time (ms): %d\n", Math.max(1, getTotalTimeElapsed()));

        } catch (IOException e) {
            System.err.println("Error exporting results to CSV: " + e.getMessage());
        }
    }

    public List<String> getQueries() {
        return queries;
    }

    public void setQueries(List<String> queries) {
        this.queries = queries;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getDataFile() {
        return dataFile;
    }

    public void setDataFile(String dataFile) {
        this.dataFile = dataFile;
    }

    public String getQueryFile() {
        return queryFile;
    }

    public void setQueryFile(String queryFile) {
        this.queryFile = queryFile;
    }

    public Integer getNumberOfTriplets() {
        return numberOfTriplets;
    }

    public void setNumberOfTriplets(Integer numberOfTriplets) {
        this.numberOfTriplets = numberOfTriplets;
    }

    public Integer getNumberOfQueries() {
        return numberOfQueries;
    }

    public void setNumberOfQueries(Integer numberOfQueries) {
        this.numberOfQueries = numberOfQueries;
    }

    public long getDataReadTime() {
        return dataReadTime;
    }

    public void setDataReadTime(long dataReadTime) {
        this.dataReadTime = dataReadTime;
    }

    public long getQueryReadTime() {
        return queryReadTime;
    }

    public void setQueryReadTime(long queryReadTime) {
        this.queryReadTime = queryReadTime;
    }

    public long getDictionaryCreationTime() {
        return dictionaryCreationTime;
    }

    public void setDictionaryCreationTime(long dictionaryCreationTime) {
        this.dictionaryCreationTime = dictionaryCreationTime;
    }

    public Integer getNumberOfIndexes() {
        return numberOfIndexes;
    }

    public void setNumberOfIndexes(Integer numberOfIndexes) {
        this.numberOfIndexes = numberOfIndexes;
    }

    public long getIndexesCreationTime() {
        return indexesCreationTime;
    }

    public void setIndexesCreationTime(long indexesCreationTime) {
        this.indexesCreationTime = indexesCreationTime;
    }

    public long getWorkloadEvaluationTime() {
        return workloadEvaluationTime;
    }

    public void setWorkloadEvaluationTime(long workloadEvaluationTime) {
        this.workloadEvaluationTime = workloadEvaluationTime;
    }

    public long getTotalTimeElapsed() {
        return totalTimeElapsed;
    }

    public void setTotalTimeElapsed(long totalTimeElapsed) {
        this.totalTimeElapsed = totalTimeElapsed;
    }

}
