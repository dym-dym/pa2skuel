package qengine.program;

import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.util.FileManager;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Engine {
    private static final MainRDFHandler rdfHandler = new MainRDFHandler();
    private String baseURI = null;
    /**
     * Votre répertoire de travail où vont se trouver les fichiers à lire
     */
    private String workingDir = "./data/";
    /**
     * Fichier contenant les requêtes sparql
     */
    private String queryFile = workingDir + "sample_query.queryset";
    /**
     * Fichier contenant des données rdf
     */
    private String dataFile = workingDir + "sample_data.nt";
    private boolean shuffleQueries = false;
    private boolean compareToJena = false;
    private int warmupPercentage;


    public Engine() {
    }

    public Engine(String baseURI, String workingDir, String queryFile, String dataFile, boolean shuffle, boolean compareToJena, Integer warmupPercentage) {
        this.baseURI = baseURI;
        this.workingDir = workingDir;
        this.queryFile = queryFile;
        this.dataFile = dataFile;
        shuffleQueries = shuffle;
        this.compareToJena = compareToJena;
        this.warmupPercentage = warmupPercentage;
    }

    // Renvoie le résultat d'une requête en étoile
    public static List<String> parseQuery(List<StatementPattern> patterns, MainRDFHandler rdfHandler) {
        Store posStore = rdfHandler.getPosStore();

        List<List<Integer>> filteredLists = patterns.stream()
                // Pour chacun des patterns de la requête
                .map(pattern -> {
                    // On récupère le prédicat et l'objet
                    Integer predicateKey = rdfHandler.getDictionary().getKeyIfExists(pattern.getPredicateVar().getValue().stringValue());
                    Integer objectKey = rdfHandler.getDictionary().getKeyIfExists(pattern.getObjectVar().getValue().stringValue());

                    // On filtre les éléments qui concernent ce prédicat et cet objet
                    return posStore.twoValuesFilter(predicateKey, objectKey);
                })
                // On en fait une liste de liste d'entiers
                .collect(Collectors.toList());

        // On retransforme cette liste en la liste de strings
        // en utilisant notre dictionnaire
        return findCommonElements(filteredLists)
                .stream()
                .map(element -> rdfHandler.getDictionary().getValue(element))
                .collect(Collectors.toList());
    }

    // Trouve les éléments communs entre deux listes d'entiers
    private static List<Integer> findCommonElements(List<List<Integer>> listOfLists) {
        if (listOfLists == null || listOfLists.isEmpty()) {
            return new ArrayList<>(); // Si une des listes est vide alors il n'y a aucun élément commun entre les listes
        }

        // On crée un ensemble pour s'assure d'ignorer les doublons
        Set<Integer> commonElementsSet = new HashSet<>(listOfLists.getFirst());

        // Pour chacune des liste on transforme cette liste en ensemble
        // puis on retiens les éléments communs aux deux ensembles
        for (int i = 1; i < listOfLists.size(); i++) {
            Set<Integer> currentListSet = new HashSet<>(listOfLists.get(i));
            commonElementsSet.retainAll(currentListSet);
        }

        // On convertit notre ensemble en liste
        return new ArrayList<>(commonElementsSet);
    }

    // ========================================================================

    /**
     * Méthode utilisée ici lors du parsing de requête sparql pour agir sur l'objet
     * obtenu.
     */
    public List<String> processAQuery(ParsedQuery query) {
        List<StatementPattern> patterns = StatementPatternCollector.process(query.getTupleExpr());
        return parseQuery(patterns, rdfHandler);
    }

    // ========================================================================

    /**
     * Traite chaque requête lue dans {@link #queryFile} avec
     * {@link #processAQuery(ParsedQuery)}.
     */
    public void parseQueries() throws IOException {
        SPARQLParser sparqlParser = new SPARQLParser();
        List<String> queryList  = Arrays.stream(Files.readString(Paths.get(queryFile))
                        .trim()
                        .split("(?<=})"))
                .map(String::trim)
                .collect(Collectors.toList());

        if (shuffleQueries) {
            Collections.shuffle(queryList);
        }
        if(warmupPercentage > 0) {
            int nbOfElements = warmupPercentage * (queryList.size() / 100);
            queryList.subList(0, nbOfElements + 1).forEach(element -> processAQuery(sparqlParser.parseQuery(element, baseURI)));
        }

        List<List<String>> jenaResults = null;

        if (compareToJena) {
            jenaResults = jenaResults(queryList, dataFile);
        }

        List<List<String>> engineResults = queryList.stream().map(element -> processAQuery(sparqlParser.parseQuery(element, baseURI))).toList();

        for (int i = 0; i < engineResults.size(); i++) {
            assert jenaResults != null;
            System.out.println("------------------");
            System.out.println("Jena : " + jenaResults.get(i));
            System.out.println("CustomEngine : " + engineResults.get(i));
            System.out.println("Equals ? : " + engineResults.get(i).stream().sorted().toList().equals(jenaResults.get(i).stream().sorted().toList()));
        }
    }

    public static <T> Stream<T> getSliceOfStream(Stream<T> stream, int startIndex, int endIndex)
    {
        return stream
                .toList()
                .subList(startIndex, endIndex + 1)
                .stream();
    }

    /**
     * Traite chaque triple lu dans {@link #dataFile} avec {@link MainRDFHandler}.
     */
    public void parseData() throws IOException {

        try (Reader dataReader = new FileReader(dataFile)) {
            // On va parser des données au format ntriples
            RDFParser rdfParser = Rio.createParser(RDFFormat.NTRIPLES);

            // On utilise notre implémentation de handler
            rdfParser.setRDFHandler(rdfHandler);

            // Parsing et traitement de chaque triple par le handler
            rdfParser.parse(dataReader, baseURI);
        }
    }

    private List<List<String>> jenaResults(List<String> queries, String data) {
        Model model = ModelFactory.createDefaultModel();
        FileManager.get().readModel(model, data);

        return queries.stream().map(query -> {
            QueryExecution queryExecutor = QueryExecutionFactory.create(QueryFactory.create(query), model);
            return processJenaResults(queryExecutor.execSelect());
        }).toList();
    }

    private static List<String> processJenaResults(ResultSet resultSet) {
        ArrayList<String> resultURLs = new ArrayList<>();

        while (resultSet.hasNext()) {
            QuerySolution solution = resultSet.nextSolution();
            RDFNode resultNode = solution.get("v0");

            if (resultNode.isResource()) {
                resultURLs.add(resultNode.asResource().getURI());
            } else {
                resultURLs.add(resultNode.toString());
            }
        }
        return resultURLs;
    }

}