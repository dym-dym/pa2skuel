package qengine.program;

import org.apache.commons.cli.*;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.jetbrains.annotations.NotNull;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Programme simple lisant un fichier de requête et un fichier de données.
 *
 * <p>
 * Les entrées sont données ici de manière statique,
 * à vous de programmer les entrées par passage d'arguments en ligne de commande
 * comme demandé dans l'énoncé.
 * </p>
 *
 * <p>
 * Le présent programme se contente de vous montrer la voie pour lire les
 * triples et requêtes
 * depuis les fichiers ; ce sera à vous d'adapter/réécrire le code pour
 * finalement utiliser les requêtes et interroger les données.
 * On ne s'attend pas forcémment à ce que vous gardiez la même structure de
 * code, vous pouvez tout réécrire.
 * </p>
 *
 * @author Olivier Rodriguez <olivier.rodriguez1@umontpellier.fr>
 */
final class Main {
    static final String baseURI = null;
    private static final MainRDFHandler rdfHandler = new MainRDFHandler();
    private static final Engine queryHandlerEngine = new Engine();
    /**
     * Votre répertoire de travail où vont se trouver les fichiers à lire
     */
    static String workingDir = "./data/";
    /**
     * Fichier contenant les requêtes sparql
     */
    static String queryFile = workingDir + "sample_query.queryset";
    /**
     * Fichier contenant des données rdf
     */
    static String dataFile = workingDir + "sample_data.nt";
    private static CommandLine commands;
    private static boolean hasJena;
    private static boolean debug;
    private static Integer warmupDataAmount;

    /**
     * Entrée du programme
     */
    public static void main(String[] args) throws Exception {

        Options options = getOptions();

        CommandLineParser cliParser = new DefaultParser();

        // TODO: Handle missing arguments if not passed to the program

        try {
            commands = cliParser.parse(options, args);
            queryFile = commands.getOptionValue("queries");
            dataFile = commands.getOptionValue("data");
            hasJena = commands.hasOption("Jena");
            debug = commands.hasOption("debug");

            if (commands.hasOption("warm")) {
                warmupDataAmount = Math.max(0, Math.min(100, Integer.parseInt(commands.getOptionValue("warm"))));

            }

        } catch (ParseException exception) {
            System.err.println("Error: Could not parse arguments properly: " + exception.getMessage());
            System.exit(1);
        }

        parseData();
        parseQueries();
    }

    @NotNull
    private static Options getOptions() {
        Options options = new Options();

        options.addOption("queries", true, "Path to queries file");
        options.addOption("data", true, "Path to dataset file");
        options.addOption("output", true, "Path to output folder");
        options.addOption("Jena", false, "Sets Jena as an oracle for query testing purposes against qengine");
        options.addOption("warm", true, "Sets a warming up round for queries using an int as a percentage of queries to use");
        options.addOption("shuffle", true, "Should the entry data be shuffled");
        options.addOption("debug", false, "Enable debug output");
        return options;
    }

    // ========================================================================

    /**
     * Méthode utilisée ici lors du parsing de requête sparql pour agir sur l'objet
     * obtenu.
     */
    public static void processAQuery(ParsedQuery query) {
        List<StatementPattern> patterns = StatementPatternCollector.process(query.getTupleExpr());
        queryHandlerEngine.parseQuery(patterns, rdfHandler).forEach(System.out::println);
    }

    // ========================================================================

    /**
     * Traite chaque requête lue dans {@link #queryFile} avec
     * {@link #processAQuery(ParsedQuery)}.
     */
    private static void parseQueries() throws IOException {
        /*
         * Try-with-resources
         *
         * @see <a href=
         *      "https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html">Try-with-resources</a>
         */
        /*
         * On utilise un stream pour lire les lignes une par une, sans avoir à toutes
         * les stocker
         * entièrement dans une collection.
         */
        try (Stream<String> lineStream = Files.lines(Paths.get(queryFile))) {
            SPARQLParser sparqlParser = new SPARQLParser();
            Iterator<String> lineIterator = lineStream.iterator();
            StringBuilder queryString = new StringBuilder();

            while (lineIterator.hasNext())
                /*
                 * On stocke plusieurs lignes jusqu'à ce que l'une d'entre elles se termine par
                 * un '}'
                 * On considère alors que c'est la fin d'une requête
                 */ {
                String line = lineIterator.next();
                queryString.append(line);

                if (line.trim().endsWith("}")) {

                    //System.out.println(queryString);

                    ParsedQuery query = sparqlParser.parseQuery(queryString.toString(), baseURI);

                    processAQuery(query); // Traitement de la requête, à adapter/réécrire pour votre programme

                    queryString.setLength(0); // Reset le buffer de la requête en chaine vide
                }
            }
        }
    }

    /**
     * Traite chaque triple lu dans {@link #dataFile} avec {@link MainRDFHandler}.
     */
    private static void parseData() throws IOException {


        try (Reader dataReader = new FileReader(dataFile)) {
            // On va parser des données au format ntriples
            RDFParser rdfParser = Rio.createParser(RDFFormat.NTRIPLES);

            // On utilise notre implémentation de handler
            rdfParser.setRDFHandler(rdfHandler);

            // Parsing et traitement de chaque triple par le handler
            rdfParser.parse(dataReader, baseURI);
        }
    }
}
