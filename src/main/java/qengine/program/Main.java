package qengine.program;

import org.apache.commons.cli.*;
import org.jetbrains.annotations.NotNull;

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

    public static long startTime;
    public static long dataParsingTime;

    /**
     * Entrée du programme
     */
    public static void main(String[] args) throws Exception {


        Engine queryHandlerEngine = null;
        CommandLine commands;
        boolean compareToJena;
        boolean shuffle;
        int warmupDataAmount = 0;

        Options options = getOptions();

        CommandLineParser cliParser = new DefaultParser();

        try {
            commands = cliParser.parse(options, args);
            String queryFile = commands.getOptionValue("queries");
            String dataFile = commands.getOptionValue("data");
            compareToJena = commands.hasOption("Jena");
            shuffle = commands.hasOption("shuffle");

            if (commands.hasOption("warm")) {
                warmupDataAmount = Math.max(0, Math.min(100, Integer.parseInt(commands.getOptionValue("warm"))));

            }
            queryHandlerEngine = new Engine(null, "./data/", queryFile, dataFile, shuffle, compareToJena, warmupDataAmount);

        } catch (ParseException exception) {
            System.err.println("Error: Could not parse arguments properly: " + exception.getMessage());
            System.exit(1);
        }

        startTime = System.currentTimeMillis();

        long begin = System.currentTimeMillis();
        queryHandlerEngine.parseData();
        dataParsingTime = System.currentTimeMillis() - begin;

        queryHandlerEngine.parseQueries();
    }

    @NotNull
    private static Options getOptions() {
        Options options = new Options();

        options.addOption("queries", true, "Path to queries file");
        options.addOption("data", true, "Path to dataset file");
        options.addOption("output", true, "Path to output folder");
        options.addOption("Jena", false, "Sets Jena as an oracle for query testing purposes against qengine");
        options.addOption("warm", true, "Sets a warming up round for queries using an int as a percentage of queries to use");
        options.addOption("shuffle", false, "Should the entry data be shuffled");
        return options;
    }

}
