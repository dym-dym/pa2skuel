package qengine.program;

import org.eclipse.rdf4j.query.algebra.StatementPattern;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class Engine {

    // Renvoie le résultat d'une requête en étoile
    public List<String> parseQuery(List<StatementPattern> patterns, MainRDFHandler rdfHandler) {
        Store posStore = rdfHandler.getPosStore();

        List<List<Integer>> filteredLists = patterns.stream()
                // Pour chacun des patterns de la requête
                .map(pattern -> {
                    // On récupère le prédicat et l'objet
                    Integer predicateKey = rdfHandler.getDictionary().getKeyIfExists(pattern.getPredicateVar().getValue().stringValue());
                    Integer objectKey = rdfHandler.getDictionary().getKeyIfExists(pattern.getObjectVar().getValue().stringValue());

                    // On filtre les éléments qui concernent ce prédicat et cet objet
                    return posStore.entrySet().stream()
                            .filter(entry -> entry.getKey().equals(predicateKey))
                            .flatMap(entry -> entry.getValue().entrySet().stream())
                            .filter(innerEntry -> innerEntry.getKey().equals(objectKey))
                            .flatMap(innerEntry -> innerEntry.getValue().stream())
                            .collect(Collectors.toList());
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
}
