package qengine.program;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.jetbrains.annotations.NotNull;

/**
 * Le RDFHandler intervient lors du parsing de données et permet d'appliquer un
 * traitement pour chaque élément lu par le parseur.
 *
 * <p>
 * Ce qui servira surtout dans le programme est la méthode
 * {@link #handleStatement(Statement)} qui va permettre de traiter chaque triple
 * lu.
 * </p>
 * <p>
 * À adapter/réécrire selon vos traitements.
 * </p>
 */
public final class MainRDFHandler extends AbstractRDFHandler {
    private final Dictionary<String> dictionary = new Dictionary<String>();
    private final Store spoStore = new Store();
    private final Store sopStore = new Store();
    private final Store opsStore = new Store();
    private final Store ospStore = new Store();
    private final Store psoStore = new Store();
    private final Store posStore = new Store();

    @Override
    public void handleStatement(@NotNull Statement st) {

        Integer subjectKey = dictionary.getKeyIfExists(st.getSubject().stringValue());
        Integer predicateKey = dictionary.getKeyIfExists(st.getPredicate().stringValue());
        Integer objectKey = dictionary.getKeyIfExists(st.getObject().stringValue());

        this.sopStore.update(subjectKey, objectKey, predicateKey);
        this.opsStore.update(objectKey, predicateKey, subjectKey);
        this.spoStore.update(subjectKey, predicateKey, objectKey);
        this.ospStore.update(objectKey, subjectKey, predicateKey);
        this.psoStore.update(predicateKey, subjectKey, objectKey);
        this.posStore.update(predicateKey, objectKey, subjectKey);
    }

    public Store getSopStore() {
        return sopStore;
    }

    public Store getOpsStore() {
        return opsStore;
    }

    public Store getSpoStore() {
        return spoStore;
    }

    public Store getOspStore() {
        return ospStore;
    }

    public Store getPsoStore() {
        return psoStore;
    }

    public Store getPosStore() {
        return posStore;
    }

    public Dictionary<String> getDictionary() {
        return dictionary;
    }
}