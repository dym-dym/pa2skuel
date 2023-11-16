package qengine.program;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import java.util.HashMap;

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

  private final HashMap<String, Integer> invertedDictionnary = new HashMap<String, Integer>();
  private final HashMap<Integer, String> dictionnary = new HashMap<Integer, String>();

  private Integer currentKey = 0;

  @Override
  public void handleStatement(Statement st) {

    Integer subjectKey = getKeyIfExists(st.getSubject().stringValue());
    Integer predicateKey = getKeyIfExists(st.getPredicate().stringValue());
    Integer objectKey = getKeyIfExists(st.getObject().stringValue());
  }

  public int getKeyIfExists(String value) {
    if (invertedDictionnary.containsKey(value)) {
      return invertedDictionnary.get(value);
    }

    currentKey++;

    dictionnary.put(currentKey, value);
    invertedDictionnary.put(value, currentKey);

    return currentKey;
  }

}
