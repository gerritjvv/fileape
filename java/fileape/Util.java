package fileape;

import clojure.lang.Counted;
import clojure.lang.ISeq;

/**
 * Helper functions, especially where performance is required
 */
public class Util {

    /**
     * Check that an Object is not nil and if a collection is not empty<br/>
     * This function performs better than using clojure or and or seq to test.
     * @param v
     * @return
     */
    public static final boolean notNilOrEmpty(Object v){

        if(v == null)
            return false;
        else if(v instanceof Number)
            return true;
        else if(v instanceof String)
            return true;
        else if(v instanceof Counted)
            return ((Counted)v).count() > 0;
        else if(v instanceof ISeq)
            return ((ISeq)v).first() != null;

        return true;
    }
}
