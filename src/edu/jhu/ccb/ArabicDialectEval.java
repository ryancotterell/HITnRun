package edu.jhu.ccb;
/**
 * An evaluator class for the Arabic Dialect HIT
 *
 * @author ryan cotterell
 */

public class ArabicDialectEval implements Evaluator {

    /**
     * The preprocessing allows for this to be a direct
     * string comparison
     */
    @Override
    public boolean evaluate(String userResponse,String control) {

        if (control == null) {
            control = "msa";
       
	    return userResponse.contains(control);
	} else {
	    
	    return !userResponse.contains("msa");
	}

    }


}
