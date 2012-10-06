package edu.jhu.ccb;
/**
 * This is an interface for all HIT evalutator. It allows for the
 * encapsulation of the evaluation of the evaluation of the controls
 * 
 */

public interface Evaluator {

    /**
     * A predicate that returns true if the users answer corresponds
     * to the control
     */
    public boolean evaluate(String userResponse,String control);
}
