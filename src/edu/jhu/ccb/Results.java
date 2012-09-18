package edu.jhu.ccb;

import java.io.StringReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.io.IOException;

import com.amazonaws.mturk.addon.BatchItemCallback;
import com.amazonaws.mturk.addon.HITDataCSVReader;
import com.amazonaws.mturk.addon.HITDataInput;
import com.amazonaws.mturk.addon.HITProperties;
import com.amazonaws.mturk.addon.HITResults;
import com.amazonaws.mturk.requester.Assignment;
import com.amazonaws.mturk.requester.AssignmentStatus;
import com.amazonaws.mturk.util.HITResultProcessor;
import com.amazonaws.mturk.util.ClientConfig;
import com.amazonaws.mturk.util.PropertiesClientConfig;

import com.amazonaws.mturk.service.axis.RequesterService;
public class Results  implements BatchItemCallback {

    private HITResultProcessor resultProcessor;
    
    public Results() throws IOException {
	String outputFile = "test";
       
	resultProcessor = new HITResultProcessor(outputFile, false);

	String successFile = "exp.results";
	ClientConfig config = new PropertiesClientConfig();
	HITDataInput success = new HITDataCSVReader(successFile); 
	RequesterService service = new RequesterService(config);

	service.getResults(success,this);
    }
    
    public static void main(String[] args) throws IOException {
	new Results();
    }


    /**
     * Callback passed to the SDK which is invoked after a HIT result has been retrieved
     * or an error occurred
     */
    public synchronized void processItemResult(Object itemId, boolean succeeded, Object result, Exception itemException) {
	try {
	    // resultsCount++;
	    if (succeeded) {
		HITResults r = (HITResults)result;
        
                
		r.writeResults(resultProcessor);
		//log.info(String.format("Retrieved HIT %d/%d, %s", resultsCount, rowCount, itemId));
		//updateStatistics(r);
	    }
	    else {
		//log.error(String.format("Error retrieving HIT results for HIT %d/%d (%s): %s", resultsCount, rowCount,itemId, itemException.getMessage()));
	    }
	}
	catch (Exception ex) {
	    //log.error(String.format("Error processing HIT results for HIT %s: %s", itemId, ex.getMessage()));
	}   
    }
    
}