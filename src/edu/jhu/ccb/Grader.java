package edu.jhu.ccb;

import java.util.Map;
import java.util.HashMap;

import java.util.Calendar;
import java.util.GregorianCalendar;

import java.net.UnknownHostException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.mongodb.Mongo;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.ServerAddress;

import com.amazonaws.mturk.service.axis.RequesterService;
import com.amazonaws.mturk.util.PropertiesClientConfig;
import com.amazonaws.mturk.service.exception.ServiceException;
import com.amazonaws.mturk.requester.GetAccountBalance;

/**
 * This is a grader. There will eventually be an interface the grader
 * must be HIT dependent. It pushs the controls into the control collection
 * and chooses to accept or reject each hit based on the worker's aggregate performance.
 * The grader then pushes the rejected HITs back up to MTurk.
 *
 */
public class Grader {

    private static final String db_name = "arabic_dialect";
    private static final String submitted = "submitted";
    private static final String approved = "approved";
    private static final String upload = "upload";
    private static final String rejected = "rejected";
    private static final String error = "error";


    private static final String approvedPending = "approved_pending";
    private static final String rejectedPending = "rejected_pending";
    
    private static final String controls = "controls";

    private static final String configPath = "mturk.properties";

    private static final String approvalMessage = "Your HIT has been approved. Thank you for your time and effort.";
    private static final String rejectionMessage = "Your HIT has been rejected because you did not meet our gold standard. If you feel you were rejected without just cause please send us an email";

    
    private static final int controlMinThreshold = 20; // this is double the number of HITs    
    private static final double percentageCorrectThreshold = .5;

    //when the HIT is this close to being autoapproved (in miliseconds) the 
    //a decision will be made based on performance regardless of the number of
    //HITs the turker performed
    private static final int autoApproveThreshold = 60 * 60 * 60 * 60 * 24 * 2; //two days (in milliseconds) 

    private static final long expirationIncrement = 60 * 60 * 24 * 3; //three days

    private Map<String,Double> percentCorrectStorage;

    private Mongo m;
    private DB db;
    private DBCollection submittedColl;
    private DBCollection controlColl;
    private DBCollection approvedColl;
    private DBCollection rejectedColl;
    //these are for the simulation of approval and rejection
    private DBCollection approvedPendingColl;
    private DBCollection rejectedPendingColl;

    private DBCollection uploadColl;
    private DBCollection errorColl;

    private RequesterService service;

    private Evaluator eval;

    public Grader(Evaluator eval) throws UnknownHostException {
	this.eval = eval;
	percentCorrectStorage = new HashMap<String,Double>();

	m = new Mongo();
	db = m.getDB(db_name);
	submittedColl = db.getCollection(submitted);
	controlColl = db.getCollection(controls);
	rejectedColl = db.getCollection(rejected);
	approvedColl = db.getCollection(approved);
	uploadColl = db.getCollection(upload);


	approvedPendingColl = db.getCollection(approvedPending);
	rejectedPendingColl = db.getCollection(rejectedPending);
	//this collection is for hits which caued an exception during processing
	errorColl = db.getCollection(error);

	service = new RequesterService(new PropertiesClientConfig(configPath));

	//extract controls for grading
	extractControls();

	grade();
       
    }

    /**
     * Repost a HIT that failed the grading criteria
     */
    private void repost() {
	DBCursor cursor = uploadColl.find();

	for (DBObject doc : cursor) {
	    String hitid = (String)doc.get("hitid");
	    
	    try {
		service.extendHIT(hitid,1,expirationIncrement);
		uploadColl.remove(doc);
	    } catch (ServiceException ex) {
		System.out.println("Could not repost the hit with HITId " + hitid + ".");
	    }
	}

    }

    /**
     * Grades the workers based on extracted controls
     *
     */
    private void grade() {
	DBCursor cursor = submittedColl.find();
	

	for (DBObject doc : cursor) {
	    String workerid = (String)doc.get("workerid");
	    String cal = (String)doc.get("auto_approval");
	    //can be graded - there should also be a time condition 
	    
	    if (controlCount(workerid) >= controlMinThreshold || forceJudgement(cal) ) {

		String assignmentId = (String)doc.get("assignmentid");
		

		if (percentageCorrect(workerid) >= percentageCorrectThreshold) {
		    //approved
		    try {
			//service.approveAssignment(assignmentId,approvalMessage);
			submittedColl.remove(doc);
			approvedPendingColl.insert(doc);

		    } catch (ServiceException ex) {
			System.err.println("Could not approve assignment " + assignmentId + ".");
		    }
		  
		} else {
		    //rejected
		    
		    try {
			//service.rejectAssignment(assignmentId,rejectionMessage);
			submittedColl.remove(doc);

			rejectedPendingColl.insert(doc);

			//uploadColl.insert(doc);
		    } catch (ServiceException ex) {
			System.err.println("Could not reject assignment  " + assignmentId + ".");
		    }
		}				
	    } 
	}
    }

    /**
     * Returns the true is the HIT will be
     * autoapproved imminently and it a judgement
     * should be made about whether to autoapprove
     * immediately
     * @param cal a calendar toString
     */
    private boolean forceJudgement(String cal) {
	
	String yearRegex = ",YEAR=(\\d\\d\\d\\d),";
	String monthRegex = ",MONTH=(\\d*?),";
	String dayRegex = ",DAY_OF_MONTH=(\\d*?),";
	String unixRegex = "time=(\\d*?),";

	Pattern yearPattern = Pattern.compile(yearRegex);
	Pattern monthPattern = Pattern.compile(monthRegex);
	Pattern dayPattern = Pattern.compile(dayRegex);
	Pattern unixPattern = Pattern.compile(unixRegex);

	Matcher mYear = yearPattern.matcher(cal);
	Matcher mMonth = monthPattern.matcher(cal);
	Matcher mDay = dayPattern.matcher(cal);
	Matcher mUnix = unixPattern.matcher(cal);


	long unix = 0;
	
	if (mUnix.find()) {
	    unix = Long.parseLong(mUnix.group(1));
	} else {
	    //should throw an exception
	    System.out.println(cal);
	}

	Calendar cur = new GregorianCalendar();
	long diff = unix - cur.getTimeInMillis();
	
	if (diff > autoApproveThreshold) {
	    return false;
	} 
	return true;
    }

    /**
     * Returns the number of controls
     *
     */
    private int controlCount(String workerid) {
	BasicDBObject query = new BasicDBObject();
	query.put("workerid",workerid);

	DBCursor find = controlColl.find(query);
	return find.count();

    }

    /**
     * Returns the percentage of the controls the worker
     * got correct
     *
     */
    private double percentageCorrect(String workerid) {
	
	//check whether it has been calculated before


	if (percentCorrectStorage.keySet().contains(workerid)) {
	    return percentCorrectStorage.get(workerid);
	}

	

	BasicDBObject query = new BasicDBObject();
	query.put("workerid",workerid);

	DBCursor find = controlColl.find(query);
	    
	int total = 0;
	int totalCorrect = 0;
	for (DBObject control : find) {
	    boolean response = (Boolean)control.get("correct");
		
	    if (response)
		++totalCorrect;
	    ++total;
	}

	double percentCorrect = (double)totalCorrect / (double)total;
	percentCorrectStorage.put(workerid,percentCorrect);

	return percentCorrect;


    }

    /**
     * Extracts the controls from the submitted pile
     */
    private void extractControls() {
	
	DBCursor cursor = submittedColl.find();
 
	//pulls out the controls
	for (DBObject doc : cursor) {

	    try {
		
		int[] controls = {Integer.parseInt((String)doc.get("control1")), Integer.parseInt((String)doc.get("control2"))};
		String[] controlClasses = {(String)doc.get("control_class1"),(String)doc.get("control_class2")};  
		String[] controlSentences = {(String)doc.get("sentence" + controls[0]), (String)doc.get("sentence" + controls[1])};
		String[] controlSentenceIds = {(String)doc.get("sentence" + controls[0]), (String)doc.get("sentence" + controls[1])};
		String[] controlDLevels = {(String)doc.get("DLevel" + controls[0]), (String)doc.get("DLevel" + controls[1])};
		String[] controlDClasses = {(String)doc.get("DClass" + controls[0]),(String)doc.get("DClass" + controls[1])};
		
		boolean[] isCorrect = {eval.evaluate(controlClasses[0],controlDClasses[0]), eval.evaluate(controlClasses[0],controlDClasses[1])};
		
		BasicDBObject[] controlDocs = {new BasicDBObject(), new BasicDBObject()};
		
		for (int i = 0; i < 2; ++i) {
		    controlDocs[i].put("control_sentence",controlSentences[i]);
		    controlDocs[i].put("control_class",controlClasses[i]);
		    
		    if (controlDClasses[i] != null) {
			controlDocs[i].put("control_dclass",controlDClasses[i]);
		    } else {
			controlDocs[i].put("control_dclass","msa");
		    }
		    controlDocs[i].put("correct",isCorrect[i]);
		    
		    controlDocs[i].put("workerid",(String)doc.get("workerid"));
		    controlDocs[i].put("assignmentid",(String)doc.get("assignmentid"));
		    controlDocs[i].put("hitid",(String)doc.get("hitid"));
		    
		    if (controlColl.find(controlDocs[i]).hasNext() == false) {
			
			controlColl.insert(controlDocs[i]);
		    }
		}
	    } catch (NumberFormatException nfe) {
		submittedColl.remove(doc);
		errorColl.insert(doc);
	    }
	}	
    }


    public static void main(String[] args) throws UnknownHostException {
	new Grader(new ArabicDialectEval());
    }
    
}
