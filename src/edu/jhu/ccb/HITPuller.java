package edu.jhu.ccb;

import java.io.File;
import java.io.IOException;

import java.util.List;
import java.util.LinkedList;
import java.net.UnknownHostException;

import com.amazonaws.mturk.addon.HITDataCSVReader;
import com.amazonaws.mturk.addon.HITDataCSVWriter;
import com.amazonaws.mturk.addon.HITDataInput;
import com.amazonaws.mturk.addon.HITTypeResults;
import com.amazonaws.mturk.dataschema.QuestionFormAnswers;
import com.amazonaws.mturk.dataschema.QuestionFormAnswersType;
import com.amazonaws.mturk.dataschema.impl.QuestionFormAnswersImpl;
import com.amazonaws.mturk.dataschema.impl.QuestionFormAnswersTypeImpl.AnswerTypeImpl;
import com.amazonaws.mturk.requester.Assignment;
import com.amazonaws.mturk.requester.AssignmentStatus;
import com.amazonaws.mturk.service.axis.RequesterService;
import com.amazonaws.mturk.util.PropertiesClientConfig;
import com.amazonaws.mturk.service.exception.ObjectDoesNotExistException;

import com.mongodb.Mongo;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.ServerAddress;

public class HITPuller {

    private static final String db_name = "arabic_dialect";
    private static final String submitted = "submitted";
    private static final String approved = "approved";
    private static final String pending = "pending";
    private static final String rejected = "rejected";

    private static final String configPath = "mturk.properties";


    private RequesterService service;
    

    /**
     * Constructor
     *
     */
    public HITPuller(String dirName) throws UnknownHostException,IOException {
	service = new RequesterService(new PropertiesClientConfig(configPath));


	List<String> hitIds = new LinkedList<String>();

	File dir = new File(dirName);
	
        for (String successFile : dir.list()) {
	    HITDataInput success = new HITDataCSVReader(dir + "/" + successFile);
	    //unix specific should use java library 
	    for (int i = 1; i < success.getNumRows(); ++i)
		hitIds.add(success.getRowValues(i)[0]);
	}


	List<List<QuestionFormAnswersType.AnswerType>> results = new LinkedList<List<QuestionFormAnswersType.AnswerType>>();

	int count = 0;
	for (String hitId : hitIds) {


	    List<List<QuestionFormAnswersType.AnswerType>> tmp = reviewAnswers(hitId);
	    
	   	    
	    if (tmp != null) {
		results.addAll(tmp);
	    }

	    count += 1;
	    
	    System.out.format("%d\t%s\n",count,hitId);
	}
	populateDB(results);
   }

    /**
     * Pulls all HITs down from a directory full of successfiles and dumps
     * all assignments that have the status "submitted" into the database
     * For further processing
     */

    private void populateDB(List<List<QuestionFormAnswersType.AnswerType>> results) throws UnknownHostException {

	
	Mongo m = new Mongo();
	DB db = m.getDB(db_name);
	DBCollection coll = db.getCollection(submitted);

	
	for (List<QuestionFormAnswersType.AnswerType> answers : results) {


	    
	    BasicDBObject doc = new BasicDBObject();
	    
	    for (QuestionFormAnswersType.AnswerType answer : answers) {
		doc.put(answer.getQuestionIdentifier(),answer.getFreeText());
	    }


	    if (coll.find(doc).hasNext() == false) {
		coll.insert(doc);
	    }
	}
 

    }
 
    /**
     * Gets the results from a hit id
     *
     */

    private List<List<QuestionFormAnswersType.AnswerType>> reviewAnswers(String hitId) {
      
	
	List<List<QuestionFormAnswersType.AnswerType>> results = new LinkedList<List<QuestionFormAnswersType.AnswerType>>();

	 

        try { 
	    Assignment[] assignments = service.getAllAssignmentsForHIT(hitId);

	    
	    for (Assignment assignment : assignments) {
				
		//hack to get approved hits
		if (assignment.getAssignmentStatus() == AssignmentStatus.Submitted) {
		    
		    //By default, answers are specified in XML
		    String answerXML = assignment.getAnswer();
		    QuestionFormAnswers qfa = RequesterService.parseAnswers(answerXML);

		    List<QuestionFormAnswersType.AnswerType> answers = (List<QuestionFormAnswersType.AnswerType>) qfa.getAnswer();
		
		
		    QuestionFormAnswersType.AnswerType workerid = new com.amazonaws.mturk.dataschema.impl.QuestionFormAnswersTypeImpl.AnswerTypeImpl();
		    workerid.setQuestionIdentifier("workerid");
		    workerid.setFreeText(assignment.getWorkerId());

		
		    QuestionFormAnswersType.AnswerType hitid = new com.amazonaws.mturk.dataschema.impl.QuestionFormAnswersTypeImpl.AnswerTypeImpl();
		    hitid.setQuestionIdentifier("hitid");
		    hitid.setFreeText(assignment.getHITId());
		

		    QuestionFormAnswersType.AnswerType assignmentid = new com.amazonaws.mturk.dataschema.impl.QuestionFormAnswersTypeImpl.AnswerTypeImpl();
		    assignmentid.setQuestionIdentifier("assignmentid");
		    assignmentid.setFreeText(assignment.getAssignmentId());


		    QuestionFormAnswersType.AnswerType autoApproval = new com.amazonaws.mturk.dataschema.impl.QuestionFormAnswersTypeImpl.AnswerTypeImpl();
		    autoApproval.setQuestionIdentifier("auto_approval");
		    autoApproval.setFreeText(assignment.getAutoApprovalTime().toString());
		
		    QuestionFormAnswersType.AnswerType acceptTime = new com.amazonaws.mturk.dataschema.impl.QuestionFormAnswersTypeImpl.AnswerTypeImpl();
		    acceptTime.setQuestionIdentifier("accept_time");
		    acceptTime.setFreeText(assignment.getAcceptTime().toString());
		    
		    QuestionFormAnswersType.AnswerType submitTime = new com.amazonaws.mturk.dataschema.impl.QuestionFormAnswersTypeImpl.AnswerTypeImpl();
		    submitTime.setQuestionIdentifier("submit_time");
		    submitTime.setFreeText(assignment.getSubmitTime().toString());
		    
	      
		    
		    answers.add(workerid);
		    answers.add(hitid);
		    answers.add(assignmentid);
		    answers.add(autoApproval);
		    answers.add(acceptTime);
		    answers.add(submitTime);

		    results.add(answers);
		}
	
		
	    }
	} catch (ObjectDoesNotExistException ex) {
	    System.err.println(hitId + " does not exist. Skipping.");
	}

	return results;


    }

    public static void main(String[] args) throws UnknownHostException, IOException {

	new HITPuller("success_files");
    }
}
