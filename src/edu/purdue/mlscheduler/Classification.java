package edu.purdue.mlscheduler;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;

import java.math.BigInteger;

import weka.classifiers.Classifier;
import weka.classifiers.AbstractClassifier;
import weka.classifiers.bayes.BayesNet;
import weka.classifiers.evaluation.Evaluation;
import weka.classifiers.trees.J48;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;

@SuppressWarnings("deprecation")
public class Classification {
	public class Predictions{
		String cinder_id;
		double[] read_predictions;
		double[] write_predictions;
	}
	
	public class ClassifierSet{
		String cinder_id;
		public Classifier read_classifier;
		public Classifier write_classifier;
	}
	
	public static Connection CurrentConnection;
	
	public Map<String, ClassifierSet> classifier_sets;
	
	public Evaluation classifierEvaluation;
	
	public Classification(){
		this.classifier_sets = new HashMap<>();
	}
	
	public static Connection getConnection() throws SQLException, Exception {

		if (CurrentConnection != null && !CurrentConnection.isClosed())

			return CurrentConnection;
		
		Class.forName("com.mysql.jdbc.Driver");
		
		CurrentConnection = DriverManager.getConnection(
				"jdbc:mysql://10.18.75.100:3306/MLScheduler?useServerPrepStmts=false&rewriteBatchedStatements=true&allowMultiQueries=true",
				"babak", "123");
		
//		CurrentConnection = DriverManager.getConnection(
//				"jdbc:jdbc:mysql://localhost:3306/blockstoragesimulator?user=root&useServerPrepStmts=false&rewriteBatchedStatements=true&allowMultiQueries=true",
//				"root", "234");

		return CurrentConnection;
	}

	public LinkedList<Predictions> predict(int clock, BigInteger volume_request_id) throws Exception{
		BackendWeights_VolumeRequest backend_current_weights = new BackendWeights_VolumeRequest(volume_request_id);
		
		LinkedList<Predictions> results = new LinkedList<>();
		
		for (String cinder_id : this.classifier_sets.keySet()){
			Predictions predictions = new Predictions();
			results.add(predictions);
			ClassifierSet classifier_set = this.classifier_sets.get(cinder_id);

			predictions.cinder_id = cinder_id;
			
			predictions.read_predictions = this.predict_with_classifier(
					classifier_set.read_classifier,
					clock,
					backend_current_weights.volume_request,
					backend_current_weights.backend_weight_map.get(cinder_id),
					true);
			
			predictions.write_predictions = this.predict_with_classifier(
					classifier_set.read_classifier,
					clock,
					backend_current_weights.volume_request,
					backend_current_weights.backend_weight_map.get(cinder_id),
					false);
		}
		
		return results;
	}
	
	protected double[] predict_with_classifier(
			Classifier classifier, 
			int clock,
			BackendWeights_VolumeRequest.VolumeRequest volume_request,
			BackendWeights_VolumeRequest.BackendWeight backend_weight,
			boolean is_for_read) throws Exception {
		
		Instances read_weka_dataset = Classification.create_weka_dataset(0, true);
		DenseInstance read_instance = new DenseInstance(read_weka_dataset.numAttributes());
		read_instance.setDataset(read_weka_dataset);
		
		Instances write_weka_dataset = Classification.create_weka_dataset(0, false);
		DenseInstance write_instance = new DenseInstance(write_weka_dataset.numAttributes());
		write_instance.setDataset(write_weka_dataset);

		int volume_count = backend_weight.live_volume_count_during_clock + 1;
		
		
		read_instance.setValue(read_weka_dataset.attribute("clock"), clock);
		read_instance.setValue(read_weka_dataset.attribute("volume_count"), volume_count);
		
		double[] predictions = null;
		
		if (is_for_read){
			
			read_instance.setValue(
					read_weka_dataset.attribute("read_req_total_iops"),
					backend_weight.requested_read_iops_total + volume_request.read_iops);
			
			predictions = classifier.distributionForInstance(read_instance);
				
		}else{
			write_instance.setValue(
					write_weka_dataset.attribute("write_req_total_iops"),
					backend_weight.requested_read_iops_total + volume_request.write_iops);
			
			predictions = classifier.distributionForInstance(write_instance);
		}

		return predictions;
	}
	
	public void create_model(int training_data, boolean for_read_iops){
		
	}
	
	@SuppressWarnings({})
	public static Instances create_weka_dataset(int includeViolationsNumber, boolean is_for_read) {

//		if (includeViolationsNumber == -1 && wekaDataset != null)
//
//			return wekaDataset;

		FastVector<Attribute> attributes_vector = new FastVector<Attribute>(4);

		Attribute clock_attribute = new Attribute("clock");

		Attribute volume_count_attribute = new Attribute("volume_count");

		FastVector<String> fvClassVal = new FastVector<String>(4);

		fvClassVal.addElement("v1");
		fvClassVal.addElement("v2");
		fvClassVal.addElement("v3");
		fvClassVal.addElement("v4");

		Attribute requested_read_or_write_iops_total= null;
		Attribute read_violation_GroupAttribute = null;
		Attribute write_violation_GroupAttribute = null;
		
		if(is_for_read){
			read_violation_GroupAttribute = new Attribute("read_vio_group", fvClassVal);
			attributes_vector.addElement(read_violation_GroupAttribute); // 2
			
			requested_read_or_write_iops_total = new Attribute("read_req_total_iops");
		}
		else{
			write_violation_GroupAttribute = new Attribute("write_vio_group", fvClassVal);
			attributes_vector.addElement(write_violation_GroupAttribute);
			
			requested_read_or_write_iops_total = new Attribute("write_req_total_iops");
		}

		attributes_vector.addElement(clock_attribute); // 0

		attributes_vector.addElement(volume_count_attribute); // 1

		attributes_vector.addElement(requested_read_or_write_iops_total); // 3

		Attribute violationNumberAttribute = null;

//		Attribute clockModAttribute = null;

//		if (includeViolationsNumber > 0) {
//
//			violationNumberAttribute = new Attribute("vioNum");
//
//			attributesVector.addElement(violationNumberAttribute);
//
//			clockModAttribute = new Attribute("clockMod");
//
//			attributesVector.addElement(clockModAttribute);
//		}

		Instances trainingInstances = new Instances("Rel", attributes_vector, 10);
		
		if(is_for_read){
			trainingInstances.setClass(read_violation_GroupAttribute);
		}
		else{
			trainingInstances.setClass(write_violation_GroupAttribute);
		}

//		wekaDataset = trainingInstances;

		return trainingInstances;
	}
	
	/**
	 * @param training_resultset
	 * @param backend
	 * @param path
	 * @param includeViolationsNumber
	 *            0: Don't include SLA violations number 1: include SLA
	 *            violations number 2: include SLA violation number and remove
	 *            violation label
	 * @throws Exception
	 */
//	@SuppressWarnings({})
	private Instances[] create_training_instances(
			ResultSet training_resultset, 
			String path, 
			int includeViolationsNumber,
			boolean updateLearningModel) throws Exception {
		
		Instances result[] = new Instances[2];
		
		//make sure to retun weka model / models
		Instances weka_dataset_for_read = Classification.create_weka_dataset(includeViolationsNumber, true);
		Instances weka_dataset_for_write = Classification.create_weka_dataset(includeViolationsNumber, false);
		
		result[0] = weka_dataset_for_read;
		result[1] = weka_dataset_for_write;
		
		while (training_resultset.next()) {

			Instance training_instance_for_read = new DenseInstance(weka_dataset_for_read.numAttributes());
			Instance training_instance_for_write = new DenseInstance(weka_dataset_for_write.numAttributes());
			
			
			int clock = training_resultset.getInt(1);

			training_instance_for_read.setValue(weka_dataset_for_read.attribute("clock"), clock);
			training_instance_for_write.setValue(weka_dataset_for_read.attribute("clock"), clock);

			String group = "";
			Attribute att = null;
			
			training_instance_for_read.setValue(weka_dataset_for_read.attribute("volume_count"), training_resultset.getInt(4));
			training_instance_for_write.setValue(weka_dataset_for_read.attribute("volume_count"), training_resultset.getInt(4));
			
			// rs.getInt(1);
			int sampled_read_violation_count = training_resultset.getInt(3);
			// rs.last() rs.getRow()
			if (weka_dataset_for_read.attribute("read_vio_group") != null) {

				if (sampled_read_violation_count == 0) {
					group = "v1";
				} else if (sampled_read_violation_count > 0 && sampled_read_violation_count <= 2) {
					group = "v2";
				} else if (sampled_read_violation_count > 2 && sampled_read_violation_count <= 4) {
					group = "v3";
				} else {
					group = "v4";
				}

				att = weka_dataset_for_read.attribute("read_vio_group");
				
//				weka_dataset_for_read.setClass(att);
				training_instance_for_read.setValue(att, group);
			}
			
			int sampled_write_violation_count = training_resultset.getInt(3);
			// rs.last() rs.getRow()
			if (weka_dataset_for_write.attribute("write_vio_group") != null) {

				if (sampled_write_violation_count == 0) {
					group = "v1";
				} else if (sampled_write_violation_count > 0 && sampled_write_violation_count <= 2) {
					group = "v2";
				} else if (sampled_read_violation_count > 2 && sampled_write_violation_count <= 4) {
					group = "v3";
				} else {
					group = "v4";
				}

				att = weka_dataset_for_write.attribute("write_vio_group");
				
//				weka_dataset_instances_for_write.setClass(att);
				training_instance_for_write.setValue(att, group);
			}
			
			training_instance_for_read.setValue(weka_dataset_for_read.attribute("read_req_total_iops"), training_resultset.getInt(6));
			training_instance_for_write.setValue(weka_dataset_for_write.attribute("write_req_total_iops"), training_resultset.getInt(5));

//			if (trainingInstances.attribute("vioNum") != null) {
//
//				trainingInstance.setValue(trainingInstances.attribute("vioNum"), sampled_read_violation_count);
//
//				trainingInstance.setValue(trainingInstances.attribute("clockMod"), clock % Scheduler.modClockBy);
//			}

			// add the instance
			weka_dataset_for_read.add(training_instance_for_read);
			weka_dataset_for_write.add(training_instance_for_write);
		}

		if (updateLearningModel == true) {

//			int trainingSize = trainingInstances.size();
//
//			if (trainingSize < Scheduler.updateLearning_MinNumberOfRecords) {
//
//				System.out.println(
//						"[small traning dataset for feedback] trainingInstances.size() < Scheduler.updateLearningModelByLastNumberOfRecords: "
//								+ trainingInstances.size() + " < " + Scheduler.updateLearning_MinNumberOfRecords);
//
//				return;
//			}
//
//			Scheduler.execute_AllBatchQueries(true);
//
//			double accuracy = backend.updateModel(trainingInstances);
//
//			Object[][] backendAccuracy = BlockStorageSimulator.feedbackAccuracy.get(Experiment.clock.intValue());
//
//			int backendIndex = Experiment.backendList.indexOf(backend);
//
//			backendAccuracy[backendIndex][0] = backend;
//			backendAccuracy[backendIndex][1] = accuracy;

		} else {
//			ArffSaver saver = new ArffSaver();
//
//			saver.setInstances(trainingInstances);
//
//			String saveToPath = "";
//
//			if (path == null || path == "")
//
//				path = Experiment.saveResultPath;
//
//			saveToPath = path + //
//					(Scheduler.isTraining == true ? "TRN_" : "EXP_") //
//					+ backend.getExperiment().getID() + "_" + backend.getID() + "_" + backend.getDescription()
//					+ ".arff";
//
//			saver.setFile(new File(saveToPath));

			//// saver.setDestination(new File(path));

			//saver.writeBatch();
			// Create the instance

			//// edu.purdue.simulation.BlockStorageSimulator.log(Arrays.toString(repTree
			//// .distributionForInstance(iExample)));
		}
		
		return result;
	}
	
	@SuppressWarnings("unused")
	public Classifier create_weka_classifier(Classifier classifier, String params, String path, int classIndex, Instances train) throws Exception {
		
		if(classifier instanceof BayesNet){
			BayesNet bayes_net = (BayesNet)classifier;
			
			bayes_net.setOptions(weka.core.Utils.splitOptions(
				"-D -Q weka.classifiers.bayes.net.search.local.K2 -- -P 1 -S BAYES -E weka.classifiers.bayes.net.estimate.SimpleEstimator -- -A 0.5"));
		}
		
		if(classifier instanceof J48){
			J48 j48 = (J48) classifier;
			
			j48.setUnpruned(true);
		}
		
		
		if (false) {
			String arguments = params;

			classifier = new J48();

			AbstractClassifier.runClassifier(classifier, arguments.split(" "));
			String tos = classifier.toString();
		}

		if (true) {

			if (train == null) {

				BufferedReader reader;

				reader = new BufferedReader(new FileReader(path));

				train = new Instances(reader);

				reader.close();
			}

			// setting class attribute
//			train.setClassIndex(0);
			

//			result = null;
			System.gc();

//			result = new J48();

			classifier.buildClassifier(train);

			// FilteredClassifier fc = new FilteredClassifier();

			// fc.setFilter(rm);

			// fc.setClassifier(j48);

			// fc.buildClassifier(train);

			// "-t D:\\SAS\\2\\514Cat_g3.arff -M 2 -V 0.001 -N 3 -S 1 -L -1 -c
			// 3"

		}

		return classifier;
	}
	
	public double validate_classifier(Classifier classifier, Instances train) throws Exception{

		this.classifierEvaluation = new Evaluation(train);

		Random rand = new Random(1); // using seed = 1

		int folds = 10;

		this.classifierEvaluation.crossValidateModel(classifier, train, folds, rand);

		// train.size()
//		edu.purdue.simulation.BlockStorageSimulator.log(this.classifierEvaluation.toClassDetailsString());
//
//		edu.purdue.simulation.BlockStorageSimulator.log("Accuracy: " + this.classifierEvaluation.pctCorrect()
//				+ " Sample Size: " + train.size() + " backendIndex: " + Experiment.backendList.indexOf(this));

		return this.classifierEvaluation.pctCorrect();
	}

	/**
	 * @param numberOfRecords
	 *            limits the number of records to be in the resultset coming
	 *            from MySQL DB
	 * @param experiment
	 * @param path
	 *            null will use the default path.
	 * @param includeViolationsNumber
	 *            0: Don't include SLA violations number 1: include SLA
	 *            violations number 2: include SLA violation number and remove
	 *            violation label
	 * @throws Exception
	 */
	public void create_models(String path) throws java.lang.Exception {

		Connection connection = Classification.getConnection();

		// ex_ID, size
		try (CallableStatement cStmt = connection.prepareCall("{call get_training_dataset(?)}")) {

			cStmt.setBigDecimal(1, BigDecimal.valueOf(0)); // exp_ID

			cStmt.execute();

			// ResultSet rs = null;

			boolean has_result_set = true;

			boolean is_backend_id_resulset = true;

			BigInteger backend_id;
			String cinder_id = null;

			while (has_result_set) {

				try (ResultSet rs = cStmt.getResultSet()) {

					if (rs == null)

						break;
					
					if (is_backend_id_resulset) {
						// rs.last() rs.getRow()
						rs.next();
						
						backend_id = rs.getBigDecimal(1).toBigInteger();
						cinder_id = rs.getString(2); 

						is_backend_id_resulset = false;

					} else {
						// rs.last() rs.getRow()
//						this.saveBackend(rs, currentBackend, path, includeViolationsNumber,
//								updateLearningModelByLastNumberOfRecords > 0);
						
						Instances[] instances = this.create_training_instances(rs, "", 0, false);
						
						Instances read_instances = instances[0];
						Instances write_instances = instances[1];
						
						ClassifierSet classifier_set = new ClassifierSet();
						this.classifier_sets.put(cinder_id, classifier_set);
						
						J48 read_j48_tree = (J48) create_weka_classifier(new J48(), "", "", 0, read_instances);
						J48 write_j48_tree = (J48) create_weka_classifier(new J48(), "", "", 0, write_instances);
						
						classifier_set.cinder_id = cinder_id;
						classifier_set.read_classifier = read_j48_tree;
						classifier_set.write_classifier = write_j48_tree;
						
						BayesNet read_bayes_net_tree = (BayesNet) create_weka_classifier(new BayesNet(), "", "", 0, read_instances);
						BayesNet write_bayes_net_tree = (BayesNet) create_weka_classifier(new BayesNet(), "", "", 0, write_instances);
						
						is_backend_id_resulset = true;

					}

				}

				has_result_set = !((cStmt.getMoreResults() == false) && //
						(cStmt.getUpdateCount() == -1));
			}

		}
	}
	
	public static void main(String[] args){
		try {
			Classification c = new Classification();
			
			c.create_models("");
			
			LinkedList<Predictions> p = c.predict(0, new BigInteger("1490"));
			
			p.clear();
					
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
