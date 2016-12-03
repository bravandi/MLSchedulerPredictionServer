package edu.purdue.mlscheduler;

public enum MachineLearningAlgorithm {
	RepTree, Regression, J48, BayesianNetwork;

	public static MachineLearningAlgorithm parse(String input) {

		switch (input.toLowerCase()) {
		case "reptree":

			return RepTree;

		case "regression":

			return Regression;

		case "j48":

			return J48;

		case "bayesiannetwork":

			return BayesianNetwork;
		}

		return null;
	}
}
