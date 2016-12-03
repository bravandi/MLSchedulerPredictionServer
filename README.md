# Machine Learning BlockStorage Scheduler

This project simulates a BlockStorage in order to assess scheduling algorithms. Requirments to run the project are:

  - Jre 1.8.0
  - MySQL Database

## Steps to run the application:

  - Execute "restore.sql" in order to initialize the database schema
  - Initialize test workload in the database
  - Change the connection string at "edu.purdue.simulation.Database"
```
.getConnection("jdbc:mysql://SERVER/BlockStorageSimulator?user=...&password=...")
```
  - in "edu.purdue.simulation.BlockStorageSimulator" adjust bellow parts
   - workload ID. The workload that will be retrieved from the database.
```        
Workload workload = new Workload(BigDecimal.valueOf(WORKLOAD_ID));
```
   - Choose a scheduling algorithm for the experiment. Currently 2 algorithms are implimented.
```
Scheduler scheduler = new edu.purdue.simulation.blockstorage.StatisticalGroupping(experiment, workload);
```
```
Scheduler scheduler = new edu.purdue.simulation.blockstorage.MaxCapacityFirstScheduler(experiment, workload);
```
  - run the experiment
```
"edu.purdue.simulation.BlockStorageSimulator.main"
```

## Results
The results of scheduling on a workload will be stored in the database distinguished bt the "experiment" table. For example the below query returns the "Total number of backends used to schedule the wrokload" and the "total free space within all the backends" for a specific expermint. In this case the experiment ID is 47.

```
Select	Count(*)			As NumberOfUsedBackEnds,
		Sum(FreeSpace)		As TotalFreeSpaceWithinAllBackends
	From	(
				Select	BE.ID								As	BackEnd_ID,
						BE.capacity - Sum(V.capacity)		As	FreeSpace
					From	BackEnd				BE
								Inner	Join
							volume				V
								On	BE.experiment_ID		= 47	And
									V.BackEnd_ID	= BE.ID
					Group	By	BE.ID
			)	As R;
```

## Scheduling Algorithms

### Statistical Groupping 

### Max Capacity First Scheduler

### Version
1.0.0
