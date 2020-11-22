# PROJECT STRUCTURE


## Original Submission


oldSubmission/HamletOriginalSubmission.jar

This is the jar of the original submission, including all the experiments Figure 8-10

Due to the limited space, all datasets are samples.

Run the jar under its directory, the system will detect the datasets folder and run the respective experiments.

Some experiments with larger epw may fail since the limited length of the sample datasets.


## Revision Submission


### 1.Packages

**base:**

  classes of basic data structures including
   
  * dataset schema
  * attribute of the schema
  * stream event
  * event type
  * snapshot
  * template
  * pane

**query:**

  classes of clauses of a query, including aggregator, pattern, predicate, window and groupby. 
  
  QueryParser parses a query string into a Query object.

**stream:**

  stream related classes, including stream loader and partitioner.
    
**users:**

  the dataset specifications. Only have the Nasdaq Stock dataset specifications for now.

**workload:**

  workloadTemplate and generator produce the workload file. The generated workloads are under src/main/resources/Nasdaq
    
  WorklaodAnalyzer parses a workload file into a workload object and analyzes the sharing opportunity.

**graph**

  tools: including all the managers of graphlets, snapshots, predecessors, windows.
  
  graphlet: the abstractions of graphlet
  
  static and dynamic graphs
    
**optimizer:**

  the dynmaic optimizer of dynamic hamlet.
    
**executor:**

  executor class has the methods of a single run of static and dynamic hamlet. Experiment provides the actual setting and methods of the whole experiment.
    
  LocalMain is the main class of local running on an IDE. 
    
  serverMain is the main class for the jar that could be run on a server.


### 2.Dataset

  src/main/resources/Nasdaq/Nasdaq_sample.csv is a sample of the dataset, it has 50k events.


### 3.Output


 Outputs are under ~/output/output
 
 The output stores the details of each experiment including latency, memory, throughput, overhead and decisions of the dynamic optimizer.
 
 The output path is generated automatically if it doesn't exist.


### 4.Execution


#### 4.1 Main Function


 Main function: src/main/java/hamlet/executor/LocalMain.java<br>
 
 simply run the main function directly.

 Main function compares static VS. dynamic in two settings:
 
 * vary query number

 * vary events per window


 In each of these settings, we run the models several iterations with one setting and log the results of latency, throughput, memory and other details for each model in each iteration.
 
 All charts are plotted using the average of iterations for a unique setting of experiment.