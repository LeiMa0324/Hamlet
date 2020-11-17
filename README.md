# PROJECT STRUCTURE


## Original Submission


out/artifacts/HamletOriginalSubmission.jar

This is the jar of the original submission, including all the experiments Figure 14-16


## Revision Submission


### 1.Packages

base:

classes of basic data structures including dataset schema, attribute of the schema, stream event, event type, snapshot and template.

query:

classes of components of a query, including aggregator, pattern, predicate, window and groupby. QueryParser parses a query string into a Query object.

stream:

stream related classes, including stream loader and partitioner.
    
users:

the dataset specifications. 

workload:

workloadTemplate and generator produce the workload file. The generated workloads are under src/main/resources/Nasdaq
    
WorklaodAnalyzer parses a workload file into a workload object and analyzes the sharing opportunity.
    
optimizer:

the dynmaic optimizer of dynamic hamlet.
    
executor:

executor class has the methods of a single run of static and dynamic hamlet. Experiment provides the actual setting and methods of the whole experiment.
    
LocalMain is the main class of local running on an IDE. 
    
serverMain is the main class for the jar that could be run on a server.


### 2.Dataset

src/main/resources/Nasdaq/Nasdaq.csv


### 3.Output


 Outputs are under ~/output/output
 
 The output stores the details of each experiment including latency, memory, throughput, overhead and decisions of the dynamic optimizer.
 
 The output path is generated automatically if it doesn't exist.


### 4.Execution


#### 4.1 Main Function


 Main function: src/main/java/hamlet/executor/LocalMain.java<br>
 
 simply run the main function directly.

 Main function compares static VS. dynamic in two settings:
 
vary query number
vary events per window


 In each of these settings, we run the models several iterations and log the results of latency, throughput and memory for each model in each iteration.
 Plots are based on the average of these results over all iterations for a single experiment.

