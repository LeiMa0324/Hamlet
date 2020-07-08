PROJECT STRUCTURE
====

Packages
----
**dataGenerator package**
* Generate the ride sharing stream
* Generate the workload for all three data sets

**dataProcessing package**
* Process the raw data of NYC Taxi and Smart Home data sets

**baselines package**
* All baseline models including greta, sharon, mcep

**hamlet package**
* Graph - super class of all hamlet graphs
* HamletGraph - hamlet graph without predicates implementation
* StaticGraph - static hamlet with decision of sharing under situations of predicates
* DynamicGraph - dynamic hamlet with benefit model and dynamic decisions of sharing or not sharing

**executor package**
* Executor to run the experiments
* Main function

Dataset
----

 NYC Taxi dataset: https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-01.csv
 
 Smart Home dataset: http://www.doc.ic.ac.uk/~mweidlic/sorted.csv.gz

 All processed data sets are stored under ~/src/main/resources/[DATASET].
 Each dataset has a stream folder ("Streams") and a workload folder("Queries").
 Different workload files have different number of queries.

Output
----

 Outputs are under ~/output/"dataset", file name is in EXP_[X]_[method].csv form.
 The output stores the latency, memory, throughput for each model in an experiment.
 The output path is generated automatically if it doesn't exist.


EXECUTION
====

Main Function
----


 Main function: ~/src/main/java/executor/main.java
 simply run the main function directly.

 Main function includes four experiments in the paper:
 * Hamlet versus state-of-the-art approaches(Ridesharing data set)
     Figure 14, 16
     output directory: ~/output/RideSharing

 * Hamlet versus state-of-the-art approaches(NY City Taxi data set)
     Figure 15
     output directory: ~/output/NYCTaxi

 * Hamlet versus state-of-the-art approaches(Smart Home data set)
     Figure 15
     output directory: ~/output/SmartHome

 * Dynamic versus static sharing decision (Ridesharing data set)
     Figure 17
     output directory: ~/output/RideSharing

Experiment
----
    
 For the first three experiments, we run two methods varying:
* The events per window
* The number of queries

 For the last experiment, we run two methods varying:
* The events per window
* The burst size


 In each of these methods, we run the models several iterations and log the results of latency, throughput and memory for each model in each iteration.
 Plots are based on the average of these results over all iterations for a single experiment.

