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

 All processed data sets are stored under ~/src/main/resources/[DATASET].<br>
 Each dataset has a stream folder ("Streams") and a workload folder("Queries").<br>
 Different workload files have different number of queries.<br>

Output
----

 Outputs are under ~/output/"dataset", file name is in EXP_[X]_[method].csv form.
 The output stores the latency, memory, throughput for each model in an experiment.
 The output path is generated automatically if it doesn't exist.


EXECUTION
====

Main Function
----


 Main function: ~/src/main/java/executor/main.java<br>
 simply run the main function directly.

 Main function includes two experiments in the paper:
 * Hamlet versus state-of-the-art approaches(Ridesharing data set)<br>
     Figure 14, 16<br>
     output directory: ~/output/RideSharing


 * Dynamic versus static sharing decision (Ridesharing data set)<br>
     Figure 17<br>
     output directory: ~/output/RideSharing

Experiment
----
 Main function read the experiment number from the file ~/ExpNo.txt and increment it after the experiment.<br>
 For the first experiments, we run two methods varying:
* The events per window
* The number of queries

 For the last experiment, we run two methods varying:
* The events per window
* The burst size


 In each of these methods, we run the models several iterations and log the results of latency, throughput and memory for each model in each iteration.
 Plots are based on the average of these results over all iterations for a single experiment.

