Implementation of Youtube's Absorption algorithm that uses MapReduce to calculate friend recommendations. This was intended for use
in a "Pennbook" social network web app based on Facebook that allowed users to create profiles, add friends, and select 
organization afflications, interests, etc. In the friend graph, we represented users as nodes and calculate edges between 
two nodes based on the sum of 
1. # of shared interests
2. whether or not affiliation is shared
3. # of mutual friends and whether or not they are friends

Input: A directed, weighted graph where each line in the file is an edge
Intermediate Format: key = a user, value = a list of weighted edges and weighted labels
Output: a list of weighted labels for each user, representing the friend recommendations

Use the provided jar to run from the command line: 
hadoop jar recs.jar src.AdsorptionDriver {arguments}

To run the full MapReuce job use the arguments: 
composite <inputDir> <outputDir> <intermDir1> <intermDir2> <diffDir> <#reducers>

To reproduce the provided example, run: 
hadoop jar recs.jar src.AdsorptionDriver composite input output int1 int2 diff 2
