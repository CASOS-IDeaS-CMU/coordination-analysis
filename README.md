### COORDINATION ANALYSIS 

Constructing multi-view synchronized action networks of Twitter users.

```
Magelinski, T., Ng, L. H. X., & Carley, K. M. (2021). [A Synchronized Action Framework for Responsible Detection of Coordination on Social Media](https://arxiv.org/abs/2105.07454). MAISoN’21: Mining Actionable Insights from Social Networks.

```

Input: 
    Single Twitter V1 or V2 .json file, or a directory of these files
    Timewindow to consider (in minutes)
Output: 
    .csv containing a weighted multi-view edgelist where actors are linked based on the number of times they took the same action within the time-window

To run: 
Change lines 13 and 14 of `general_coordination_output.py` to your data and output directory