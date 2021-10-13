### COORDINATION ANALYSIS 

Constructing multi-view synchronized action networks of Twitter users.

```
Magelinski, T., Ng, L. H. X., & Carley, K. M. (2021). [A Synchronized Action Framework for Responsible Detection of Coordination on Social Media](https://arxiv.org/abs/2105.07454). MAISoN’21: Mining Actionable Insights from Social Networks.

```

Input (in CONFIG.json): 
    - Either a single .json or .json.gz file, or a directory containing many of these files
    - The API version used to get the twitter data "V1" or "V2" (all files need to be the same type)
    - Timewindow to consider (in minutes)
    - Filename to write edgelist to (must be .csv)
    - action_types, a list of the action types to consider. The available options are url, hashtag, mention, and combined. Where combined looks at url-hashtag combination

Output: 
    .csv containing a weighted multi-view edgelist where actors are linked based on the number of times they took the same action within the time-window

To run: 
1. Create a new python enviornment `python3 -m venv venv`
2. Activate it. In windows: `venv\Scripts\Activate.bat` or Mac/Linux: `source venv\bin\activate`
3. Install dependencies: `pip install -r requirements.txt`
4. Edit `CONFIG.json` with your parameters
5. Run the program `python main.py`
