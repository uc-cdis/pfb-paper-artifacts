import gen3.auth
from gen3.auth import Gen3Auth
import gen3.submission
from gen3.submission import Gen3Submission
from datasimulator import graph
from dictionaryutils import DataDictionary, dictionary
import glob
import time
import json

def initalize_dictionary(url):
	dictionary.init(DataDictionary(url=url))

def initalize_graph(dictionary, program, project):
	built_graph = graph.Graph(dictionary, program, project)
	built_graph.generate_nodes_from_dictionary()
	built_graph.construct_graph_edges()
	return built_graph

auth = Gen3Auth(refresh_file="credentials.json")
sub = Gen3Submission(auth)

initalize_dictionary("https://s3.amazonaws.com/dictionary-artifacts/datadictionary/develop/schema.json")

built_graph = initalize_graph(dictionary, "DEV", "test")

built_graph.generate_nodes_from_dictionary()
built_graph.construct_graph_edges()

submission_order = built_graph.generate_submission_order()

so = []

for i in submission_order:
	if i.name not in so:
		so.append(i.name)

so.remove("project")

print(so[::-1])



sub.delete_nodes("DEV", "test", so[::-1])
