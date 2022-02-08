import gen3.auth
from gen3.auth import Gen3Auth
import gen3.submission
from gen3.submission import Gen3Submission
from datasimulator import graph
from dictionaryutils import DataDictionary, dictionary
import glob
import time
import json

startTime = time.time()
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
	# print(i.name)

# print("submission order")
# print(so)

thing = glob.glob("../json/100000/*.json")
fileName = {}
for file in thing:
	fileName[file.replace("../json/100000/", "").replace(".json", "")] = file

for i in so:
	print("submitting file: ", fileName[i])
	print("node number: ", so.index(i))
	with open(fileName[i], "r") as jsonFile:
		json_sub = json.loads(jsonFile.read())
		if json_sub == []:
			print("This is an empty file we are skipping submission for: ", i)
			continue
		if len(json_sub) > 100:
			start = 0
			end = 100
			while start < len(json_sub):
				sub.submit_record("DEV", "test", json_sub[start:end])
				start += 100
				end += 100
		else:
			sub.submit_record("DEV", "test", json_sub)
		print("this node finished: ", (time.time()-startTime), " after starting")


endTime = time.time()

print("time this script took:", (endTime-startTime))

# submit record
# sub.submit_record("DEV", "test", thing)
