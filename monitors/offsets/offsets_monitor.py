import argparse
import json
import time
from kazoo.client import KazooClient
from kazoo.client import KazooState
from kafka import SimpleClient
from kafka.protocol.offset import OffsetRequest, OffsetResetStrategy
from kafka.common import OffsetRequestPayload

# Constants
MEASUREMENT_INTERVAL_SEC = 3
# KAFKA_HOST = 'kafka1:9092'
KAFKA_HOST = 'localhost:9092'
# ZK_HOST = 'zkserver1:2181'
ZK_HOST = 'localhost:2181'

class KafkaClient(object):
	def __init__(self, kafka_host, topic, group_id):
		self.client = SimpleClient(kafka_host)
		self.topic = topic
		self.partitions = self.client.topic_partitions[topic]
		self.group_id = group_id

	def close(self):
		self.client.close()

	def get_tail_offsets(self):
		request = [OffsetRequestPayload(self.topic, p, -1, 1) for p in self.partitions.keys()]
		response = self.client.send_offset_request(request)	
		offsets = {r.partition: r.offsets[0] for r in response} # build dictionary
		return offsets

class ZkClient(object):
	def __init__(self, zk_host, topic, partitions, group_id):
		self.client = KazooClient(hosts=zk_host, read_only=True)
		self.client.start()
		self.topic = topic
		self.partitions = partitions
		self.group_id = group_id

	def close(self):
		self.client.stop()

	def get_commited_offsets(self):
		zk_path = '/' + self.topic + '/' + self.group_id
		offsets = {}
		for p in self.partitions.keys():
			path = zk_path + '/partition_' + str(p)
			if self.client.exists(path):
				data, stat = self.client.get(path)
				json_data = json.loads(data)
				if json_data['partition'] == p:
					offsets[p] = json_data['offset']
				else:
					print('Requested and received partitions do not match: {} vs. {}'. \
						  format(json_data['partition'], p))
		return offsets
	

def run_monitor(kafka, zk, interval_sec):
	while True:
		tail_offsets = kafka.get_tail_offsets()
		commited_offsets = zk.get_commited_offsets()
		partitions = []
		for p in kafka.partitions.keys():
			if p in commited_offsets:
				partitions.append({'partition_' + str(p) : \
								   {'tail': tail_offsets[p], 'commited': commited_offsets[p], \
									'lag': tail_offsets[p] - commited_offsets[p]}})
		offsets = {'offsets': partitions}
		print(json.dumps(offsets))
		time.sleep(interval_sec)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('-k', '--kafka_host', default=KAFKA_HOST, type=str)
	parser.add_argument('-z', '--zk_host', default=ZK_HOST, type=str)
	parser.add_argument('-t', '--topic', required=True, type=str)
	parser.add_argument('-g', '--group_id', required=True, type=str)
	parser.add_argument('-i', '--interval_sec', default=MEASUREMENT_INTERVAL_SEC, type=int)
	args = parser.parse_args()
	print('Arguments: {}'.format(vars(args)))

	kafka = KafkaClient(args.kafka_host, args.topic, args.group_id)
	zk = ZkClient(args.zk_host, args.topic, kafka.partitions, args.group_id)

	# infinite loop
	run_monitor(kafka, zk, args.interval_sec)

	# never come here
	kafka_close(kafka)
	zk_close(zk)

	


	




	

