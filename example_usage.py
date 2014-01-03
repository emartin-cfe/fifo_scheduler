import os, time, sys
from glob import glob
sys.path.append('/usr/local/share/fifo_scheduler')
from fifo_scheduler import Factory

# Create factory with one worker on node 1 + two workers on node 2
factory_resources = [("bpsh 2", 2), ("bpsh 1", 1)]
mapping_factory = Factory(factory_resources)
# mapping_factory.hire_worker("bpsh -1")	# Manually add worker to node -1

# Factory can work with multi-threaded jobs (bowtie-specific example below)
root = "/data/miseq/0_FACTORY_TEST"
refs = "/usr/local/share/miseq/refs/cfe"
num_pthreads = 8 

for R1_fastq in glob(root + '/*R1*'):
	R2_fastq = R1_fastq.replace("R1", "R2")
	fields = os.path.basename(R1_fastq).split("_")
	sam_output = "{}/{}.sam".format(root,fields[0])
	command = "bowtie2 --quiet -p {} --local -x {} -1 {} -2 {} -S {}".format(num_pthreads, refs, R1_fastq, R2_fastq, sam_output)
	print "QUEUING: {}".format(command)

	# If queue_work() is done while workers are idle, a tuple containing the popen
	# is spawned, and the request you originally fed in is returned
	queue_request = mapping_factory.queue_work(command)
	if queue_request:
		p, command = queue_request
		print "STARTED: pID {}, {}".format(p.pid, command)

# At a natural barrier, call Factory.supervise() while polling Factory.completely_idle()
while True:

	# Factory.supervise() is similar to Factory.queue_work() - if it successfully pairs
	# pending work with available workers, it returns a list of (popen, command) tuples
	processes_spawned = mapping_factory.supervise()
	if processes_spawned:
		for popen_object, command_invoked in processes_spawned:
			print "STARTED: pID {}, {}".format(popen_object.pid, command_invoked)

	if mapping_factory.completely_idle(): break
	time.sleep(1)
