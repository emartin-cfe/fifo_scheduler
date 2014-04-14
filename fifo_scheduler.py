import atexit, subprocess, sys
from Queue import Queue

class Job:
	"""
	A job is a shell command + optional file paths for stdout/stderr.
	If file paths are undefined, outputs default to the console.
	"""

	def __init__(self, shell_command, standard_output=None, standard_error=None):
		self.shell_command = shell_command
		self.standard_output = standard_output
		self.standard_error = standard_error
		self.standard_output_f = None
		self.standard_error_f = None

	def teardown(self):
		if self.standard_output_f is not None: self.standard_output_f.close()
		if self.standard_error_f is not None: self.standard_error_f.close()

class Job_Queue:
	"""FIFO queue of outstanding jobs"""

	def __init__(self):
		self.jobs = Queue()

	def add_job(self,job):
		self.jobs.put(job)

	def num_jobs(self):
		return int(self.jobs.qsize())

	def jobs_outstanding(self):
		return self.num_jobs() != 0

	def get_next_job(self):
		"""Return next Job + remove it from the queue"""
		if self.jobs_outstanding(): return self.jobs.get()
		raise Exception("No jobs in queue")

class Worker:

	def __init__(self, resource):
		self.resource_allocated = resource
		self.process = None
		self.curr_job = None

	def available_for_work(self):

		# Worker was never allocated a job
		if self.process is None:
			return True

		# A non-none returncode means the process has terminated
		returncode = self.process.poll()
		if returncode is not None:

			# If previous process terminated with non-zero exit code, report it and clear worker for work
			if returncode != 0:
				import datetime
				curr_datetime = datetime.datetime.now()
				formatted_datetime = curr_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")
				sys.stderr.write("{} - pid {} returned non-zero exit code '{}' from command '{}'\n".format(formatted_datetime,
						self.process.pid, returncode, self.curr_job.shell_command))
				self.process = None

			return True

		return False

	def clean_terminate(self):
		try:
			self.process.terminate()
		except:
			pass

	def start_job(self,job):
		"""Run command using resource + open unbuffered files and pipe stdout/error to them"""

		if not self.available_for_work(): raise Exception('Worker currently allocated')

		# If a previous job was processed, close it's file handles
		if self.curr_job is not None: self.curr_job.teardown()

		command = "{} {}".format(self.resource_allocated, job.shell_command)

		# If job standard in/out is specified to be sent to a file, do so - else default to sys stdout/stderr
		job.standard_out_f = open(job.standard_output, "a", 0) if job.standard_output != None else sys.stdout
		job.standard_error_f = open(job.standard_error, "a", 0) if job.standard_error != None else sys.stderr

		# We use subprocess but the pipeline still expects shell=True access for bash operators (>, >>, |, etc)
		# FIXME: Change to shell=False but only after the pipeline has been migrated
		self.process = subprocess.Popen(command, shell=True, stdout=job.standard_out_f, stderr=job.standard_error_f)

		# Terminate child gracefully if parent terminates gracefully
		atexit.register(self.clean_terminate)
		self.curr_job = job
		return self.process, command

class Factory:
	"""Factories have a queue of jobs and workers to work on them"""	

	def __init__(self, assigned_resources=[]):
		self.workers = []
		self.jobs = Job_Queue()

		for resource, number in assigned_resources:
			for _ in range(number):
				self.hire_worker(resource)

	def hire_worker(self,resource):
		"""Give factory additional resources"""
		self.workers.append(Worker(resource))

	def queue_work(self, shell_command, standard_out=None, standard_error=None):
		"""Start a job if a worker is available, else add it to the queue"""
		new_job = Job(shell_command, standard_out, standard_error)
		for worker in self.workers:
			if worker.available_for_work():
				return worker.start_job(new_job)
		self.jobs.add_job(new_job)

	def supervise(self):
		"""Assigns idling workers to outstanding jobs"""
		processes = []
		for worker in self.workers:
			if worker.available_for_work() and self.jobs.jobs_outstanding():
				process_spawned = worker.start_job(self.jobs.get_next_job())
				processes.append(process_spawned)
		return processes

	def completely_idle(self):
		"""True if no jobs remain and no worker is working"""
		if self.jobs.jobs_outstanding(): return False
		for worker in self.workers:
			if not worker.available_for_work(): return False
		return True
