import d6tflow
import logging

import sys
log = logging.getLogger()
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

class TaskTest(d6tflow.tasks.TaskCache):

    def run(self):
        log.info("Hello!")
        self.save({"output": 0})


d6tflow.run(TaskTest())