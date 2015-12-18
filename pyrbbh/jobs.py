# Copyright 2015-2016, The James Hutton Insitute
# Author: Leighton Pritchard
#
# jobs.py
#
# This code is part of the pyrbbh package, and is governed by its licence.
# Please see the LICENSE file that should have been included as part of
# this package.

from .config import SGE_WAIT


# The Job class describes a single command-line job, with dependencies (jobs
# that must be run first.
class Job:
    """Objects in this class represent individual jobs to be run, with a list
    of dependencies (jobs that must be run first).
    """
    def __init__(self, name, command, queue=None):
        """Instantiates a Job object.

        - name           String describing the job (uniquely)
        - command        String, the valid shell command to run the job
        - queue          String, the SGE queue under which the job shall run

        >>> job = Job('myjob', 'ls -l')
        >>> djob = Job('required', 'cd .')
        >>> job.name
        'myjob'
        >>> job.command
        'ls -l'
        >>> job.dependencies
        []
        >>> job.add_dependency(djob)
        >>> job.dependencies #doctest: +ELLIPSIS
        [<jobs.Job instance at 0x...>]
        >>> job.remove_dependency(djob)
        >>> job.dependencies
        []
        >>> job.submitted
        False
        """
        self.name = name                 # Unique name for the job
        self.queue = queue               # The SGE queue to run the job under
        self.command = command           # Command line to run for this job
        self.script = command
        self.scriptPath = None           # Will hold path to the script file
        self.dependencies = []           # List of jobs that must be submitted
                                         # before this job may be submitted
        self.children = []               # List of jobs that depend on this job
        self.submitted = False           # Flag indicating whether the job has
                                         # already been submitted

    def add_dependency(self, job):
        """Add the passed job to the dependency list for this Job.  This
        Job should not execute until all dependent jobs are completed

        - job     Job to be added to the Job's dependency list
        """
        self.dependencies.append(job)
        job.children.append(self)

    def remove_dependency(self, job):
        """Remove the passed job from this Job's dependency list

        - job     Job to be removed from the Job's dependency list
        """
        job.children.remove(self)
        self.dependencies.remove(job)

    def wait(self, interval=SGE_WAIT):
        """Wait until the job finishes."""
        finished = False
        while not finished:
            time.sleep(interval)
            interval = min(2 * interval, 60)
            finished = os.system("qstat -j %s > /dev/null" % (self.name))
