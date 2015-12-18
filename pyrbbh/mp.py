# Copyright 2015-2016, The James Hutton Insitute
# Author: Leighton Pritchard
#
# mp.py
#
# This code is part of the pyrbbh package, and is governed by its licence.
# Please see the LICENSE file that should have been included as part of
# this package.

"""Code to aid the parallelisation of tasks with multiprocessing."""

import multiprocessing
import subprocess
import sys

CUMRETVAL = 0

# Create sets of jobs at distinct levels of a dependency tree
def create_cmdsets(jobgraph, logger=None):
    """Returns sets of commands at distinct levels of a job dependency tree.

    - jobgraph - dependency graph of Job objects as list of Jobs
    - logger - logger object

    Given a dependency tree, returns distinct sets of commands that it is safe
    to submit to multiprocessing in a series of asynchronous pools. For
    example:

    j0 <- j1
    j0 <- j2 <- j5
    j3 <- j4

    should return two sets of jobs: (j0, j3), (j1, j2, j4), (j5), such that
    the first set returned can be executed, and the dependencies of the
    second set will be satisfied and, when the second set is executed, the 
    dependencies of the third set will be satisfied, and so on.
    """
    if logger:
        logger.info("Subdividing job dependency tree by depth")
    jobsets = []
    for job in [j for j in jobgraph if len(j.dependencies) == 0]:
        jobsets = __populate_jobsets(job, jobsets, depth=0)
    return jobsets


# Recursive function to split dependency tree into levels
def __populate_jobsets(job, jobsets, depth):
    """Populates jobsets in create_jobsets().

    - job - a Job object
    - jobsets - the current set of Job objects
    - depth - current depth in dependency graph

    A recursive function creating lists of sets containing items at distinct
    depths of a dependency tree.
    """
    if len(jobsets) < depth+1:
        jobsets.append(set())
    jobsets[depth].add(job.command)
    if len(job.children) == 0:
        return jobsets
    for j in job.children:
        jobsets = __populate_jobsets(j, jobsets, depth+1)
    return jobsets


# Run a set of jobs with multiprocessing, returning sum of error values
def mp_run_cmdset(cmdset, logger=None):
    """Distribute the set of command-lines in cmdset with multiprocessing,
    returning sum of error values.

    - jobset - set of command-lines to be run in an asynchronous pool
    - logger - logger object
    
    Runs the passed set of commands in an asynchronous pool. Collects function 
    exit values with a callback. Returns the sum of collected function
    exit values.
    """
    global CUMRETVAL  # tracks return value sum

    # Run jobs
    pool = multiprocessing.Pool()
    completed = []
    pool_outputs = [pool.apply_async(subprocess.call,
                                     (str(cline), ),
                                     {'stderr': subprocess.PIPE,
                                      'shell': sys.platform != "win32"},
                                     callback = __status_callback)
                    for cline in cmdset]
    pool.close()
    pool.join()
    return CUMRETVAL


# Callback function for multiprocessing runs
def __status_callback(val):
    """Basic callback to collect exit code

    - val - exit code from command
    """
    global CUMRETVAL
    CUMRETVAL += val
