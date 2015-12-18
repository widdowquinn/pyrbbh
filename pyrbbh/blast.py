# Copyright 2015-2016 The James Hutton Institute
# Author: Leighton Pritchard
#
# blast.py 
#
# This code is part of the pyrbbh package, and is governed by its licence.
# Please see the LICENSE file that should have been included as part of this
# package.

"""Module to produce BLAST command-line jobs for RBH analysis.
"""

import os
import time

from .config import BLASTP_DEFAULT, BLASTDB_DEFAULT

from . import jobs

# Make a dependency graph of BLAST database and query jobs
def make_blast_jobs(infiles, outdir,
                    blastp_exe=BLASTP_DEFAULT, blastdb_exe=BLASTDB_DEFAULT,
                    jobprefix="PYRBBH_%s" % str(int(time.time()))):
    """Returns a list of Job objects that represent BLAST commands required
    to conduct RBBH on the list of passed sequence files.

    The returned list is essentially a job dependency graph. Individual jobs
    record their upstream dependencies on other jobs. In all cases, this 
    should take the form of each query against a database constructed from
    one of the input files being dependent on the database construction job.

    The database construction jobs are initially stored in a dictionary, keyed
    by the input filestem. Query job dependencies are then determined by
    reference to this dictionary.

    - infiles - a list of paths to input FASTA files
    - outdir - path to directory for BLAST databases/output
    - blastp_exe - path to BLASTP executable
    - blastdb_exe - path to BLAST database formatting executable
    - jobprefix - a string to prefix job IDs if run on SGE scheduler
    """
    # Create dictionary of database jobs, keyed by filestem
    dbjobs = make_blastdb_jobs(infiles, outdir, blastdb_exe, jobprefix)
    # Create list of BLAST query jobs
    queryjobs = make_blastp_jobs(infiles, outdir, blastp_exe, jobprefix, dbjobs)
    return list(dbjobs.values()) + queryjobs
    

# Make a dictionary of makeblastdb jobs
def make_blastdb_jobs(infiles, outdir, blastdb_exe, jobprefix):
    """Returns a dictionary of BLAST database construction command-lines,
    keyed by the input filestem.

    - infiles - a list of paths to input FASTA files
    - outdir - path to directory for BLAST databases/output
    - blastdb_exe - path to BLAST database formatting executable
    - jobprefix - a string to prefix job IDs if run on SGE scheduler

    >>> sorted(make_blastdb_jobs(['../tests/seqdata/infile1.fasta', \
'../tests/seqdata/infile2.fasta'], '../tests/output/', 'makeblastdb', \
'RBH_BLAST').items()) #doctest: +ELLIPSIS
    [('infile1', <jobs.Job instance at 0x...>), ('infile2', \
<jobs.Job instance at 0x...>)]
    """
    # Create dictionary of database jobs
    dbjobdict = {}
    for idx, fname in enumerate(infiles):
        dbcmd, dbname = construct_makeblastdb_cmd(fname, outdir, blastdb_exe)
        job = jobs.Job("%s_db_%06d" % (jobprefix, idx), dbcmd)
        dbjobdict[dbname] = job
    return dbjobdict


# Build a makeblastdb command line
def construct_makeblastdb_cmd(infile, outdir, blastdb_exe):
    """Returns a tuple of (cmd_line, filestem) where cmd_line is the BLAST
    database formatting command for the passed filename, placing the result
    in outdir, with the same filestem as the input filename.

    The formatting assumes that the executable is makeblastdb from BLAST+

    - infile - input filename
    - outdir - location to write the database
    - blastdb_exe - path toBLAST database construction executable

    >>> construct_makeblastdb_cmd('../tests/seqdata/infile1.fasta', \
'../tests/output/', 'makeblastdb')
    ('makeblastdb -dbtype prot -in ../tests/seqdata/infile1.fasta -title \
infile1 -out ../tests/output/infile1.fasta', 'infile1')
    """
    filename = os.path.split(infile)[-1]  # strip directory
    filestem = os.path.splitext(filename)[0]  # strip extension
    outfname = os.path.join(outdir, filename)  # location to write db
    cmd = "{0} -dbtype prot -in {1} -title {2} -out {3}".format(blastdb_exe,
                                                                infile,
                                                                filestem,
                                                                outfname)
    return (cmd, filestem)


# Make list of BLAST query jobs
def make_blastp_jobs(infiles, outdir, blastp_exe, jobprefix, dbjobs):
    """Returns a list of BLASTP query jobs for RBH analysis.

    This requires nested loops of 

    - infiles - a list of paths to input FASTA files
    - outdir - path to directory for BLAST output
    - blastp_exe - path to BLASTP
    - jobprefix - a string to prefix job IDs if run on SGE scheduler
    - dbjobs - dictionary of database construction jobs, keyed by filestem

    >>> joblist = make_blastp_jobs(['../tests/seqdata/infile1.fasta', \
'../tests/seqdata/infile2.fasta', '../tests/seqdata/infile3.fasta'], \
'../tests/output/', 'makeblastdb', 'RBH_BLAST', {'infile1': 'dbjob1', \
'infile2': 'dbjob2', 'infile3': 'dbjob3'})
    >>> [j.name for j in joblist]
    ['RBH_BLAST_query_000001_fwd', 'RBH_BLAST_query_000001_rev', \
'RBH_BLAST_query_000002_fwd', 'RBH_BLAST_query_000002_rev', \
'RBH_BLAST_query_000003_fwd', 'RBH_BLAST_query_000003_rev']
    >>> joblist[0].dependencies
    ['dbjob2']
    """
    # Create list of BLASTP jobs
    joblist = []
    jobnum = 0
    for idx, infile1 in enumerate(infiles):
        fname1 = os.path.split(infile1)[-1]  # strip directory
        fstem1 = os.path.splitext(fname1)[0]  # strip extension
        dbname1 = os.path.join(outdir, fstem1)
        for infile2 in infiles[idx+1:]:
            jobnum += 1
            fname2 = os.path.split(infile2)[-1]  # strip directory
            fstem2 = os.path.splitext(fname2)[0]  # strip extension
            dbname2 = os.path.join(outdir, fstem2)
            cmd1 = construct_blastp_cmd(infile1, dbname2, outdir, blastp_exe)
            cmd2 = construct_blastp_cmd(infile2, dbname1, outdir, blastp_exe)
            job1 = jobs.Job("%s_query_%06d_fwd" % (jobprefix, jobnum), cmd1)
            job2 = jobs.Job("%s_query_%06d_rev" % (jobprefix, jobnum), cmd2)
            job1.add_dependency(dbjobs[fstem2]) # add dependency on db job
            job2.add_dependency(dbjobs[fstem1])
            joblist.extend([job1, job2])
    return joblist


# Make a BLASTP query command line
def construct_blastp_cmd(qfile, dbname, outdir, blastp_exe):
    """Returns a single BLASTP command, using the input qfile against the
    database dbname, writing results to outdir, using the executable in
    blastp_exe.

    Output filename is formatted 'qstem_vs_dbstem.out'

    >>> construct_blastp_cmd('../tests/seqdata/infile1.fasta', \
'../tests/output/infile2', '../tests/output', 'blastp')
    'blastp -out ../tests/output/infile1_vs_infile2.xml -query \
../tests/seqdata/infile1.fasta -db ../tests/output/infile2 -outfmt 5'
    """
    qstem = os.path.splitext(os.path.split(qfile)[-1])[0]
    dbstem = os.path.splitext(os.path.split(dbname)[-1])[0]
    prefix = os.path.join(outdir, '%s_vs_%s' % (qstem, dbstem))
    cmd = "{0} -out {1}.xml -query {2} -db {3} -outfmt 5"
    return cmd.format(blastp_exe, prefix, qfile, dbname)
