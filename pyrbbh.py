#!/usr/bin/env python

import argparse
import logging
import os
import sys
import time

from pyrbbh import blast, config, io

class PyRBBH(object):
    """pyrbbh module script"""
    def __init__(self):
        """Basic commandline."""
        usage = """pyrbbh.py <command> [<args>]

The most commonly used commands are:
    rbbh        Reciprocal best BLASTP
    rbvh        Reciprocal best VSEARCH
    mclb        MCL clustering from best BLASTP
    mclv        MCL clustering from best VSEARCH
"""
        # set up parser for common arguments
        parser = argparse.ArgumentParser(prog="pyrbbh.py",
                                         description="aa sequence clustering",
                                         usage=usage)
        parser.add_argument('command', help='Subcommand to run')
        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.command):
            print("Unrecognised command")
            parser.print_help()
            sys.exit(1)
        # invoke method with command name
        getattr(self, args.command)()

    def __build_common_parser(self, description="Common Parser"):
        """Constructs argument parser with common arguments."""
        self._parser = argparse.ArgumentParser(description=description)
        self._parser.add_argument('indirname',
                                  action='store', default=None,
                                  help='Path to input files')
        self._parser.add_argument('outdirname',
                                  action='store', default=None,
                                  help='Path to output files')
        self._parser.add_argument('-v', '--verbose', dest='verbose',
                                  action='store_true', default=False,
                                  help='Give verbose output')
        self._parser.add_argument('-l', '--logfile', dest='logfile',
                                  action='store', default=None,
                                  help='Path to logfile')
        self._parser.add_argument('-f', '--force', dest='force',
                                  action='store_true', default=False,
                                  help='Force output overwrite')
        self._parser.add_argument('-s', '--scheduler', dest='scheduler',
                                  action='store', default='mp',
                                  type=str, choices=['mp', 'SGE'],
                                  help='Scheduler')


    def rbbh(self):
        "Conduct reciprocal best BLAST hit analysis"
        # Parse arguments
        self.__build_common_parser(description="Reciprocal best BLASTP")
        self._parser.add_argument('-p', '--pid', dest='identity',
                                  action='store', default=0.8,
                                  help='Percentage identity threshold')
        self._parser.add_argument('-c', '--cov', dest='coverage',
                                  action='store', default=0.8,
                                  help='Percentage coverage threshold')
        self._parser.add_argument('--blastp_exe', dest='blastp_exe',
                                  action='store',
                                  default=config.BLASTP_DEFAULT,
                                  help='Path to BLASTP executable')
        self._parser.add_argument('--blastdb_exe', dest='blastdb_exe',
                                  action='store',
                                  default=config.BLASTDB_DEFAULT,
                                  help='Path to makeblastdb executable')
        self._parser.add_argument('--jobprefix', dest='jobprefix',
                                  action='store',
                                  default="PyRBBH_%s" % str(int(time.time())),
                                  help='Prefix for jobs in this run')
        self._args = self._parser.parse_args(sys.argv[2:])
        
        # Set up logger
        self.__start_logger()

        # Validate input/output locations
        self.__validate_paths()

        # Process input files
        self.__get_input_files()

        # Get BLAST jobs
        self.__make_rbbh_jobs()


    def __get_input_files(self):
        """Get list of input FASTA files."""
        self._logger.info("Processing %s for input FASTA files" %
                          self._args.indirname)
        self._infiles = io.get_fasta_files(self._args.indirname)
        self._logger.info("Found %d FASTA files" % len(self._infiles))


    def __make_rbbh_jobs(self):
        """Make dependency graph of RBBH BLAST jobs."""
        self._logger.info("Creating BLAST jobs for RBBH")
        self._jobs = blast.make_blast_jobs(self._infiles,
                                           self._args.outdirname,
                                           self._args.blastp_exe,
                                           self._args.blastdb_exe,
                                           self._args.jobprefix)
        self._logger.info("Created %d jobs" % len(self._jobs))


    def __validate_paths(self):
        """Exits if the input/output paths have problems. Creates output
        directory if doesn't already exist.
        """
        # Input directory path invalid
        if not os.path.isdir(self._args.indirname):
            self._logger.error("%s is not a valid directory path (exiting)" % 
                               self._args.indirname)
            sys.exit(1)
        # Output directory already exists
        if os.path.isdir(self._args.outdirname):
            self._logger.warning("Output directory %s already exists" % 
                                 self._args.outdirname)
            if not self._args.force:
                self._logger.error("Will not overwrite output in %s (exiting)" %
                                   self._args.outdirname)
                sys.exit(1)
            else:
                self._logger.warning("Will overwrite output in %s" % 
                                     self._args.outdirname)
        else:
            try:  # Make the directory recursively
                self._logger.info("Creating new directory %s" %
                                  self._args.outdirname)
                os.makedirs(self._args.outdirname)
                self._logger.info("New directory %s created" %
                                  self._args.outdirname)                
            except OSError:
                self._logger.error(last_exception())
                sys.exit(1)
        


    def __start_logger(self):
        """Start up logger."""
        # Instantiate logger and formatting
        self._t0 = time.time()
        self._logger = logging.getLogger('pyrbbh.py: %s' % time.asctime())
        self._logger.setLevel(logging.DEBUG)
        err_handler = logging.StreamHandler(sys.stderr)
        err_formatter = logging.Formatter('%(levelname)s: %(message)s')
        err_handler.setFormatter(err_formatter)

        # Use logfile if it exists
        if self._args.logfile is not None:
            try:
                logstream = open(self._args.logfile, 'w')
                err_handler_file = logging.StreamHandler(logstream)
                err_handler_file = setFormatter(err_formatter)
                err_handler_file.setLevel(logging.INFO)
                self._logger.addHandler(err_handler_file)
            except:
                self._logger.error('Could not open %s for logging (exiting)' % 
                                   self._args.logfile)
                sys.exit(1)

        # Use verbose reporting, if requested
        if self._args.verbose:
            err_handler.setLevel(logging.INFO)
        else:
            err_handler.setLevel(logging.WARNING)
        self._logger.addHandler(err_handler)

        # Log command-line and arguments, if verbose
        self._logger.info("command-line: %s" % ' '.join(sys.argv))
        self._logger.info(self._args)


if __name__ == "__main__":
    PyRBBH()
