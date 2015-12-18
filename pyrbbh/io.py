# Copyright 2015-2016 The James Hutton Institute
# Author: Leighton Pritchard
#
# io.py 
#
# This code is part of the pyrbbh package, and is governed by its licence.
# Please see the LICENSE file that should have been included as part of this
# package.

"""Module to process input/output operations for RBH analysis."""

import os

from Bio import SeqIO


FASTA_EXTS = ('.fasta', '.faa', '.fas', 'fa')

# Returns a list of FASTA files in a directory
def get_fasta_files(dirname):
    """Returns list of paths to FASTA files in passed directory.

    - dirname - path to directory
    """
    return get_files_by_ext(dirname, *FASTA_EXTS)


# Returns a list of files in a directory, filtered by extension
def get_files_by_ext(dirname, *exts):
    """Returns list of paths to files with extensions in *exts.

    - dirname - path to directory
    - *exts - file extensions to filter on
    """
    fnames = [f for f in os.listdir(dirname) if os.path.splitext(f)[-1] in exts]
    return [os.path.join(dirname, f) for f in fnames]
