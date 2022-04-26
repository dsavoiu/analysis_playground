# coding: utf-8

"""
Testing tasks
"""

__all__ = []

import os
import subprocess

import luigi
import law

from functools import reduce
from law.contrib.tasks import ForestMerge

from ap.tasks.framework import AnalysisTask


class MakeLetters(AnalysisTask, law.LocalWorkflow):
    """Example workflow: creates 26 JSON files, each containing one letter of the
    alphabet. We will use this workflow as an input to the merging step, which
    will contain the full alphabet string."""

    ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

    def create_branch_map(self):
        """One branch per letter of the alphabet"""
        return {
            i: dict(letter=letter)
            for i, letter in enumerate(self.ALPHABET)
        }

    def output(self):
        """One JSON file per letter."""
        return self.local_target(f"letter_{self.branch}.json")

    def run(self):
        """Retrieve letter from branch data and dump to JSON file."""
        self.output().dump([self.branch_data["letter"]], formatter="json")


class MergeLetters(ForestMerge, AnalysisTask):
    """Example of a ForestMerge workflow to merge inputs in a highly parallel way.

    ForestMerge parallelizes the merging process using a tree structure. The leaf
    tasks consisting of the branches of the input workflow are first merged in
    small groups. These intermediate results are then merged again, continuing until
    the inputs are fully merged.
    """

    merge_factor = 2  # how many leaf tasks/intermediary merge results to merge at once

    def merge_workflow_requires(self):
        """Input workflow. Branches of this should be merged in some way."""
        return MakeLetters.req(self)

    def merge_requires(self, start_leaf, end_leaf):
        """Input tasks that should be merged for a given leaf range. In this case,
        leaves are branches of the dependent workflow."""

        # note: requirements should be *exclusive* in `end_leaf`
        return [MakeLetters.req(self, branch=i) for i in range(start_leaf, end_leaf)]

    def merge_output(self):
        """Fully merged output."""
        return self.local_target(f"merged.json")

    def merge(self, inputs, output):
        """Actual merging algorithm. Here: trivial adding of lists."""
        merged = reduce(list.__add__, (i.load() for i in inputs))
        output.dump(merged, formatter="json")
