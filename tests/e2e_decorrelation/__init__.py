"""
End-to-end tests for subquery decorrelation engine.

These tests verify that the decorrelation engine correctly rewrites
correlated and uncorrelated subqueries into join-based logical plans
while preserving SQL semantics.
"""
