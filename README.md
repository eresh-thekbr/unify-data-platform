# Unify Data Platform

A Databricks project with a separate directory for each use case. Each use case
contains its own scripts, notebooks, tests, configuration template, and README.

## Use cases

- [SPY market bars pipeline](spy_bars/README.md): replay one-minute SPY bars from
  MongoDB through Kafka into bronze, silver, and quarantine tables.

Add future use cases as sibling directories named for their purpose.
