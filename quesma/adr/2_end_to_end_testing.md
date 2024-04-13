# Use replayed HTTP traffic for end-to-end testing

## Context and Problem Statement

We need to ensure that the Quesma gateway works correctly in production.

Quesma transforms data intelligently and is prone to bugs.

Quesma contracts are defined by existing implementations of database protocols, which have many edge cases.

Traditional end-to-end may involve many components which Quesma does not own.

Our end-to-end cases should:
* based on actual interactions with the database (e.g. Kibana, Wazuh, ElastiAlert)
* stable flakiness should be rarer than finding real issues
* easy to maintain. We don't want to spend time updating versions and debugging unrelated issues to Quesma
* fast, efficiently run them at least daily under an hour
* reproducible: all engineers should be able to re-run failing tests locally
* extensible, we should be able to add new tests easily

## Considered Options

1. Hire a team of manual QA testers.
2. Use Selenium/Cypress with end-to-end tools such as Kibana.
3. Use some AI tool that will fix this issue for us.
4. Use replayed HTTP traffic for end-to-end testing.

## Decision Outcome and Drivers

QA Team was rejected due to a slow feedback loop and high maintenance cost.

Selenium/Cypress required a huge upfront investment and maintenance effort. Moreover, it may catch issues with UI interactions rather than DB interactions.

We were unable to find an AI tool that worked as we imagined.

We concluded the best option is to construct test cases based on actual interactions with the database:
1. Generate a fixed dataset.
2. Record HTTP queries using mitmproxy while playing with the tool using the DB interface.
3. Replay those queries against Quesma.
4. Compare the result and be able to write rules on how to compare the result.

We concluded this could fill our requirements.

We predict recording tests would be infrequent:
* data generator or fixed dataset
* separate docker-compose with no Quesma
* session with an external tool and tweaking mitmproxy dataset
  We could repeat the process for each supported tool or significant functionality.

However, replaying tests would be a regular part of our continuous integration pipeline.

## People
- @jakozaur
- @mieciu
