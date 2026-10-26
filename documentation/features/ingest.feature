Feature: Validate that job can run
    Scenario:
        Given that the databricks job exists
        When the job is triggered
        Then the job should complete successfully