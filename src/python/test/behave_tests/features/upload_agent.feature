@upload_agent
Feature: 2: Upload agent example

  Scenario: 2000_upload file with upload agent
    Given CLI login as user qa_admin_org1
    And CLI create new project
      | name      | --select |
      | [default] |          |
    And generate file big 2 Bytes named zzz.file
    When upload file [context] with Upload Agent
    And add qa_tester_orgs123 user as ADMINISTER to project
