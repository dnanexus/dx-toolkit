@dx
Feature: 1: dx commands example

  Scenario: 1010_CLI new project test
    Given CLI login as user qa_admin_org1
    When CLI create new project
      | name      | --bill-to     | expect_error |
      | [default] | org-test_org1 | no           |
    Then login as user qa_admin_org1
    And add qa_tester_orgs123 user as ADMINISTER to project


  Scenario: 1020_CLI new project test with error
    Given CLI login as user qa_admin_org1
    When CLI create new project
      | name      | --bill-to | expect_error                                                             |
      | [default] | zzz       | InvalidInput: Expected key "billTo" to be a user id or org id, code 422. |


  Scenario: 1030_CLI file upload test, project select on login
    Given CLI login as user qa_admin_org1
    When CLI create new project
    # TODO: remove expect error (by default no)
      | name      | --select | expect_error |
      | [default] |          | no           |
    And generate file big 2 Bytes named zzz.file
    And CLI upload file [context]
    Then login as user qa_admin_org1
    And add qa_tester_orgs123 user as ADMINISTER to project


  Scenario: 1040_CLI file upload test passing the project
    Given CLI login as user qa_admin_org1
    When CLI create new project
      | name      | expect_error |
      | [default] | no           |
    And generate file big 2 Bytes named zzz.file
    And CLI upload file [context]
      | --path                  |
      | {[context].project_id}: |
    Then login as user qa_admin_org1
    And add qa_tester_orgs123 user as ADMINISTER to project


  # An example of replacing several values in the parameter string
  Scenario: 1060_CLI file upload test passing the project
    Given CLI login as user qa_admin_org1
    When CLI create new project
      | name      | expect_error |
      | [default] | no           |
    And generate file big 2 Bytes named zzz.file
    And CLI upload file [context]
      | --path                  |
      | {[context].project_id}:{[context].project_name} |
    Then login as user qa_admin_org1
    And add qa_tester_orgs123 user as ADMINISTER to project
