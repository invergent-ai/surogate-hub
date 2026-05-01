# System and Integration Testing

When a pull request is being committed / merged, we run several test suites to ensure nothing got broken due to the introduced changes.
Since these are system level tests, we created an infrastructure which deploys Surogate Hub on a VM and executes the tests using different clients on various storage adapters.

The following guide explains how to set up and execute the system tests infrastructure in order to optimize the development process, and enable debugging of the tests in a local environment.

## Running system tests

A couple of prerequisites before we start:

* Make sure you have a working Surogate Hub environment.
* [Docker](https://www.docker.com/)
* [Curl](https://curl.se/)

### Running Surogate Hub

*Before running the tests we need to run a Surogate Hub instance configured for local execution of system tests*

Under _**esti/scripts**_ you will find the following

1. **hub.yaml**
   1. Contains the Surogate Hub configuration required for local test execution
   2. We use local storage type to run the tests
   3. The data will be stored under the filesystem's _/tmp_ folder (can be configured to any folder with the required permissions)
2. **runner.sh**
   1. Script to execute Surogate Hub and tests
3. **set_env_vars.sh**
   1. Defines and sets all environment variables required for both tests and Surogate Hub execution
   2. Adding env vars or modifying existing variables should be done on this file
   3. Used as a reference for debug env as well (see next)
   

To run Surogate Hub for testing simply execute the following command in a shell console:
```shell
   esti/scripts/runner.sh -r sghub
```
The Surogate Hub server will run in the console and logs will be piped to both stdout and in a temporary file in the form of hub_<XXX>.log

### Executing tests

To run all the tests under the system tests folder, execute the following command in a new shell console:
```shell
   esti/scripts/runner.sh -r test
```
The tests will run, with the output piped to both stdout and in a temporary file in the form of hub_tests_<XXX>.log

The system test infrastructure allows additional flags and parameters to be passed for execution.
For example to run a specific test (by regex) we can use:
```shell
   esti/scripts/runner.sh -r test -test.run TestHooksSuccess
```
This command will execute all tests which match the given string regex (in this case it will run a single test by that name)  
For a full list of arguments run: 
```shell
   esti/scripts/runner.sh -r test --help
```

---

Notes:
1. By default, tests are performing Surogate Hub setup using predefined credentials. As consequence:
   1. Tests that validate creation of access key id and secret access key will not test this functionality
   2. This will require stopping and re-running the Surogate Hub server on each test execution
2. To run tests on an already existing Surogate Hub instance (after initial setup has occurred):
   1. Modify the ESTI_SETUP_SGHUB environment variable from 'true' to 'false'
   2. Use cautiously as some test preconditions will cause tests to fail on existing Surogate Hub environments

---

## Debugging Surogate Hub and the system tests using IntelliJ

To debug the tests and server code we must create run configurations for both Surogate Hub and the system tests.

### Surogate Hub run configuration

1. From the '**Run**' menu select '**Edit Configurations...**', then '**Add New Configuration**' (the plus sign) and create a new '**go build**' configuration  
    ![](img/go_build.png)
2. Copy the configuration from the following screenshot
   1. Add basic environment variables from the _set_env_vars.sh_ script's Surogate Hub section
   2. Add additional / modify environment variables as needed
   
3. Create a before launch, external tool to load the postgres docker image
   ![](img/postgres.png)
4. Press 'Apply', make sure the configuration is saved (it is not grayed out on the left side menu) and press 'OK'
5. Try to execute the target (either by run / debug) and verify Surogate Hub server is running

### System tests run configuration

1. From the '**Run**' menu select '**Edit Configurations...**', and then '**Add New Configuration**' (the plus sign) and create a new '**go test**' configuration

   ![](img/go_test.png)
2. Copy the configuration from the following screenshot
   1. Add basic environment variables from the _set_env_vars.sh_ script's System tests section
   2. Add additional / modify environment variables as needed
   3. To filter tests, you can provide a regular expression in the '**Pattern**' text box or alternatively provide the proper command line argument in the '**Prgram arguments**' text box
   
   ![](img/esti_config.png)
3. Press 'Apply', make sure the configuration is saved (it is not grayed out on the left side menu) and press 'OK'
4. Try to execute the target (either by run / debug) and verify tests are running

---
