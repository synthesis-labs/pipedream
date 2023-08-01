import pytest
from unittest.mock import Mock
import resource.glue
import unittest


class WhenCallingFilterJobNamesGivenAListOfJobNamesThenTest(unittest.TestCase):
    def setUp(self):
        glue_jobs = resource.glue.GlueJobManager(None)
        job_names = ["Flights Conversion", "cloudtrail_bp_test_post_etl_f3e9473f", "TestCall"]
        self.results = glue_jobs.filter_job_names(job_names, "Test")

    def test_that_match_the_correct_values_are_returned(self):
        assert self.results == ["cloudtrail_bp_test_post_etl_f3e9473f", "TestCall"]

    def test_that_the_correct_number_of_match_is_returned(self):
        assert len(self.results) == 2


class WhenCallingGetJobsGivenInvalidQueryStringAndJobNamesAreReturnedThenTest(unittest.TestCase):
    def setUp(self):
        mock_glue_client = Mock(spec=resource.glue.GlueClientWrapper)
        mock_glue_client.list_jobs.return_value = {"JobNames": [], "NextToken": "string"}

        self.glue_jobs = resource.glue.GlueJobManager(mock_glue_client)

    def test_no_job_found_exception(self):
        query_string = "does not matter"
        with pytest.raises(Exception) as exc_info:
            self.glue_jobs.get_jobs(query_string)

        assert exc_info.type is Exception
        assert exc_info.value.args[0] == "No jobs found"


class WhenCallingGetJobsGivenNoJobNamesAreReturnedThenTest(unittest.TestCase):
    def setUp(self):
        mock_glue_client = Mock(spec=resource.glue.GlueClientWrapper)
        mock_glue_client.list_jobs.return_value = {
            "JobNames": ["Flights Conversion", "cloudtrail_bp_test_post_etl_f3e9473f"],
            "NextToken": "string",
        }

        self.glue_jobs = resource.glue.GlueJobManager(mock_glue_client)

    def test_no_match_found_for_job_query_string(self):
        query_string = "abcdef"
        with pytest.raises(Exception) as exc_info:
            self.glue_jobs.get_jobs(query_string)

        assert exc_info.type is Exception
        assert exc_info.value.args[0] == f"No Match found for the query string {query_string}"


class WhenCallingGetJobsGivenValidQueryStringAndJobNameIsReturnedThenTest(unittest.TestCase):
    def setUp(self):
        self.mock_glue_client = Mock(spec=resource.glue.GlueClientWrapper)
        self.mock_glue_client.list_jobs.return_value = {
            "JobNames": ["Flights Conversion", "cloudtrail_bp_test_post_etl_f3e9473f"],
            "NextToken": "string",
        }
        self.mock_glue_client.batch_get_jobs.return_value = {
            "Jobs": [
                {
                    "Name": "cloudtrail_bp_test_post_etl_f3e9473f",
                    "Description": "A job to change state to COMPLETED",
                    "Role": "arn:aws:iam::296274010522:role/service-role/AWSGlueServiceRole-test",
                    "ExecutionProperty": {"MaxConcurrentRuns": 1},
                    "Command": {
                        "Name": "pythonshell",
                        "ScriptLocation": "s3://aws-glue-assets-eu-west-1/scripts/pythonshell/state_manager.py",
                        "PythonVersion": "2",
                    },
                    "MaxRetries": 0,
                    "AllocatedCapacity": 0,
                    "Timeout": 2880,
                    "MaxCapacity": 0.0625,
                    "GlueVersion": "1.0",
                    "CreatedOn": "2022-06-02 16:36:44.887000+02:00",
                    "LastModifiedOn": "2022-06-02 17:16:54.650000+02:00",
                }
            ]
        }

        glue_jobs = resource.glue.GlueJobManager(self.mock_glue_client)
        self.result = glue_jobs.get_jobs("cloud")

    def test_list_job_is_called(self):
        self.mock_glue_client.list_jobs.assert_called_once()

    def test_batch_get_jobs_is_called_with_the_correct_names(self):
        self.mock_glue_client.batch_get_jobs.assert_called_once_with(JobNames=["cloudtrail_bp_test_post_etl_f3e9473f"])

    def test_job_details_should_contain_parameters_for_creating_glue_jobs(self):
        expected_result = [
            {
                "Name": "cloudtrail_bp_test_post_etl_f3e9473f",
                "Description": "A job to change state to COMPLETED",
                "Role": "arn:aws:iam::296274010522:role/service-role/AWSGlueServiceRole-test",
                "ExecutionProperty": {"MaxConcurrentRuns": 1},
                "Command": {
                    "Name": "pythonshell",
                    "ScriptLocation": "s3://aws-glue-assets-eu-west-1/scripts/pythonshell/state_manager.py",
                    "PythonVersion": "2",
                },
                "MaxRetries": 0,
                "AllocatedCapacity": 0,
                "Timeout": 2880,
                "MaxCapacity": 0.0625,
                "GlueVersion": "1.0",
            }
        ]
        assert self.result == expected_result

class WhenCallingGetJobsGivenValidQueryStringAndMoreThan25JobNamesAreReturnedThenTest(unittest.TestCase):
    def setUp(self):
        self.mock_glue_client = Mock(spec=resource.glue.GlueClientWrapper)
        self.mock_glue_client.list_jobs.return_value = {
            "JobNames": [f"a{x}" for x in range(50)],
            "NextToken": "string",
        }
        self.mock_glue_client.batch_get_jobs.return_value = {
            "Jobs": [{ "Name": f"a{x}"} for x in range(25)]
        }

        glue_jobs = resource.glue.GlueJobManager(self.mock_glue_client)
        self.result = glue_jobs.get_jobs()
        print(self.mock_glue_client.batch_get_jobs)

    def test_list_job_is_called(self):
        self.mock_glue_client.list_jobs.assert_called()

    def test_batch_get_jobs_is_called_once_for_each_batch(self):
        assert self.mock_glue_client.batch_get_jobs.call_count == 2

    def test_result_should_contain_the_correct_jobs(self):
        expected_result = [{ "Name": f"a{x}"} for x in range(25)] + [{ "Name": f"a{x}"} for x in range(25)]
        assert self.result == expected_result