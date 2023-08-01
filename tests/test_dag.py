import dag_manager
import unittest
class WhenCallingCreateGraphFromGlueJobsGivenAValidListOfJobs(unittest.TestCase):
    def setUp(self):
        glue_jobs = [
            {
                "Name": "Flights Conversion",
                "Role": "arn:aws:iam::296274010522:role/service-role/AWSGlueServiceRole-DefaultRole1",
                "ExecutionProperty": {"MaxConcurrentRuns": 1},
                "Command": {
                    "Name": "glueetl",
                    "ScriptLocation": "s3://aws-glue-scripts-296274010522-eu-west-1/admin/tutorial-script",
                    "PythonVersion": "3",
                },
                "MaxRetries": 0,
                "AllocatedCapacity": 10,
                "Timeout": 2880,
                "MaxCapacity": 10.0,
                "WorkerType": "G.1X",
                "NumberOfWorkers": 10,
                "GlueVersion": "2.0",
            },
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
            },
        ]
        self.result = dag_manager.create_graph_from_glue_jobs(glue_jobs)

    def test_that_the_resulting_graph_starts_and_ends_with_the_correct_nodes(self):
        assert "start" and "stop" in list(self.result.nodes)

    def test_that_the_resulting_graph_contains_the_correct_nodes(self): 
        assert list(self.result.nodes) == [
            "start",
            "Flights Conversion",
            "cloudtrail_bp_test_post_etl_f3e9473f",
            "stop",
        ]
