import logging

logger = logging.getLogger()

from abc import ABC, abstractmethod


class GlueClientWrapper(ABC):
    @abstractmethod
    def list_jobs(self):
        pass

    @abstractmethod
    def batch_get_jobs(self, JobNames):
        pass


class GlueJobManager:
    def __init__(self, glue_client: GlueClientWrapper):
        self.glue_client = glue_client

    # TODO: Create unit tests that take into account the logic around which fields are needed when creating a glue job, This also impacts the importing logic
    def create_glue_job(self, job_creation_parameters: dict):
        # TODO: This is a horrible hack but we will clean this up as we add unit tests
        deployment_parameters = self._remove_non_deployment_fields([job_creation_parameters])[0]
        response = self.glue_client.create_job(**deployment_parameters)
        return response

    def delete_glue_job(self, job_name: str):
        response = self.glue_client.delete_job(JobName=job_name)
        return response

    def _get_all_job_names(self):
        get_client_jobs = self.glue_client.list_jobs(MaxResults=1000)
        list_of_jobs = get_client_jobs["JobNames"]
        return list_of_jobs

    def _chunks(self, list, chunk_size):
        for i in range(0, len(list), chunk_size):
            yield list[i:i + chunk_size]

    def _get_job_details(self, list_of_jobs):
        jobs_with_metadata = []
        for job_batch in self._chunks(list_of_jobs, 25):
            jobs_with_metadata += self.glue_client.batch_get_jobs(JobNames=job_batch)["Jobs"]
        return jobs_with_metadata

    def filter_job_names(self, job_names, query_string = None):
        if not query_string:
            return job_names
        filtered_jobs = [job_name for job_name in job_names if query_string.lower() in job_name.lower()]
        return filtered_jobs

    def _remove_non_deployment_fields_from_job(self, job_detail):
        remove_fields = ["CreatedOn","LastModifiedOn", "AllocatedCapacity", "MaxCapacity"]
        for field in remove_fields:
            job_detail.pop(field, None)
        return job_detail

    def _remove_non_deployment_fields(self, jobs_with_details):
        for job_detail in jobs_with_details:
            self._remove_non_deployment_fields_from_job(job_detail)
        return jobs_with_details

    def get_jobs(self, query_string = None):
        all_glue_job_names = self._get_all_job_names()
        if len(all_glue_job_names) == 0:
            raise Exception("No jobs found")

        filtered_jobs = self.filter_job_names(all_glue_job_names, query_string)
        if len(filtered_jobs) == 0:
            raise Exception(f"No Match found for the query string {query_string}")

        job_details = self._get_job_details(filtered_jobs)
        job_details_without_datetime = self._remove_non_deployment_fields(job_details)
        return job_details_without_datetime

