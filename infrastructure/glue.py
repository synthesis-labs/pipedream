def deploy_glue_jobs(client, json_object):
    for job in json_object["nodes"]:
        if job["id"] not in ["start", "stop"]:
            job_parameters = job["metadata"]["properties"]
            client.create_job(**job_parameters)
            print(f"{job['id']} Created")

