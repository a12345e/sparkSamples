import datetime
import requests
import os
from pathlib import Path

# --- Configuration ---
BASE_URL = "https://trialo0su9q.jfrog.io/artifactory"
AUTH = ("alone@kayhut.com", 'nM609396')
REPO_NAME = "alon-basic-repo"  # e.g., 'libs-release-local'
CUTOFF_DATE = "2026-04-22T00:00:00.000Z"  # ISO 8601 format
SIZE_LIMIT_BYTES = 1000010240  # 10KB
LOCAL_REPO_PATH = 'c:/users/a1234/.m2/repository'

token = 'cmVmdGtuOjAxOjE4MDg1MTA1NTA6UVNpaTlGaFc0V1VuSnlGQXdNVW9CSDRFSGhO'

REMOTE_CUTOFF_DATE = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)


def run_sync_process():
    # 1. Search JFrog for JAR files
    endpoint = f"{BASE_URL}/api/search/aql"
    # We only search for .jar to trigger the logic; we'll handle poms as companions
    query = (
        f'items.find({{'
        f'  "repo": "{REPO_NAME}",'
        f'  "name": {{"$match": "*.jar"}}'
        f' }})'
    )

    response = requests.post(endpoint, auth=AUTH, data=query, headers={'Content-Type': 'text/plain'})
    if response.status_code != 200:
        print(f"Failed to fetch from JFrog: {response.text}")
        return

    remote_artifacts = response.json().get('results', [])

    for item in remote_artifacts:
        name = item['name']
        r_path = item['path']
        r_size = item['size']
        # Convert remote ISO string to datetime object
        r_mtime = datetime.datetime.fromisoformat(item['modified'].replace('Z', '+00:00'))

        # Check Condition 1: Remote update time is BEFORE specific date
        if r_mtime > REMOTE_CUTOFF_DATE:

            # Look for this file locally to check Condition 2
            for root, dirs, files in os.walk(LOCAL_REPO_PATH):
                if name in files:
                    local_jar_path = os.path.join(root, name)
                    l_size = os.stat(local_jar_path).st_size

                    # Check Condition 2: Remote size is LESS than local size
                    if r_size < l_size:
                        print(f"\n[MATCH] {name} under {r_path} is smaller and older than local version.")

                        # Define the POM name (replaces .jar with .pom)
                        pom_name = name.replace(".jar", ".pom")
                        local_pom_path = os.path.join(root, pom_name)

                        # Replace JAR
                        upload_to_jfrog(local_jar_path, r_path, name)

                        # Replace POM (if it exists locally)
                        if os.path.exists(local_pom_path):
                            upload_to_jfrog(local_pom_path, r_path, pom_name)
                        else:
                            print(f"  [WARN] Local POM not found for {name}")
                    break


def upload_to_jfrog(local_path, artifactory_path, file_name):
    target_url = f"{BASE_URL}/{REPO_NAME}/{artifactory_path}/{file_name}"
    with open(local_path, 'rb') as f:
        put_res = requests.put(target_url, auth=AUTH, data=f)
        if put_res.status_code in [200, 201]:
            print(f"  [SUCCESS] Replaced: {file_name}")
        else:
            print(f"  [ERROR] Could not upload {file_name}: {put_res.status_code}")


if __name__ == "__main__":
    run_sync_process()