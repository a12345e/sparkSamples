import requests
import os
import hashlib
import json

# --- CONFIGURATION ---
BASE_URL = "https://trialo0su9q.jfrog.io/artifactory"
AUTH = ("alone@kayhut.com", 'nM609396')
REPO_NAME = "alon-basic-repo"
LOCAL_REPO_PATH = "C:/users/a1234/.m2/repository"

# Only process remote artifacts updated after this date
DATE_LIMIT = "2026-04-22T00:00:00.000Z"


def compute_sha1(file_path):
    sha1 = hashlib.sha1()
    with open(file_path, 'rb') as f:
        while chunk := f.read(8192):
            sha1.update(chunk)
    return sha1.hexdigest()


def deploy_to_jfrog(content, remote_path, file_name, is_file=True):
    """Handles both binary file uploads and raw string (checksum) uploads."""
    target_url = f"{BASE_URL}/{REPO_NAME}/{remote_path}/{file_name}"

    if is_file:
        with open(content, 'rb') as f:
            response = requests.put(target_url, auth=AUTH, data=f)
    else:
        # content is the raw hash string
        response = requests.put(target_url, auth=AUTH, data=content)

    if response.status_code in [200, 201]:
        print(f"  [SUCCESS] Deployed: {file_name}")
        return True
    else:
        print(f"  [FAILED] {file_name} ({response.status_code}): {response.text}")
        return False


def run_final_sync():
    # 1. AQL Query: Filtered by Date and JAR extension
    endpoint = f"{BASE_URL}/api/search/aql"
    query = (
        f'items.find({{'
        f'  "repo": "{REPO_NAME}",'
        f'  "modified": {{"$gt": "{DATE_LIMIT}"}},'
        f'  "name": {{"$match": "*.jar"}}'
        f' }})'
    )

    response = requests.post(endpoint, auth=AUTH, data=query, headers={'Content-Type': 'text/plain'})
    if response.status_code != 200:
        print("Error: Could not retrieve data from Artifactory.");
        return

    remote_jars = response.json().get('results', [])
    print(f"Found {len(remote_jars)} remote JARs modified after {DATE_LIMIT}.")

    for r_jar in remote_jars:
        name = r_jar['name']
        path = r_jar['path']
        r_size = r_jar['size']

        # 2. Check Local File
        local_jar_path = os.path.join(LOCAL_REPO_PATH, path, name)

        if os.path.exists(local_jar_path):
            l_size = os.stat(local_jar_path).st_size

            # CONDITION: Local is SMALLER than Remote
            if l_size <= r_size:
                print(f"\n[SYNC TRIGGERED] {name}")
                print(f"  Remote Size: {r_size} | Local Size: {l_size}")

                # Prepare POM path
                pom_name = name.replace(".jar", ".pom")
                local_pom_path = os.path.join(LOCAL_REPO_PATH, path, pom_name)

                # --- STEP 1: Upload JAR & POM ---
                deploy_to_jfrog(local_jar_path, path, name)
                if os.path.exists(local_pom_path):
                    deploy_to_jfrog(local_pom_path, path, pom_name)

                # # --- STEP 2: Compute & Upload SHA-1 Signatures ---
                # # JAR SHA-1
                # jar_hash = compute_sha1(local_jar_path)
                # deploy_to_jfrog(jar_hash, path, name + ".sha1", is_file=False)
                #
                # # POM SHA-1
                # if os.path.exists(local_pom_path):
                #     pom_hash = compute_sha1(local_pom_path)
                #     deploy_to_jfrog(pom_hash, path, pom_name + ".sha1", is_file=False)
            else:
                print(f"  [SKIPPING] {name}: Local size ({l_size}) is not smaller than remote ({r_size}).")
        else:
            print(f"  [SKIPPING] {name}: Not found in local backup.")


if __name__ == "__main__":
    run_final_sync()