import requests
import os
import datetime
import json
import hashlib

# --- CONFIGURATION ---
BASE_URL = "https://trialo0su9q.jfrog.io/artifactory"
AUTH = ("alone@kayhut.com", 'nM609396')
REPO_NAME = "alon-basic-repo"  # e.g., 'libs-release-local'
CUTOFF_DATE = "2026-04-22T00:00:00.000Z"  # ISO 8601 format
SIZE_LIMIT_BYTES = 1000010240  # 10KB
LOCAL_BACKUP_PATH = 'c:/users/a1234/.m2/repository'



def get_sha1(file_path):
    """Generates SHA-1 hash for local file."""
    sha1 = hashlib.sha1()
    try:
        with open(file_path, 'rb') as f:
            while chunk := f.read(8192):
                sha1.update(chunk)
        return sha1.hexdigest()
    except:
        return None


def run_discrepancy_audit():
    # 1. Fetch Remote Metadata
    endpoint = f"{BASE_URL}/api/search/aql"
    # Note: We include 'actual_sha1' which Artifactory stores automatically
    query = f'items.find({{"repo": "{REPO_NAME}"}}).include("name", "path", "size", "modified", "actual_sha1")'

    response = requests.post(endpoint, auth=AUTH, data=query, headers={'Content-Type': 'text/plain'})
    if response.status_code != 200:
        print("Connection failed.");
        return

    remote_items = response.json().get('results', [])

    # Group by path
    remote_tree = {}
    for item in remote_items:
        path = item['path']
        if path not in remote_tree: remote_tree[path] = []
        remote_tree[path].append(item)

    diff_report = []

    # 2. Compare only shared directories
    for path, r_files in remote_tree.items():
        local_dir_path = os.path.join(LOCAL_BACKUP_PATH, path)

        if os.path.exists(local_dir_path):
            dir_artifacts = []
            has_difference = False

            for r_file in r_files:
                fname = r_file['name']
                local_path = os.path.join(local_dir_path, fname)

                if os.path.exists(local_path):
                    l_size = os.stat(local_path).st_size
                    l_sha1 = get_sha1(local_path)

                    # Remote values from AQL
                    r_size = r_file['size']
                    r_sha1 = r_file.get('actual_sha1')

                    # CHECK FOR DIFFERENCES
                    size_diff = (l_size != r_size)
                    sig_diff = (l_sha1 != r_sha1)

                    if size_diff or sig_diff:
                        has_difference = True
                        dir_artifacts.append({
                            "filename": fname,
                            "issue_detected": {
                                "size_mismatch": size_diff,
                                "signature_mismatch": sig_diff
                            },
                            "remote": {"size": r_size, "sha1": r_sha1},
                            "local": {"size": l_size, "sha1": l_sha1}
                        })

            # Only add the directory to report if a discrepancy was found
            if has_difference:
                diff_report.append({
                    "directory": path,
                    "mismatched_files": dir_artifacts
                })

    # 3. Output to JSON
    with open("artifact_discrepancy_report.json", "w") as f:
        json.dump(diff_report, f, indent=4)

    print(f"Audit complete. Found {len(diff_report)} directories with differences.")


if __name__ == "__main__":
    run_discrepancy_audit()