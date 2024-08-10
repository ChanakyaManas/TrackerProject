import subprocess
import json
import os
from datetime import datetime
from typing import List, Dict
from dateutil import parser, tz
from typing import Tuple
import requests
import re

# Configuration
GOOGLE_SHEET_URL_FETCH = "https://script.google.com/macros/s/AKfycbyNII9AtRei3JTHDpQAQp2O8kfc1Ql2E7TtoKWIZCEHFafXTrDw4LvMMprI-bbyKpLttg/exec"
GOOGLE_SHEET_URL_UPDATE = "https://script.google.com/macros/s/AKfycbyNII9AtRei3JTHDpQAQp2O8kfc1Ql2E7TtoKWIZCEHFafXTrDw4LvMMprI-bbyKpLttg/exec"
NAMES_FILE = "6.0_Names.txt"
INDIA_TZ = tz.gettz("Asia/Kolkata")

def check_gh_command() -> bool:
    """Check if GitHub CLI is available."""
    try:
        result = subprocess.run(['gh', '--version'], capture_output=True, text=True, check=True)
        print(f"'gh' command is available:\n{result.stdout}")
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        print("Error: 'gh' command is not recognized. Ensure GitHub CLI is installed and added to PATH.")
        return False

def sanitize_repo_name(repo_url: str) -> str:
    """Sanitize repository name to avoid invalid characters for directories."""
    repo_name = repo_url.rstrip('/').split('/')[-1]
    return re.sub(r'[<>:"/\\|?*\x00-\x1F]', '_', repo_name)

def clone_repo(repo_url: str):
    """Clone the repository if it does not exist."""
    repo_dir = sanitize_repo_name(repo_url)
    if not os.path.exists(repo_dir):
        try:
            subprocess.run(['git', 'clone', repo_url, repo_dir], check=True)
            print(f"Cloned repository {repo_url} into {repo_dir}.")
        except subprocess.CalledProcessError as e:
            print(f"Error cloning repository {repo_url}: {e}")
    else:
        print(f"Repository {repo_dir} already exists.")

def get_pr_list(repo_url: str) -> List[Dict]:
    """Retrieve a list of pull requests from the repository."""
    if not check_gh_command():
        return []

    try:
        result = subprocess.run(
            ['gh', 'pr', 'list', '--repo', repo_url, '--json', 'number,author,createdAt'],
            capture_output=True,
            text=True, check=True
        )
        pr_list = json.loads(result.stdout)
        pr_list.sort(key=lambda x: parser.parse(x['createdAt']), reverse=True)

        pr_dict = {}
        for pr in pr_list:
            pr_id = pr['number']
            pr_author = pr['author']['login']
            pr_created_at = parser.parse(pr['createdAt']).astimezone(INDIA_TZ)
            
            if pr_author not in pr_dict:
                pr_dict[pr_author] = {'ids': [pr_id], 'createdAt': pr_created_at}
            else:
                pr_dict[pr_author]['ids'].append(pr_id)
                pr_dict[pr_author]['createdAt'] = max(pr_dict[pr_author]['createdAt'], pr_created_at)

        pr_details_list = []
        for pr_info in pr_dict.values():
            for pr_id in pr_info['ids']:
                pr_details_list.append(get_pr_details(repo_url, pr_id))

        return pr_details_list
    except subprocess.CalledProcessError as e:
        print(f"Error getting PR list from {repo_url}: {e}")
        return []

def get_pr_details(repo_url: str, pr_id: int) -> Dict:
    """Retrieve detailed information about a specific pull request."""
    if not check_gh_command():
        return {}

    try:
        result = subprocess.run(
            ['gh', 'pr', 'view', '--repo', repo_url, str(pr_id), '--json', 'title,author,files,createdAt'],
            capture_output=True,
            text=True, check=True
        )
        pr_details = json.loads(result.stdout)
        pr_files = [os.path.basename(file['path']) for file in pr_details['files']]
        pr_created_at = parser.parse(pr_details['createdAt']).astimezone(INDIA_TZ)
        print(f"PR Details: ID: {pr_id}, Author: {pr_details['author']['login']}, Files: {pr_files}, Created At: {pr_created_at}")
        return {'id': pr_id, 'author': pr_details['author']['login'], 'files': pr_files, 'createdAt': pr_created_at}
    except subprocess.CalledProcessError as e:
        print(f"Error getting PR details for {pr_id} from {repo_url}: {e}")
        return {}

def get_latest_commit_time(repo_url: str, pr_id: int) -> datetime:
    """Retrieve the latest commit time for a specific PR."""
    try:
        result = subprocess.run(
            ['gh', 'pr', 'view', '--repo', repo_url, str(pr_id), '--json', 'commits'],
            capture_output=True,
            text=True,
            check=True
        )
        pr_details = json.loads(result.stdout)
        latest_commit = max(pr_details['commits'], key=lambda c: parser.parse(c['committedDate']))
        latest_commit_time = parser.parse(latest_commit['committedDate']).astimezone(INDIA_TZ)
        return latest_commit_time
    except subprocess.CalledProcessError as e:
        print(f"Error getting commits for PR #{pr_id} from {repo_url}: {e}")
        return None

def determine_status(pr_files: List[str], target_files: List[str]) -> str:
    """Determine the status based on target files present in the PR."""
    pr_files_set = set(pr_files)
    target_files_set = set(target_files)

    # Normalize all file names to lower case for consistent comparison
    target_files_set = set(file.lower() for file in target_files_set)
    pr_files_set = set(file.lower() for file in pr_files_set)

    # If any target file is not present in PR files, return 'Not Attended'
    if not target_files_set.issubset(pr_files_set):
        return 'Not Done'

    # If all target files are present, return 'Done'
    return 'Done'

def load_names_from_file(names_file: str) -> Dict[str, str]:
    """Load names from the names file into a dictionary."""
    names_dict = {}
    try:
        with open(names_file, 'r') as file:
            for line in file:
                name_parts = line.strip().split(',')
                if len(name_parts) == 2:
                    github_username, real_name = name_parts
                    names_dict[github_username.strip().lower()] = real_name.strip()
    except FileNotFoundError:
        print(f"Error: The file {names_file} was not found.")
    except Exception as e:
        print(f"Error reading {names_file}: {e}")
    return names_dict

def fetch_data_from_sheet() -> List[Dict]:
    """Fetch data from the Google Sheet."""
    try:
        response = requests.get(GOOGLE_SHEET_URL_FETCH)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching data from Google Sheet: {e}")
        return []

def get_repo_created_date(repo_url: str) -> datetime:
    """Retrieve the repository creation date."""
    repo_name = repo_url.rstrip('/').split('/')[-1]
    repo_owner = repo_url.rstrip('/').split('/')[-2]
    repo_full_name = f"{repo_owner}/{repo_name}"

    try:
        result = subprocess.run(
            ['gh', 'repo', 'view', repo_full_name, '--json', 'createdAt'],
            capture_output=True,
            text=True,
            check=True
        )
        repo_info = json.loads(result.stdout)
        repo_created_at = parser.parse(repo_info['createdAt']).astimezone(INDIA_TZ)
        return repo_created_at
    except subprocess.CalledProcessError as e:
        print(f"Error getting creation date for repository {repo_full_name}: {e}")
        return datetime.now()

def get_repo_latest_commit_date(repo_url: str) -> datetime:
    """Retrieve the latest commit date of the repository."""
    if not check_gh_command():
        return None

    try:
        repo_owner = repo_url.rstrip('/').split('/')[-2]
        repo_name = repo_url.rstrip('/').split('/')[-1]
        repo_full_name = f"{repo_owner}/{repo_name}"

        result = subprocess.run(
            ['gh', 'repo', 'view', repo_full_name, '--json', 'defaultBranchRef'],
            capture_output=True,
            text=True,
            check=True
        )
        repo_info = json.loads(result.stdout)
        branch_name = repo_info['defaultBranchRef']['name']
        
        commits_result = subprocess.run(
            ['gh', 'api', f'repos/{repo_full_name}/commits?sha={branch_name}&per_page=1'],
            capture_output=True,
            text=True,
            check=True
        )
        commits_info = json.loads(commits_result.stdout)
        latest_commit_date = parser.parse(commits_info[0]['commit']['committer']['date']).astimezone(INDIA_TZ)
        return latest_commit_date
    except (subprocess.CalledProcessError, IndexError, KeyError) as e:
        print(f"Error getting latest commit date for repository {repo_full_name}: {e}")
        return None
def enforce_constraints(time_str: str, date_str: str, launched_str: str) -> Tuple[str, str]:
    """Enforce time and date constraints for the PR data."""
    max_time = "21:00:00"
    max_date = parser.parse(launched_str) if launched_str != 'N/A' else None

    if time_str != 'N/A':
        if time_str > max_time:
            time_str = max_time

    if date_str != 'N/A' and max_date:
        date_obj = parser.parse(date_str)
        if date_obj > max_date:
            date_str = launched_str  # Set date to Launched date

    return time_str, date_str

def prepare_and_send_data():
    """Prepare data and send it to the specified Google Sheets URL."""
    data_from_sheet = fetch_data_from_sheet()
    if not data_from_sheet:
        print("No data fetched from Google Sheet.")
        return

    output_data = []
    names_dict = load_names_from_file(NAMES_FILE)
    authorized_usernames = set(names_dict.keys())

    for entry in data_from_sheet:
        repo_url = entry.get('Repo URL', '')
        assignment = entry.get('Assignment', '')
        entry_type = entry.get('Type', '')
        target_files = entry.get('Target File', '').split(',')
        repo_date_str = entry.get('Date', '')  # Fetch the date from the sheet

        if not repo_url or not assignment:
            print(f"Skipping entry due to missing data: {entry}")
            continue

        # Clone the repository and get the repo directory name
        repo_dir = sanitize_repo_name(repo_url)
        clone_repo(repo_url)

        # Handle Target File column and Raw Code type
        if not target_files or target_files[0].strip().lower() in {"n/a", "na"}:
            # Fetch all files if Target File is N/A, NA, or empty
            target_files = [file for _, _, files in os.walk(repo_dir) for file in files if file.endswith('.java') or file.endswith('.js')]
        else:
            # Use specific file names listed in the Target File column
            target_files = [file.strip() for file in target_files if file.strip()]

        # Get PR list
        pr_list = get_pr_list(repo_url)

        # Convert sheet date to datetime object
        repo_date = parser.parse(repo_date_str).astimezone(INDIA_TZ) if repo_date_str else 'N/A'

        # Format the date and time strings
        date_str = 'N/A'
        time_str = 'N/A'
        launched_str = repo_date.strftime('%Y-%m-%d') if repo_date != 'N/A' else 'N/A'
        
        # Accumulate PR files by author
        author_pr_files = {}
        for pr in pr_list:
            pr_author = pr['author'].lower()
            if pr_author in authorized_usernames:
                if pr_author not in author_pr_files:
                    author_pr_files[pr_author] = set(pr['files'])
                else:
                    author_pr_files[pr_author].update(pr['files'])

        # Process all accumulated PR files for each author
        for pr_author, pr_files in author_pr_files.items():
            real_name = names_dict[pr_author]
            latest_commit_time = None

            # Get the latest commit time for all PRs by this author
            for pr in pr_list:
                if pr['author'].lower() == pr_author:
                    commit_time = get_latest_commit_time(repo_url, pr['id'])
                    if commit_time:
                        if not latest_commit_time or commit_time > latest_commit_time:
                            latest_commit_time = commit_time

            # Determine status and score
            if latest_commit_time:
                time_str = latest_commit_time.strftime("%H:%M:%S")
                date_str = latest_commit_time.strftime('%Y-%m-%d')
            
            status = determine_status(list(pr_files), target_files)
            score = 1 if status == 'Done' else 0

            # Enforce constraints on time and date
            time_str, date_str = enforce_constraints(time_str, date_str, launched_str)

            # Append the data
            output_data.append({
                "Repo": sanitize_repo_name(repo_url),
                "Name": real_name,
                "Assignment": assignment,
                "Date": date_str,  # Ensure date format consistency
                "Files": ', '.join(pr_files) if pr_files else 'N/A',
                "Target Files": ', '.join(target_files),
                "Status": status,
                "Score": score,
                "Time": time_str,
                "Launched": launched_str,  # Ensure date format consistency
                "Type": entry_type
            })

        # Handle non-attended persons
        attended_authors = set(author_pr_files.keys())
        for github_username, real_name in names_dict.items():
            if github_username not in attended_authors:
                output_data.append({
                    "Repo": sanitize_repo_name(repo_url),
                    "Name": real_name,
                    "Assignment": assignment,
                    "Date": None,  # Indicate missing date as None
                    "Files": 'N/A',
                    "Target Files": ', '.join(target_files),
                    "Status": 'Not Done',
                    "Score": 0,
                    "Time": 'N/A',
                    "Launched": launched_str,  # Ensure date format consistency
                    "Type": entry_type
                })

    # Clear past data in the Google Sheet (if necessary)
    clear_sheet_url = f"{GOOGLE_SHEET_URL_UPDATE}?action=clear"
    try:
        response = requests.get(clear_sheet_url)
        response.raise_for_status()
        print(f"Cleared past data from Google Sheets: {response.json()}")
    except requests.RequestException as e:
        print(f"Error clearing data in Google Sheets: {e}")

    # Send the new data to Google Sheets
    try:
        response = requests.post(GOOGLE_SHEET_URL_UPDATE, json=output_data)
        response.raise_for_status()
        print(f"Data successfully sent to Google Sheets: {response.json()}")
    except requests.RequestException as e:
        print(f"Error sending data to Google Sheets: {e}")


# Example usage
if __name__ == "__main__":
    prepare_and_send_data()
