import concurrent.futures
from typing import List, Dict
from requests import Session
import json
from tqdm import tqdm

def create_session(token: str) -> Session:
    session = Session()
    session.headers.update({
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github.v3+json'
    })
    return session

def get_branches(org: str, repo: str, token: str) -> List[Dict]:
    session = create_session(token)
    all_branches = []

    url = f'https://api.github.com/repos/{org}/{repo}/branches'
    first_response = session.get(url, params={'per_page': 100, 'page': 1})
    first_response.raise_for_status()

    branches = first_response.json()
    all_branches.extend([
        {'branch_name': branch['name'], 'commit_sha': branch['commit']['sha']}
        for branch in branches
    ])

    if len(branches) == 100:
        def fetch_page(page: int) -> List[Dict]:
            response = session.get(url, params={'per_page': 100, 'page': page})
            response.raise_for_status()
            return response.json()

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_page = {
                executor.submit(fetch_page, page): page
                for page in range(2, 11)  # Limiting to 10 pages for safety
            }

            for future in concurrent.futures.as_completed(future_to_page):
                try:
                    branches = future.result()
                    if not branches:
                        break
                    all_branches.extend([
                        {'branch_name': branch['name'], 'commit_sha': branch['commit']['sha']}
                        for branch in branches
                    ])
                except Exception as e:
                    print(f"Error fetching page: {e}")

    return all_branches

def get_branch_details(org: str, repo: str, branches: List[Dict], token: str) -> List[Dict]:
    session = create_session(token)

    def fetch_branch_details(branch: Dict) -> Dict | None:
        try:
            url = f'https://api.github.com/repos/{org}/{repo}/branches/{branch["branch_name"]}'
            response = session.get(url)
            response.raise_for_status()
            details = response.json()

            user_details = {
                'author': details['commit']['author']['login'],
                'last_updated': details['commit']['commit']['author']['date'],
                'is_merged': details['commit']['commit']['message'].startswith('Merge'),
                'is_protected': details['protected'],
                **branch
            }
            return user_details
        except Exception as e:
            print(f"Error fetching details for branch {branch['branch_name']}: {e}")
            return None

    all_branch_details = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_branch = {
            executor.submit(fetch_branch_details, branch): branch
            for branch in branches
        }

        for future in tqdm(
            concurrent.futures.as_completed(future_to_branch),
            total=len(branches),
            desc="Fetching branch details"
        ):
            try:
                result = future.result()
                if result:
                    all_branch_details.append(result)
            except Exception as e:
                print(f"Error processing branch: {e}")

    return all_branch_details

if __name__ == "__main__":
    org = "MY_ORG"
    repo = "MY_REPO"
    token = "MY_TOKEN"

    print("Fetching branches...")
    branches = get_branches(org, repo, token)
    print(f"Found {len(branches)} branches")

    branch_details = get_branch_details(org, repo, branches, token)
    print(f"Processed {len(branch_details)} branch details")

    with open('branches.json', 'w') as f:
        json.dump(branch_details, f, indent=2)
