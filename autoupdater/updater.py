import os
import requests
from zipfile import ZipFile
import sandbox.main as main
from autoupdater.version import Version
from github import Github
from github.GitRelease import GitRelease

REPO_ID = 673123229

def get_latest_release(g: Github) -> GitRelease:
    repo = g.get_repo(REPO_ID)
    return repo.get_latest_release()

def check_for_update():
    g = Github()
    release = get_latest_release(g)
    latest_version_str = release.tag_name
    latest_version = Version(latest_version_str)
    current_version = Version(main.__version__)
    if latest_version > current_version:
        if len(release.assets) != 1:
            print("asset size mismatch")

        asset = release.assets[0]
        r = requests.get(asset.browser_download_url)
        bin_path = "/apis/bin"
        zip_path = "{bin_path}/{zip_name}.zip".format(bin_path=bin_path, zip_name=latest_version_str)
        if not os.path.exists(bin_path):
            os.makedirs(bin_path)
        with open(zip_path, "wb") as f:
            f.write(r.content)

        with ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(bin_path)
        
        if os.path.exists(zip_path):
            os.remove(zip_path)
        
    