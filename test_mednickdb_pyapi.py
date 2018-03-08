from mednickdb_pyapi import MednickAPI


def test_login():
    med_api = MednickAPI('http://localhost:8001', 'bdyetton@hotmail.com', 'Pass1234')
    assert(med_api.token)
    assert(med_api.usertype == 'Admin')


def test_upload_and_download_file():
    med_api = MednickAPI('http://localhost:8001', 'bdyetton@hotmail.com', 'Pass1234')
    with open('README.md') as uploaded_version:
        fid = med_api.upload_file(uploaded_version,'TestFile.yay','PSTIM', 1, 1)
        downloaded_version = med_api.download_file(fid)
        assert(downloaded_version == uploaded_version)
