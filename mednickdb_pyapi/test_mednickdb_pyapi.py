from mednickdb_pyapi.mednickdb_pyapi import MednickAPI


def test_login():
    med_api = MednickAPI('http://saclab.ss.uci.edu:8000', 'bdyetton@hotmail.com', 'Pass1234')
    assert med_api.token
    assert med_api.usertype == 'admin'


def test_upload_and_download_file():
    med_api = MednickAPI('http://saclab.ss.uci.edu:8000', 'bdyetton@hotmail.com', 'Pass1234')
    files_on_server_before_upload = med_api.get_files()
    parsed_files_before_upload = med_api.get_unparsed_files()
    with open('testfiles/scorefile1.mat', 'rb') as uploaded_version:
        fid = med_api.upload_file(fileobject=uploaded_version,
                                  fileformat='sleep',
                                  studyid='TEST',
                                  subjectid=str(1),
                                  versionid=str(1),
                                  filetype='mat')
        downloaded_version = med_api.download_file(fid[0])
    with open('testfiles/scorefile1.mat', 'rb') as uploaded_version:
        assert downloaded_version == uploaded_version.read()
    files_on_server_after_upload = med_api.get_files()
    parsed_files_after_upload = med_api.get_unparsed_files()
    assert len(files_on_server_before_upload)+1 == len(files_on_server_after_upload)
    assert len(parsed_files_before_upload)+1 == len(parsed_files_after_upload)

