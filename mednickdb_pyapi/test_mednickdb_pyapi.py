from mednickdb_pyapi.mednickdb_pyapi import MednickAPI

user = 'bdyetton@hotmail.com'
password = 'Pass1234'


def test_login():
    """Test login, this will always pass until we deal with login"""
    med_api = MednickAPI('http://saclab.ss.uci.edu:8000', user , password)
    assert med_api.token
    assert med_api.usertype == 'admin'


def test_upload_and_download_file():
    """Uploaded a file and download it again and make sure it matches"""
    med_api = MednickAPI('http://saclab.ss.uci.edu:8000', user, password)
    files_on_server_before_upload = med_api.get_files()
    parsed_files_before_upload = med_api.get_unparsed_files()
    with open('testfiles/scorefile1.mat', 'rb') as uploaded_version:
        fid = med_api.upload_file(fileobject=uploaded_version,
                                  fileformat='scorefile',
                                  studyid='TEST',
                                  subjectid=str(1),
                                  versionid=str(1),
                                  filetype='sleep')
        downloaded_version = med_api.download_file(fid)
    with open('testfiles/scorefile1.mat', 'rb') as uploaded_version:
        assert downloaded_version == uploaded_version.read()
    files_on_server_after_upload = med_api.get_files(active_only=True)
    parsed_files_after_upload = med_api.get_unparsed_files(active_only=True)
    assert len(files_on_server_before_upload)+1 == len(files_on_server_after_upload)
    assert len(parsed_files_before_upload)+1 == len(parsed_files_after_upload)


def test_upload_and_overwrite():
    """Test that a file uploaded with the same name and info overwrites the older version"""
    med_api = MednickAPI('http://saclab.ss.uci.edu:8000', user, password)
    with open('testfiles/TEST_Demographics.xlsx', 'rb') as uploaded_version_1:
        fid1 = med_api.upload_file(fileobject=uploaded_version_1,
                                   fileformat='scorefile',
                                   studyid='TEST',
                                   subjectid=str(1),
                                   versionid=str(1),
                                   filetype='unique_thing_1')
    downloaded_version_1 = med_api.download_file(fid1)

    file1_info_before_overwrite = med_api.get_file_by_fid(fid1)

    with open('testfiles/updated_versions/TEST_Demographics.xlsx', 'rb') as uploaded_version_2:
        fid2 = med_api.upload_file(fileobject=uploaded_version_2,
                                   fileformat='scorefile',
                                   studyid='TEST',
                                   subjectid=str(1),
                                   versionid=str(1),
                                   filetype='unique_thing_1')
    downloaded_version_2 = med_api.download_file(fid2)

    downloaded_version_1_post_overwrite = med_api.download_file(fid1)
    assert downloaded_version_1 == downloaded_version_1_post_overwrite
    assert downloaded_version_2 != downloaded_version_1_post_overwrite

    file1_info_after_overwrite = med_api.get_file_by_fid(fid1)
    assert ~file1_info_after_overwrite['active']

    file = med_api.get_files(studyid='TEST', subjectid='1', versionid='1', filetype='unique_thing_1')[0]
    assert fid1 != fid2 #It gets a new fid
    assert file['_id'] == fid2
    downloaded_version_current = med_api.download_file(file['_id'])
    assert downloaded_version_current == downloaded_version_2
    assert downloaded_version_1 != downloaded_version_2
    with open('testfiles/updated_versions/TEST_Demographics.xlsx', 'rb') as uploaded_version_2:
        assert downloaded_version_2 == uploaded_version_2.read()
