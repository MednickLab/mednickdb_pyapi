from mednickdb_pyapi.mednickdb_pyapi import MednickAPI
import pytest
import time

user = 'bdyetton@hotmail.com'
password = 'Pass1234'

server_address = 'http://saclab.ss.uci.edu:8000'


def test_login():
    """Test login, this will always pass until we deal with login"""
    med_api = MednickAPI(server_address, user , password)
    assert med_api.token
    assert med_api.usertype == 'admin'


def test_clear_test_study():
    med_api = MednickAPI(server_address, 'test_grad_account@uci.edu', 'Pass1234')
    fids = med_api.extract_var(med_api.get_files(studyid='TEST'), '_id')
    if fids:
        for fid in fids:
            med_api.delete_file(fid, delete_all_versions=True)
            med_api.delete_data_from_single_file(fid)
        fids2 = med_api.extract_var(med_api.get_files(studyid='TEST'),'_id')
        assert fid not in fids2
        assert (fids2 == [])
        deleted_fids = med_api.extract_var(med_api.get_deleted_files(),'_id')
        assert all([dfid in deleted_fids for dfid in fids])
    med_api.delete_data(studyid='TEST')
    assert len(med_api.get_data(studyid='TEST', format='nested_dict')) == 0 #TODO after clearing up sourceid bug


@pytest.mark.dependency(['test_clear_test_study'])
def test_upload_and_download_file():
    """Uploaded a file and download it again and make sure it matches"""
    med_api = MednickAPI(server_address, user, password)
    files_on_server_before_upload = med_api.get_files()
    parsed_files_before_upload = med_api.get_unparsed_files()
    with open('testfiles/scorefile1.mat', 'rb') as uploaded_version:
        file_info = med_api.upload_file(fileobject=uploaded_version,
                                  fileformat='scorefile',
                                  studyid='TEST',
                                  subjectid=1,
                                  versionid=1,
                                  filetype='sleep')
        downloaded_version = med_api.download_file(file_info['_id'])
    with open('testfiles/scorefile1.mat', 'rb') as uploaded_version:
        assert downloaded_version == uploaded_version.read()
    files_on_server_after_upload = med_api.get_files()
    parsed_files_after_upload = med_api.get_unparsed_files()
    assert len(files_on_server_before_upload)+1 == len(files_on_server_after_upload)
    assert len(parsed_files_before_upload)+1 == len(parsed_files_after_upload)

@pytest.mark.dependency(['test_clear_test_study'])
def test_upload_and_overwrite():
    """Test that a file uploaded with the same name and info overwrites the older version
    When a file with the same filename, and same location in the file servers is uploaded:
        - The previous version will be set as active=False
        - The new version will get a new FID
        -

    """
    med_api = MednickAPI(server_address, user, password)
    with open('testfiles/TEST_Demographics.xlsx', 'rb') as uploaded_version_1:
        file1_info_before_overwrite = med_api.upload_file(fileobject=uploaded_version_1,
                                   fileformat='tabular',
                                   studyid='TEST',
                                   subjectid=1,
                                   versionid=1,
                                   filetype='unique_thing_1')
    downloaded_version_1 = med_api.download_file(file1_info_before_overwrite['_id'])
    file_version_before_overwrite = file1_info_before_overwrite['filename_version']

    with open('testfiles/updated_versions/TEST_Demographics.xlsx', 'rb') as uploaded_version_2:
        file1_info_after_overwrite = med_api.upload_file(fileobject=uploaded_version_2,
                                   fileformat='tabular',
                                   studyid='TEST',
                                   subjectid=1,
                                   versionid=1,
                                   filetype='unique_thing_1')
    downloaded_version_2 = med_api.download_file(file1_info_after_overwrite['_id'])
    file_version_after_overwrite = file1_info_after_overwrite['filename_version']

    with open('testfiles/updated_versions/TEST_Demographics.xlsx', 'rb') as uploaded_version_2:
        f = uploaded_version_2.read()
        assert downloaded_version_2 == f
        assert downloaded_version_1 != f

    #Get all versions, and make sure both versions of the file match what was uploaded
    all_versions = med_api.get_files(filename='TEST_Demographics.xlsx', previous_versions=True)
    assert all([file in med_api.extract_var(all_versions, 'filename_version') for file in [file_version_after_overwrite, file_version_before_overwrite]])

    file = med_api.get_files(filename='TEST_Demographics.xlsx')
    assert len(file) == 1
    assert file1_info_before_overwrite['_id'] != file1_info_after_overwrite['_id'] #It gets a new fid
    assert file[0]['_id'] == file1_info_after_overwrite['_id']

    downloaded_version_current = med_api.download_file(file[0]['_id'])
    assert downloaded_version_current == downloaded_version_2
    assert downloaded_version_1 != downloaded_version_2


def test_file_query():
    """Upload a bunch of files to the server, and query them using all the types of querying available"""
    test_clear_test_study()  # Start Fresh
    med_api = MednickAPI(server_address, user, password)
    with open('testfiles/scorefile1.mat', 'rb') as uploaded_version:
        file_info1 = med_api.upload_file(fileobject=uploaded_version,
                                  fileformat='scorefile',
                                  studyid='TEST',
                                  subjectid=1,
                                  versionid=1,
                                  filetype='sleep')
        fid1 = file_info1['_id']

    with open('testfiles/scorefile1.mat', 'rb') as uploaded_version:
        file_info2 = med_api.upload_file(fileobject=uploaded_version,
                                  fileformat='scorefile',
                                  studyid='TEST',
                                  subjectid=2,
                                  versionid=1,
                                  filetype='sleep')
        fid2 = file_info2['_id']

    with open('testfiles/TEST_Demographics.xlsx', 'rb') as uploaded_version_1:
        file_info3 = med_api.upload_file(fileobject=uploaded_version_1,
                                   fileformat='tabular',
                                   studyid='TEST',
                                   subjectid=3,
                                   versionid=2,
                                   filetype='unique_thing_2')
        fid3 = file_info3['_id']

    time.sleep(1)

    #Test ==
    fids = med_api.extract_var(med_api.get_files(query='studyid==TEST'),'_id')
    assert all([fid in fids for fid in [fid1, fid2, fid3]])

    #Test IN
    fids = med_api.extract_var(med_api.get_files(query='subjectid in [1,2]'),'_id')
    assert all([fid in fids for fid in [fid1, fid2]])

    #Test not in
    fids = med_api.extract_var(med_api.get_files(query='subjectid not in [1,2]'),'_id')
    assert all([fid in fids for fid in [fid3]])

    # Test and
    fids = med_api.extract_var(med_api.get_files(query='subjectid==1 and versionid==1'),'_id')
    assert all([fid in fids for fid in [fid1]])

    # Test or
    fids = med_api.extract_var(med_api.get_files(query='subjectid==2 or 1'),'_id')
    assert all([fid in fids for fid in [fid1, fid2]])

    #Test not =
    fids = med_api.extract_var(med_api.get_files(query='subjectid!=2'),'_id')
    assert all([fid in fids for fid in [fid1, fid3]])

    #Test >
    fids = med_api.extract_var(med_api.get_files(query='subjectid>2'),'_id')
    assert all([fid in fids for fid in [fid3]])

    #Test <
    fids = med_api.extract_var(med_api.get_files(query='subjectid<2'),'_id')
    assert all([fid in fids for fid in [fid1]])

    #Test <=
    fids = med_api.extract_var(med_api.get_files(query='subjectid<=2'),'_id')
    assert all([fid in fids for fid in [fid1, fid2]])

    #Test <=
    fids = med_api.extract_var(med_api.get_files(query='subjectid>=2'),'_id')
    assert all([fid in fids for fid in [fid2, fid3]])

    # Test complex #TODO
    fids = med_api.extract_var(med_api.get_files(query='subjectid>2 or <=1'),'_id')
    assert all([fid in fids for fid in [fid1, fid3]])


def test_data_query():
    med_api = MednickAPI(server_address, user, password)

    def dict_is_subset(superset, subset):
        return all(item in superset.items() for item in subset.items())

    def strip_non_matching_keys(strip_from, template):
        return {k: v for k, v in strip_from.items() if k in template}

    test_clear_test_study()

    with open('testfiles/TEST_Demographics.xlsx', 'rb') as uploaded_version_1:
        file_info1 = med_api.upload_file(fileobject=uploaded_version_1,
                                           fileformat='tabular',
                                           studyid='TEST',
                                           subjectid=1,
                                           versionid=2,
                                           filetype='unique_thing_3')
        fid1 = file_info1['_id']

    row1 = {'sex':'M', 'age':22, 'edu':12}
    row2 = {'sex':'F', 'age':19, 'edu':8}
    row3 = {'sex':'M', 'age':29, 'edu':18}
    med_api.upload_data(data=row1,
                        studyid='TEST',
                        subjectid=1,
                        versionid=1,
                        visitid=1,
                        filetype='demographics',
                        fid=fid1)

    med_api.upload_data(data=row2,
                        studyid='TEST',
                        subjectid=2,
                        versionid=1,
                        visitid=2,
                        filetype='demographics',
                        fid=fid1
                        )

    med_api.upload_data(data=row3,
                        studyid='TEST',
                        subjectid=3,
                        versionid=1,
                        filetype='demographics',
                        fid=fid1)

    time.sleep(1)

    #sanity check to see if we have any data at all:
    data_rows = med_api.get_data(format='nested_dict', studyid='TEST')
    assert len(data_rows) > 0

    #Test ==
    data_rows = [strip_non_matching_keys(row['data']['demographics'], row1) for row in med_api.get_data(format='nested_dict',query='data.demographics.sex==M')]
    assert all([row in data_rows for row in [row1]])

    # Test IN
    data_rows = [strip_non_matching_keys(row['data']['demographics'], row1) for row in med_api.get_data(format='nested_dict',query='data.demographics.age in [22,19]')]
    assert all([row in data_rows for row in [row1, row2]])

    # Test not in
    data_rows = [strip_non_matching_keys(row['data']['demographics'], row1) for row in med_api.get_data(format='nested_dict',query='data.demographics.age not in [22,19]')]
    assert all([row in data_rows for row in [row3]])

    # Test and
    data_rows = [strip_non_matching_keys(row['data']['demographics'], row1) for row in med_api.get_data(format='nested_dict',query='data.demographics.age==22 and versionid==1')]
    assert all([row in data_rows for row in [row1]])

    # Test or
    data_rows = [strip_non_matching_keys(row['data']['demographics'], row1) for row in med_api.get_data(format='nested_dict',query='data.demographics.age==22 or 19')]
    assert all([row in data_rows for row in [row1, row2]])

    # Test not =
    data_rows = [strip_non_matching_keys(row['data']['demographics'], row1) for row in med_api.get_data(format='nested_dict',query='data.demographics.age!=22')]
    assert all([row in data_rows for row in [row2, row3]])

    # Test >
    data_rows = [strip_non_matching_keys(row['data']['demographics'], row1) for row in med_api.get_data(format='nested_dict',query='data.demographics.age>19')]
    assert all([row in data_rows for row in [row1, row3]])

    # Test <
    data_rows = [strip_non_matching_keys(row['data']['demographics'], row1) for row in med_api.get_data(format='nested_dict',query='data.demographics.age<22')]
    assert all([row in data_rows for row in [row2]])

    # Test <=
    data_rows = [strip_non_matching_keys(row['data']['demographics'], row1) for row in med_api.get_data(format='nested_dict',query='data.demographics.age>=22')]
    assert all([row in data_rows for row in [row1, row3]])

    # Test >=
    data_rows = [strip_non_matching_keys(row['data']['demographics'], row1) for row in med_api.get_data(format='nested_dict',query='data.demographics.age<=22')]
    assert all([row in data_rows for row in [row1, row2]])

    # Test complex or
    data_rows = [strip_non_matching_keys(row['data']['demographics'], row1) for row in med_api.get_data(format='nested_dict',query='data.demographics.age<22 or >28')]
    assert all([row in data_rows for row in [row2, row3]])


# def test_update_file_info(): #TODO Junbai to add to simple_tests
#     med_api = MednickAPI(server_address, 'test_grad_account@uci.edu', 'Pass1234')
#     fids = med_api.get_files(studyid='TEST')
#     file_info_1 = med_api.get_file_by_fid(fid=fids[0]['_id'])
#     to_add = {'sessionid': 10}
#     med_api.update_file_info(fid=fids[0]['_id'], file_info=to_add)
#     file_info_1.update(to_add)
#     time.sleep(file_update_time)  # Give db 5 seconds to update
#
#     file_info_2 = med_api.get_file_by_fid(fids[0]['_id'])
#     assert (file_info_2 == file_info_1)


# def test_parsing_status(): #TODO Junbai to add to simple_tests
#     med_api = MednickAPI(server_address, 'test_grad_account@uci.edu', 'Pass1234')
#     fids = med_api.get_files(studyid='TEST')
#     med_api.update_parsed_status(fids[0], False)
#     time.sleep(5)
#     fids2 = med_api.get_unparsed_files()
#     assert (fids[0] in fids2)






