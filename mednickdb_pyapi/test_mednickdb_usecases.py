from mednickdb_pyapi.mednickdb_pyapi import MednickAPI
import pytest
import time
import pandas as pd
import pprint
pp = pprint.PrettyPrinter(indent=4)

server_address = 'http://saclab.ss.uci.edu:8000'

file_update_time = 2
data_update_time = 10


data_upload_working = False


def dict_issubset(superset, subset, show_diffs=False):
    if show_diffs:
        return [item for item in subset.items() if item not in superset.items()]
    return all(item in superset.items() for item in subset.items())


def pytest_namespace():
    return {'usecase_1_filedata': None}


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
def test_usecase_1():
    """runs usecase one from the mednickdb_usecase document (fid=)"""
    #a)
    med_api = MednickAPI(server_address, 'test_ra_account@uci.edu', 'pass1234')
    file_info_post = {
        'fileformat':'sleep',
        'studyid':'TEST',
        'versionid':1,
        'subjectid':1,
        'visitid':1,
        'sessionid':1,
        'filetype':'sleep_eeg',
    }
    file_data_real = file_info_post.copy()
    with open('testfiles/sleepfile1.edf','rb') as sleepfile:
        file_info_returned = med_api.upload_file(fileobject=sleepfile, **file_info_post)

    with open('testfiles/sleepfile1.edf', 'rb') as sleepfile:
        downloaded_sleepfile = med_api.download_file(file_info_returned['_id'])
        assert (downloaded_sleepfile == sleepfile.read())

    # b)
    time.sleep(file_update_time)  # give db 5 seconds to update
    file_info_get = med_api.get_file_by_fid(file_info_returned['_id'])
    file_info_post.update({'filename': 'sleepfile1.edf', 'filedir': 'uploads/TEST/1/1/1/1/sleep_eeg/'})
    assert dict_issubset(file_info_get, file_info_post)

    time.sleep(data_update_time-file_update_time)  # give db 5 seconds to update
    file_datas = med_api.get_data_from_single_file(filetype='sleep_eeg', fid=file_info_returned['_id'], format='flat_dict')
    file_data_real.pop('fileformat')
    file_data_real.pop('filetype')
    file_data_real.update({'sleep_eeg.edf_nchan': 3})  # add actual data in file. # TODO add all
    pytest.usecase_1_filedata = file_data_real
    pytest.usecase_1_filename_version = file_info_get['filename_version']

    assert(any([dict_issubset(file_data, file_data_real) for file_data in file_datas])), "Is pyparse running?"




@pytest.mark.dependency(['test_usecase_1'])
def test_usecase_2():
    # a)

    file_info_post = {'filetype':'demographics',
                      'fileformat':'tabular',
                      'studyid':'TEST',
                      'versionid':1}

    med_api = MednickAPI(server_address, 'test_grad_account@uci.edu', 'Pass1234')
    with open('testfiles/TEST_Demographics.xlsx', 'rb') as demofile:
        # b)
        file_info = med_api.upload_file(fileobject=demofile, **file_info_post)
        fid = file_info['_id']
        downloaded_demo = med_api.download_file(fid)
        with open('testfiles/TEST_Demographics.xlsx', 'rb') as demofile:
            assert downloaded_demo == demofile.read()

    # c)
    time.sleep(file_update_time)  # Give file db 5 seconds to update
    file_info_post.update({'filename': 'TEST_Demographics.xlsx', 'filedir': 'uploads/TEST/1/demographics/'})
    file_info_get = med_api.get_file_by_fid(fid)
    assert dict_issubset(file_info_get, file_info_post)

    # d)
    time.sleep(data_update_time-file_update_time)  # Give data db 50 seconds to update
    data_rows = med_api.get_data(studyid='TEST', versionid=1, format='flat_dict')
    correct_row1 = {'studyid': 'TEST', 'versionid': 1, 'subjectid': 1,
                    'demographics.age': 23, 'demographics.sex': 'F', 'demographics.bmi': 23}
    correct_row1.update(pytest.usecase_1_filedata)
    correct_row2 = {'studyid': 'TEST', 'versionid': 1, 'subjectid': 2,
                    'demographics.age': 19, 'demographics.sex': 'M', 'demographics.bmi': 20}
    correct_rows = [correct_row1, correct_row2]

    pytest.usecase_2_row1 = correct_row1
    pytest.usecase_2_row2 = correct_row2
    pytest.usecase_2_filename_version = file_info_get['filename_version']

    for correct_row in correct_rows:
        assert any([dict_issubset(data_row, correct_row) for data_row in data_rows]), "demographics data downloaded does not match expected"

    # e)
    data_sleep_eeg = med_api.get_data(studyid='TEST', versionid=1, filetype='sleep_eeg')[0] #FIXME will fail here until filetype is quierable
    assert dict_issubset(data_sleep_eeg, pytest.usecase_1_filedata), "sleep data downloaded does not match what was uploaded in usecase 1"


@pytest.mark.dependency(['test_usecase_2'])
def test_usecase_3():

    # a)
    med_api = MednickAPI(server_address, 'test_ra_account@uci.edu', 'Pass1234')
    fid_for_manual_upload = med_api.extract_var(med_api.get_files(studyid='TEST'), '_id')[0] # get a random fid
    data_post = {'studyid': 'TEST',
                 'filetype': 'memtesta',
                 'data': {'accuracy': 0.9},
                 'versionid': 1,
                 'subjectid': 2,
                 'visitid': 1,
                 'sessionid': 1}
    med_api.upload_data(**data_post, fid=fid_for_manual_upload)

    # b)
    time.sleep(5)  # Give db 5 seconds to update
    correct_filename_versions = [pytest.usecase_1_filename_version, pytest.usecase_2_filename_version]
    filename_versions = med_api.extract_var(med_api.get_files(studyid='TEST', versionid=1), 'filename_version')
    assert all([fid in correct_filename_versions for fid in filename_versions]), "Missing expected filename versions from two previous usecases"

    # c)
    time.sleep(5)  # Give db 5 seconds to update
    data_rows = med_api.get_data(studyid='TEST', versionid=1, format='flat_dict')
    correct_row_2 = pytest.usecase_2_row2.copy()
    correct_row_2.update({'memtesta.accuracy': 0.9, 'visitid': 1})
    pytest.usecase_3_row2 = correct_row_2
    correct_rows = [pytest.usecase_2_row1, correct_row_2]
    for correct_row in correct_rows:
        assert any([dict_issubset(data_row, correct_row) for data_row in data_rows])




@pytest.mark.dependency(['test_usecase_3'])
def test_usecase_4():
    # a)
    med_api = MednickAPI(server_address, 'test_grad_account@uci.edu', 'Pass1234')

    # b) uploading some scorefiles
    file_info1_post = {
        'fileformat':'sleep_scoring',
        'studyid':'TEST',
        'versionid':1,
        'subjectid':2,
        'visitid':1,
        'sessionid':1,
        'filetype':'sleep_scoring'
    }
    with open('testfiles/scorefile1.mat', 'rb') as scorefile1:
        fid1 = med_api.upload_file(scorefile1,
                                   **file_info1_post)

    file_info2_post = file_info1_post.copy()
    file_info2_post.update({'visitid':2})
    with open('testfiles/scorefile2.mat', 'rb') as scorefile2:
        fid2 = med_api.upload_file(scorefile2,
                                   **file_info2_post)

    scorefile1_data = {'sleep_scoring.epochstage': [-1, -1, -1, 0, 0, 0, 0, 0, 0, 0],
                       'sleep_scoring.epochoffset': [0, 30, 60, 90, 120, 150, 180, 210, 240, 270],
                       'sleep_scoring.starttime': 1451635302000, 'sleep_scoring.mins_in_0': 3.5, 'sleep_scoring.mins_in_1': 0,
                       'sleep_scoring.mins_in_2': 0, 'sleep_scoring.mins_in_3': 0, 'sleep_scoring.mins_in_4': 0,
                       'sleep_scoring.sleep_efficiency': 0, 'sleep_scoring.total_sleep_time': 0}
    scorefile2_data = {'sleep_scoring.epochstage': [0, 0, 1, 1, 2, 2, 3, 3, 2, 2],
                       'sleep_scoring.epochoffset': [0, 30, 60, 90, 120, 150, 180, 210, 240, 270],
                       'sleep_scoring.starttime': 1451635302000, 'sleep_scoring.mins_in_0': 1, 'sleep_scoring.mins_in_1': 1,
                       'sleep_scoring.mins_in_2': 2, 'sleep_scoring.mins_in_3': 1, 'sleep_scoring.mins_in_4': 0,
                       'sleep_scoring.sleep_efficiency': 0.8, 'sleep_scoring.total_sleep_time': 4}

    # c)
    time.sleep(data_update_time)  # Give db 50 seconds to update
    data_rows = med_api.get_data(studyid='TEST', versionid=1, format='flat_dict')
    correct_row_1 = pytest.usecase_2_row1.copy()
    scorefile1_data.update(pytest.usecase_3_row2)
    correct_row_2 = scorefile1_data
    scorefile2_data.update(pytest.usecase_2_row2)
    correct_row_3 = scorefile2_data
    correct_rows = [correct_row_1, correct_row_2, correct_row_3]
    for correct_row in correct_rows:
        assert any([dict_issubset(data_row, correct_row) for data_row in data_rows])

    pytest.usecase_4_row1 = correct_row_1
    pytest.usecase_4_row2 = correct_row_2
    pytest.usecase_4_row3 = correct_row_3


@pytest.mark.dependency(['test_usecase_4'])
def test_usecase_5():
    # a)
    med_api = MednickAPI(server_address, 'test_grad_account@uci.edu', 'Pass1234')
    data_rows = med_api.get_data(query='studyid=TEST and data.memtesta.accuracy>=0.9', format='flat_dict')
    assert any([dict_issubset(data_row, pytest.usecase_3_row2) for data_row in data_rows])


def test_get_specifiers():
    med_api = MednickAPI(server_address, 'test_grad_account@uci.edu', 'Pass1234')
    sids = med_api.get_unique_var_values('studyid', store='data')
    assert 'TEST' in sids

    vids = med_api.get_unique_var_values('versionid', studyid='TEST', store='data')
    assert vids == [1]

    sids = med_api.get_unique_var_values('subjectid', studyid='TEST', store='data')
    assert sids == [1, 2]

    vids = med_api.get_unique_var_values('visitid', studyid='TEST', store='data')
    assert vids == [1, 2]

    sids = med_api.get_unique_var_values('sessionid', studyid='TEST', store='data')
    assert sids == [1]

    filetypes = med_api.get_unique_var_values('filetype', studyid='TEST', store='data')
    assert set(filetypes) == {'sleep_eeg', 'sleep_scoring', 'demographics', 'memtesta'}
