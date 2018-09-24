from mednickdb_pyapi.mednickdb_pyapi import MednickAPI
import pytest
import time
import pandas as pd

server_address = 'http://saclab.ss.uci.edu:8000'

file_update_time = 2
data_update_time = 10


data_upload_working = False

def issubset(superset, subset):
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
    remaining_test_data = med_api.get_data(studyid='TEST')
    # for data in remaining_test_data:
    #     med_api.delete_data(dataid=data['_id'])
    # assert len(med_api.get_data(studyid='TEST')) == 0 #TODO after clearing up sourceid bug


@pytest.mark.dependency(['test_clear_test_study'])
def test_usecase_1():
    """Runs usecase one from the mednickDB_usecase document (fid=)"""
    #a)
    med_api = MednickAPI(server_address, 'test_ra_account@uci.edu', 'Pass1234')
    file_info_post = {
        'fileformat':'sleep',
        'studyid':'TEST',
        'versionid':1,
        'subjectid':1,
        'visitid':1,
        'sessionid':1,
        'filetype':'raw_sleep',
    }
    with open('testfiles/sleepfile1.edf','rb') as sleepfile:
        fid = med_api.upload_file(fileobject=sleepfile, **file_info_post)

    with open('testfiles/sleepfile1.edf', 'rb') as sleepfile:
        downloaded_sleepfile = med_api.download_file(fid)
        assert (downloaded_sleepfile == sleepfile.read())

    #b)
    time.sleep(file_update_time)  # Give db 5 seconds to update
    file_info_get = med_api.get_file_by_fid(fid)
    file_info_post.update({'filename': 'testfiles/sleepfile1.edf', 'filepath':'TEST/1/1/1/1/'})
    assert issubset(file_info_get, file_info_post)

    time.sleep(data_update_time-file_update_time)  # Give db 5 seconds to update

    file_data = med_api.get_data_from_single_file(filetype='raw_sleep', fid=fid, format='flat_dict')
    file_data_real = file_info_post.copy()
    file_data_real.pop('fileformat')
    file_data_real.pop('filetype')
    file_data_real.update({'raw_sleep.edf_starttime': "10:10"})
    assert(file_data_real in file_data)

    pytest.usecase_1_filedata = file_data
    pytest.usecase_1_fid = fid


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
        fid = med_api.upload_file(fileobject=demofile, **file_info_post)
        downloaded_demo = med_api.download_file(fid)
        with open('testfiles/TEST_Demographics.xlsx', 'rb') as demofile:
            assert downloaded_demo == demofile.read()

    # c)
    time.sleep(file_update_time)  # Give file db 5 seconds to update
    file_info_post.update({'filename': 'testfiles/TEST_Demographics.xlsx', 'filepath': 'TEST/1/1/'})
    file_info_get = med_api.get_file_by_fid(fid)
    assert issubset(file_info_get, file_info_post)

    if not data_upload_working:
        return

    # d)
    time.sleep(data_update_time)  # Give data db 50 seconds to update
    data_rows = med_api.get_data(studyid='TEST', versionid=1)
    correct_row1 = {'studyid': 'TEST', 'versionid': 1, 'subjectid': 1,
                    'demographics.age': 23, 'demographics.sex': 'F', 'demographics.bmi': 23}
    correct_row1.update(pytest.usecase_1_filedata)
    correct_row2 = {'studyid': 'TEST', 'versionid': 1, 'subjectid': 2,
                    'demographics.age': 19, 'demographics.sex': 'M', 'demographics.bmi': 20}
    correct_data = [correct_row1, correct_row2]
    assert all([row in data_rows for row in correct_data])

    # e)
    data_raw_sleep = med_api.get_data(studyid='TEST', versionid=1, filetype='raw_sleep')
    assert (data_raw_sleep in pytest.usecase_1_filedata)

    pytest.usecase_2_row1 = correct_row1
    pytest.usecase_2_row2 = correct_row2
    pytest.usecase_2_fid = fid


@pytest.mark.dependency(['test_usecase_2'])
def test_usecase_3():

    # a)
    med_api = MednickAPI(server_address, 'test_ra_account@uci.edu', 'Pass1234')
    med_api.upload_data(data={'accuracy': 0.9}, studyid='TEST', versionid=1, subjectid=2, visitid=1, sessionid=1,
                        filetype='MemTaskA')

    # b)
    time.sleep(5)  # Give db 5 seconds to update
    correct_fids = [pytest.usecase_1_fid, pytest.usecase_2_fid]
    fids = med_api.get_files(subjectid='TEST', versionid=1)
    assert (all([True if fid in correct_fids else False for fid in fids]))

    # c)
    time.sleep(5)  # Give db 5 seconds to update
    data = med_api.get_data(studyid='TEST', versionid=1)
    correct_row_2 = pytest.usecase_2_row2
    correct_row_2.update({'accuracy': 0.9})
    correct_data = [pytest.usecase_2_row1, correct_row_2]
    for i, data_i in enumerate(data):
        assert (all([data_i[k] == correct_data[i][k] for k in correct_data[i]]))
    pytest.usecase_3_row2 = correct_row_2


@pytest.mark.dependency(['test_usecase_3'])
def test_usecase_4():
    # a)
    med_api = MednickAPI(server_address, 'test_grad_account@uci.edu', 'Pass1234')

    # b)
    with open('testfile/scorefile1.mat') as scorefile1:
        fid1 = med_api.upload_file(scorefile1,
                                   fileformat='scorefile',
                                   studyid='TEST',
                                   versionid=1,
                                   subjectid=1,
                                   visitid=1,
                                   sessionid=1,
                                   filetype='sleep')

    with open('testfile/scorefile2.mat') as scorefile2:
        fid2 = med_api.upload_file(scorefile2,
                                   fileformat='scorefile',
                                   studyid='TEST',
                                   versionid=1,
                                   subjectid=1,
                                   visitid=2,
                                   sessionid=1,
                                   filetype='sleep')

    scorefile1_data = {}  # TODO add file data here
    scorefile2_data = {}

    # c)
    time.sleep(50)  # Give db 50 seconds to update
    data = med_api.get_data(studyid='TEST', versionid=1)
    correct_row_1 = pytest.usecase_2_row1
    correct_row_2 = scorefile1_data.update(pytest.usecase_3_row2)
    correct_row_3 = scorefile2_data.update(pytest.usecase_2_row2)
    correct_return = [correct_row_1, correct_row_2, correct_row_3]
    assert (data == correct_return)
    pytest.usecase_4_row1 = correct_row_1
    pytest.usecase_4_row2 = correct_row_2
    pytest.usecase_4_row3 = correct_row_3


@pytest.mark.dependency(['test_usecase_4'])
def test_usecase_5():
    # a)
    med_api = MednickAPI(server_address, 'test_grad_account@uci.edu', 'Pass1234')
    data = med_api.search_datastore(query_string='studyid = TEST and accuracy > 0.9')
    assert (data == pytest.usecase_4_row2)


def test_get_specifiers():
    med_api = MednickAPI(server_address, 'test_grad_account@uci.edu', 'Pass1234')
    sids = med_api.get_studyids()
    assert ('TEST' in sids)

    vids = med_api.get_versionids(studyid='TEST', store='data')
    assert (vids == [1])

    sids = med_api.get_subjectids(studyid='TEST')
    assert (sids == [1, 2])

    vids = med_api.get_visitids(studyid='TEST', store='data')
    assert (vids == [1, 2])

    sids = med_api.get_sessionids(studyid='TEST', store='data')
    assert (sids == [1])

    filetypes = med_api.get_filetypes(studyid='TEST', store='data')
    assert (filetypes == ['raw sleep', 'scorefile', 'demographics', 'MemTaskA'])


def test_update_file_info():
    med_api = MednickAPI(server_address, 'test_grad_account@uci.edu', 'Pass1234')
    fids = med_api.get_files(studyid='TEST')
    file_info_1 = med_api.get_file_by_fid(fids[0])
    to_add = {'sessionid': '10'}
    med_api.update_file_info(fid=fids[0], file_info=to_add)
    file_info_1.update(to_add)
    time.sleep(5)  # Give db 5 seconds to update

    file_info_2 = med_api.get_file_by_fid(fids[0])
    assert (file_info_2 == file_info_1)


def test_parsing_status():
    med_api = MednickAPI(server_address, 'test_grad_account@uci.edu', 'Pass1234')
    fids = med_api.get_files(studyid='TEST')
    med_api.update_parsed_status(fids[0], False)
    time.sleep(5)
    fids2 = med_api.get_unparsed_files()
    assert (fids[0] in fids2)
