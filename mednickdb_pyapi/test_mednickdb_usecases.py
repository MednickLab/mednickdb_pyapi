from mednickdb_pyapi.mednickdb_pyapi import MednickAPI
import pytest


def pytest_namespace():
    return {'usecase_1_filedata': None}


@pytest.mark.dependency()
def test_usecase_1():
    """Runs usecase one from the mednickDB_usecase document (fid=)"""
    #a)
    med_api = MednickAPI('http://localhost:8001', 'test_ra_account@uci.edu', 'Pass1234')
    with open('testfiles/sleepfile.edf') as sleepfile:
        fid = med_api.upload_file(sleepfile, filename='sleepfile.edf', fileformat='sleep', studyid='PSTIM', versionid=1, subjectid=1, visitid=1, sessionid=1, filetype='raw sleep')
        downloaded_sleepfile= med_api.download_file(fid)
        assert (downloaded_sleepfile == sleepfile)

    #b)
    file_data = med_api.get_file_info(fid)
    file_data_real = {
        'filename': 'sleepfile.edf',
        'filepath': 'PSTIM/1/1/1/1/',
        'studyid': 'PSTIM',
        'versionid': 1,
        'subjectid': 1,
        'visitid': 1,
        'sessionid': 1,
        'edf_starttime': "10:10",
    }
    assert(file_data == file_data_real)
    pytest.usecase_1_filedata = file_data
    pytest.usecase_1_fid = fid


@pytest.mark.dependency(['test_usecase_1'])
def test_usecase_2():
    #a)
    med_api = MednickAPI('http://localhost:8001', 'test_grad_account@uci.edu', 'Pass1234')
    with open('testfiles/PSTIM_Demographics.edf') as demofile:
        # b)
        fid = med_api.upload_file(demofile,
                                  filename='sleepfile.edf',
                                  filetype='demographics',
                                  fileformat='tabular',
                                  studyid='PSTIM',
                                  versionid=1)
        downloaded_demo = med_api.download_file(fid)
        assert (downloaded_demo == demofile)

    #c)
    file_data = med_api.get_file_info(fid)
    file_data_real = {
        'filename': 'sleepfile.edf',
        'filepath': 'PSTIM/1/1/',
        'studyid': 'PSTIM',
        'versionid': 1,
    }

    #d)
    data = med_api.get_data(studyid='PSTIM', versionid=1)
    correct_row1 = {'studyid':'PSTIM','versionid':1,'subjectid':1,'age':23, 'sex':'F', 'bmi':23}
    correct_row1.update(pytest.usecase_1_filedata)
    correct_row2 = {'studyid': 'PSTIM', 'versionid': 1, 'subjectid': 2, 'age': 19, 'sex': 'M', 'bmi': 20}
    correct_data = [correct_row1, correct_row2]
    assert (correct_data == data)

    #e)
    data_raw_sleep = med_api.get_data(studyid='PSTIM', versionid=1, filetype='raw sleep')
    assert (data_raw_sleep == pytest.usecase_1_filedata)

    pytest.usecase_2_row1 = correct_row1
    pytest.usecase_2_row2 = correct_row2
    pytest.usecase_2_fid = fid


@pytest.mark.dependency(['test_usecase_2'])
def test_usecase_3():
    #a)
    med_api = MednickAPI('http://localhost:8001', 'test_ra_account@uci.edu', 'Pass1234')
    med_api.upload_data(data={'accuracy': 0.9}, studyid='PSTIM', versionid=1, subjectid=2, visitid=1, sessionid=1, filetype='MemTaskA')

    #b)
    correct_fids = [pytest.usecase_1_fid, pytest.usecase_2_fid]
    fids = med_api.get_file_ids(subjectid='PSTIM', versionid=1)
    assert(all([True if fid in correct_fids else False for fid in fids]))

    #c)
    data = med_api.get_data(studyid='PSTIM', versionid=1)
    correct_row_2 = pytest.usecase_2_row2
    correct_row_2.update({'accuracy': 0.9})
    correct_data = [pytest.usecase_2_row1, correct_row_2]
    assert(data == correct_data)
    pytest.usecase_3_row2 = correct_row_2


@pytest.mark.dependency(['test_usecase_3'])
def test_usecase_4():
    # a)
    med_api = MednickAPI('http://localhost:8001', 'test_grad_account@uci.edu', 'Pass1234')

    #b)
    with open('testfile/scorefile1.mat') as scorefile1:
        fid1 = med_api.upload_file(scorefile1, filename='sleepfile.edf', fileformat='scorefile', studyid='PSTIM', versionid=1,
                                  subjectid=1, visitid=1, sessionid=1, filetype='scorefile')

    with open('testfile/scorefile2.mat') as scorefile2:
        fid2 = med_api.upload_file(scorefile2, filename='sleepfile.edf', fileformat='scorefile', studyid='PSTIM', versionid=1,
                                  subjectid=1, visitid=2, sessionid=1, filetype='scorefile')

    scorefile1_data = {} #TODO add file data here
    scorefile2_data = {}

    # c)
    data = med_api.get_data(studyid='PSTIM', versionid=1)
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
    #a)
    med_api = MednickAPI('http://localhost:8001', 'test_grad_account@uci.edu', 'Pass1234')
    data = med_api.search_datastore(query_string='studyid=PSTIM, accuracy>0.9')
    assert (data == pytest.usecase_4_row2)














