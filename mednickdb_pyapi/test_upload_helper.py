import pytest
import subprocess
from upload_helper import parse_args_to_reg_ex, file_path_to_file_info


def test_filename_parsing():
    # test_cases = {'subjectid23_visitid3.edf': ('subjectid{subjectid}_visitid{visitid}', {'subjectid': 23, 'visitid': 3}),
    #               'studyidPSTIM_visitid1.edf': ('studyid{studyid}_visitid{visitid}', {'studyid': 'PSTIM', 'visitid': 1}),
    #               'ID23_visit3.edf': ('ID{subjectid}_visit{visitid}', {'subjectid': 23, 'visitid': 3}),
    #               'ID24_visit3.edf': ('ID{subjectid=s}_visit{visitid}', {'subjectid': '24', 'visitid': 3}),
    #               'IDError_visit3.edf': ('ID{subjectid}_visit{visitid}', {'subjectid': '24', 'visitid': 3})}
    #
    # for file_path, (pattern, ans) in test_cases.items():
    #     re_exp, pattern_keys, pattern_types = parse_args_to_reg_ex(pattern)
    #     file_info = file_path_to_file_info(file_path, re_exp, pattern_keys, pattern_types)
    #     assert file_info == ans


    error_test_cases = {'IDError_visit3.edf': ('ID{subjectid}_visit{visitid}', ValueError)}

    for file_path, (pattern, error) in error_test_cases.items():
        error_thrown = False
        try:
            re_exp, pattern_keys, pattern_types = parse_args_to_reg_ex(pattern)
            file_info = file_path_to_file_info(file_path, re_exp, pattern_keys, pattern_types)
            assert False, "Error was not thrown"
        except error as e:
            error_thrown = True

        assert error_thrown, "Incorrect error was thrown"


def test_upload_helper_as_script():
    ret_from_script = subprocess.call('python upload_helper.py testfiles/upload_parser_test_files file{subjectid}')
