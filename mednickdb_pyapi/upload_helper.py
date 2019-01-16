import sys
import glob
import re
import os
#subject_stems = {'subjectid', 'subjectnum', 'subject', 'subj', 'subject'}
re_i_type = '\d+'
re_s_type = '[a-z0-9]*'

default_pattern_keys = {
    'subjectid': re_i_type,
    'studyid': re_s_type,
    'visitid': re_i_type,
    'sessionid': re_i_type,
    'versionid': re_i_type,
    'filetype': re_s_type,
    'fileformat': re_s_type
}

type_map = {'i':'\d+',
            's':'[A-z0-9]*'}


def parse_args_to_reg_ex(search_params):
    """
    TODO
    :param search_params:
    :return:
    """
    # parse filename matching
    pattern_to_match = search_params#sys.argv[2]
    pattern_components = pattern_to_match.split('}')[:-1]
    pattern_stems = [pattern.split('{')[0] for pattern in pattern_components]
    pattern_key_parts = [pattern.split('{')[1] for pattern in pattern_components]
    pattern_keys = []
    pattern_types = []
    for pattern_key_part in pattern_key_parts:
        if '=' in pattern_key_part:
            pattern_types.append(type_map[pattern_key_part.split('=')[1]])
            pattern_keys.append(pattern_key_part.split('=')[0])
        else:
            pattern_types.append(
                default_pattern_keys[pattern_key_part] if pattern_key_part in default_pattern_keys else '[A-z0-9]*')
            pattern_keys.append(pattern_key_part)

    re_exp = ''
    for stem, type in zip(pattern_stems, pattern_types):
        re_exp += stem + '(' + type + ')'

    return re_exp, pattern_keys, pattern_types


def file_path_to_file_info(file_path, re_exp, pattern_keys, pattern_types):
    """
    TODO
    :param file_path:
    :param re_exp:
    :param pattern_keys:
    :param pattern_types:
    :return:
    """
    values = re.search(re_exp, file_path, re.IGNORECASE)
    file_info = {}
    if values is not None:
        for pattern_key, value, dtype in zip(pattern_keys, values.groups(), pattern_types):
            if dtype == re_i_type:
                value = int(value)
            file_info[pattern_key] = value
    else:
        raise ValueError('Pattern was not matched: '+re_exp+' is not in '+file_path)
    return file_info


if __name__ == '__main__':
    folder_to_search = sys.argv[1]
    if folder_to_search[-1] is not '/' or folder_to_search[-1] is not '\\':
        folder_to_search += '/'

    # parse filename matching
    re_exp, pattern_keys, pattern_types = parse_args_to_reg_ex(sys.argv[2])

    # parse specifiers for the whole folder
    if len(sys.argv) > 3:
        other_specifiers = sys.argv[3:]
        files_info = {other_spec.split('=')[0]: other_spec.split('=')[1] for other_spec in other_specifiers}
    else:
        files_info = {}

    for file_path in glob.iglob(folder_to_search + '*'):
        file_info = file_path_to_file_info(file_path, re_exp, pattern_keys, pattern_types)
        file_info = file_info.update(files_info)
        print(file_path, '-->', file_info)
