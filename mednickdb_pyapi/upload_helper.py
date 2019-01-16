import sys
import glob
import re
from typing import List

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

type_map = {'int':'\d+',
            'str':'[A-z0-9]*'}


def _parse_args_to_reg_ex(search_pattern: str):
    """
    Take a human readable string pattern and parse to a re expression, and a set of keys and types to extract from a filename
    :param search_pattern: A string to match files to, where specifiers, like subjectid, visitid, etc can be extracted by adding {subjectid}.
        e.g. if a filename is mydata_subid43, then mydata_subid{subjectid} will create a regular expression to
        match mydata_subid and also extract 43 as the subjectid.
    :return:
    """
    # parse filename matching
    pattern_to_match = search_pattern#sys.argv[2]
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


def _file_path_to_upload_info(file_path: str, re_exp: str, pattern_keys: List[str], pattern_types: List[str]):
    """
    To be used in conjunction with _parse_args_to_reg_ex.
    Parsing can automatically remove parts of the filename and set as any
    :param file_path: file path to parse.
    :param re_exp: regular expression string to match
    :param pattern_keys: names of the keys, in order for the created file_info object
    :param pattern_types: types for the values to be extracted from
    :return: upload_info, a dict of values extracted from file_path with types pattern_types, and keys pattern_keys
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


def run_upload_helper(folder_to_search: str, pattern: str, default_upload_args: str=None):
    """
    Searches through a folder and uploads file that match pattern. Can extract upload dict key:value pairs from filenames.
    :param folder_to_search: abs or regular path to folder to search through e.g. 'C:/Users/bdyet/data/'
    :param pattern: only upload files that match this pattern. Key/Valye pairs to add to upload dict can be added with {keyname},
        e.g. if the file name is eeg_subid32.edf, then subid{subjectid} will match that file, and also add subjectid=32 to the upload dict.
        types (either int or str) of the values will be inferred if known (e.g. subjectid->int, studyid->str) or can be set subid{subjectid=str}
    :param default_upload_args: common key/value pairs to add to all the matched files upload dic, as a string.
        e.g. 'studyid=PSTIM otherkey=somevalue'
    :return: a list of sucessfully uploaded files
    """

    files_ready_to_upload, n_files = _gather_files_to_upload(folder_to_search, pattern, default_upload_args)
    files_to_actually_upload = []

    print('%i files in folder, %i match pattern and are ready for upload' % (n_files, len(files_ready_to_upload)))
    print('Files are:')
    for idx, file_info in enumerate(files_ready_to_upload):
        print(idx, ':', file_info['filepath'])
        for key, value in file_info.items():
            if key is not 'filepath':
                print('   ', key, '=', value)
        ans = input('Is this correct? (y-yes, n-no, a-yes to all, q-quit)?')
        if ans == 'a':
            print('--uploading all files')
            return None
        elif ans == 'y':
            files_to_actually_upload.append(file_info)
            print('--upload this file')
            continue
        elif ans == 'n':
            print('--skipped this file')
            continue
        else:
            print('Quitting, no files uploaded')
            return None

    # upload TODO
    return files_to_actually_upload


def _gather_files_to_upload(folder_to_search: str, pattern: str, default_upload_args: str=None):
    """
    Searches through a folder and uploads file that match pattern. Can extract upload dict key:value pairs from filenames.
    :param folder_to_search: abs or regular path to folder to search through e.g. 'C:/Users/bdyet/data/'
    :param pattern: only upload files that match this pattern. Key/Valye pairs to add to upload dict can be added with {keyname},
        e.g. if the file name is eeg_subid32.edf, then subid{subjectid} will match that file, and also add subjectid=32 to the upload dict
    :param default_upload_args: common key/value pairs to add to all the matched files upload dic, as a string.
        e.g. 'studyid=PSTIM otherkey=somevalue'
    :return: a list of upload dicts for the files to upload. File path will be added as a "filename" key to each.
    """

    if folder_to_search[-1] is not '/' or folder_to_search[-1] is not '\\':
        folder_to_search += '/'

    # parse filename matching
    re_exp, pattern_keys, pattern_types = _parse_args_to_reg_ex(pattern)

    # parse specifiers for the whole folder
    if default_upload_args is not None:
        other_specifiers = sys.argv[3:]
        files_info = {other_spec.split('=')[0]: other_spec.split('=')[1] for other_spec in other_specifiers}
    else:
        files_info = {}

    file_infos = []
    files_in_folder = glob.glob(folder_to_search + '*')
    for file_path in files_in_folder:
        file_info = _file_path_to_upload_info(file_path, re_exp, pattern_keys, pattern_types)
        file_info.update(files_info)
        file_info['filepath'] = file_path
        file_infos.append(file_info)

    return file_infos, len(files_in_folder)


if __name__ == '__main__':
    run_upload_helper(sys.argv[1], sys.argv[2], sys.argv[3:] if len(sys.argv)>2 else None)

