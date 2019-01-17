import requests
import json
import datetime
import numpy
import re
import time
import dateutil.parser
import sys
import pandas as pd
from collections import OrderedDict
from operator import itemgetter
import _io
import base64

param_map = {
    'fid':'id',
}

# Dict to help convert human readable queries into mongo-esqe queries handeled by backend
query_kwmap = OrderedDict({
    ' and ': '&',
    ' or ': '*OR*',
    ' >= ': '=*GTE*',
    ' > ': '=*GT*',
    ' <= ': '=*LTE*',
    ' < ': '=*LT*',
    ' not in ': '=*NIN*',
    ' in ': '=*IN*',
    ' not ': '=*NE*',
    ' != ': '=*NE*',
    ' = ': '=',
    '==': '=',
    '>=': '=*GTE*',
    '>': '=*GT*',
    '<=': '=*LTE*',
    '<': '=*LT*',
    '!=': '=*NE*',
    ' & ': '&',
    ' | ': '*OR*',
    '|': '*OR*',
    '*=*':'**', #remove the extra = added after or
})


class MyEncoder(json.JSONEncoder):
    """Custom JSON encoding for converting datetimes to time since epoch, and numpy types to simple python types"""
    def default(self, obj):
        """
        :param obj: Object which should be converted
        :return: Converted object
        """
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return time.mktime(obj.timetuple())*1000  # Convert to ms since epoch
        if isinstance(obj, numpy.integer):  # TODO test
            return int(obj)
        elif isinstance(obj, numpy.floating):
            return float(obj)
        elif isinstance(obj, numpy.ndarray):
            return obj.tolist()
        else:
            return super(MyEncoder, self).default(obj)


class MyDecoder(json.JSONDecoder):
    def __init__(self, *args, **kargs):
        """
        Creates a decoder object to add custom decoding on JSON encoded strings
        :param args: see parent object
        :param kargs: as above
        """
        json.JSONDecoder.__init__(self, object_hook=self.parser,
                                  *args, **kargs)

    def parser(self, dct):
        """
        Custom JSON decoder. Parses known date fields and strings that look like datetimes to python datetime.

        :param dct: Object to parse
        :return:
        """
        for k, v in dct.items():
            if isinstance(v, str) and v == '':
                dct[k] = None
            if k in ['datemodified','dateexpired']: #TODO other dates? anything with the str "date" in it?
                dct[k] = datetime.datetime.fromtimestamp(v/1000)
            # Parse datestrings back to python datetimes
            if isinstance(v, str) and re.search('[0-9]*-[0-9]*-[0-9]*T[0-9]*:[0-9]*:', v):
                try:
                    dct[k] = dateutil.parser.parse(v)
                except:
                    pass
        return dct


def _json_loads(ret, file=False):
    """
    Helper function to load JSON return object from the requests lib into python objects.
    Will log exceptions and print extra information when server side exceptions are returned.
    :param ret: Returned object from requests lib (from get or post)
    :param file: If a file is returned, set as true. Will return file, and not try to decode from JSON.
    :return: File is file is true, decoded json if no file
    :except: Raises error in python if server error detected.
    """
    try:
        ret.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(ret.headers)
        raise Exception('Server Replied "' + ret.content.decode("utf-8") + '"') from e
    if file:
        return ret.content
    else:
        return json.loads(ret.content, cls=MyDecoder)


def _parse_locals_to_data_packet(locals_dict):
    """
    Takes the locals object (i.e. function inputs as a dict), maps keys from
    :param locals_dict:
    :return:
    """
    if 'self' in locals_dict:
        locals_dict.pop('self')
    if 'kwargs' in locals_dict:
        kwargs = locals_dict.pop('kwargs')
        locals_dict.update(kwargs)
    return {(param_map[k] if k in param_map else k): v for k, v in locals_dict.items() if v is not None}


class MednickAPI:
    def __init__(self, username, password, server_address='http://saclab.ss.uci.edu:8000'):
        """server_address address constructor"""
        self.server_address = server_address
        self.s = requests.session()
        self.username = username
        self.login_token = None
        self.token, self.usertype = self.login(username, password)
        #headers = {'Content-Type': 'application/json'}
        #self.s.headers.update(headers)
        print('Successfully connected to server at', self.server_address, 'with', self.usertype, 'privileges')

    @staticmethod
    def format_as(ret_data, format='dataframe_single_index'):
        """
        Format a return data from database into useful formats
        :param ret_data: Data to format, as a list of dicts
        :param format: Format to return, one of:
         - nested_dict: standard nested object, i.e. do not apply formatting
         - flat_dict: remove keys in "data", and flatten so each var nested in each key of data is key.var
            example:
                [{'data':{'demographics':{'age':22, 'sex':'M'}}}] --> [{'demographics.age':22, 'demographics.sex':'M'}]
         - dataframe_single_index: If a list with single dict supplied, format as flat_dict,
                                    but convert to pd.Dataframe with keys as indexs.
                                   If a list with multiple dict supplied, format as flat_dict,
                                    but convert to pd.Dataframe with keys as indexs, and stack each dict in list.
         - dataframe_multi_index: TODO
        :return: Data formated as specified
        """
        if format == 'nested_dict':
            return ret_data

        assert isinstance(ret_data, list), """Input of format_as must be list, wrap single objects like [object]"""

        row_cont = []
        row_cont_dict = []
        for row in ret_data:
            if 'data' in row:
                for datatype, datadict in row.pop('data').items():
                    row.update({datatype+'.'+k: v for k, v in datadict.items()})
            row_cont_dict.append(row)
            row_cont.append(pd.Series(row))

        if format == 'flat_dict':
            return row_cont_dict

        df = pd.concat(row_cont, axis=1).T
        if format == 'dataframe_single_index':
            return df
        elif format == 'dataframe_multi_index':
            raise NotImplementedError('TODO')
        else:
            ValueError('Unknown format requested, can be single_index or multi_index')


    @staticmethod
    def extract_var(list_of_dicts, var, raise_on_missing=True):
        """
        Helper function to extract a list of values from a list of dicts
        example:
            extract_var([{'a':1, 'b':11}, {'a':2, 'b':22}]) == [1,2]

        :param list_of_dicts: A list of dictionaries, such as file_info objects
        :param var: The key of the variable to extract
        :param raise_on_missing: If true, raise an error when the var is missing from any of the dicts in the list
        :return: Returns a list of the values of the var for each dict
        """
        if raise_on_missing:
            return [d[var] for d in list_of_dicts]
        else:
            return [d[var] for d in list_of_dicts if var in d]

    @staticmethod
    def sortby(sort_x, by_key, reverse=True):
        """
        Sorts a list of dictionaries (e.g. file_info objects) by a specific key
        :param sort_x: list of dicts/objects to sort
        :param by_key: key to sort by
        :param reverse: if sorting should be reversed
        :return: sorted list of dicts
        """
        return sorted(sort_x, key=itemgetter(by_key), reverse=reverse)

    @staticmethod
    def discard_subsets(object_list):
        """
        From a list of objects, remove the objects that are complete subsets of other objects.
        This does not check data, just ['studyid', 'versionid', 'subjectid','visitid','sessionid'] keys.
        For example, {'studyid':'TEST', 'versionid':1} is a subset of {'studyid':'TEST', 'versionid':1, 'subjectid':1}
        and therefore would be removed from the list
        :param object_list: The list of objects to remove subsets from.
        :return: the object_list with subsets removed
        """
        hierarchical_specifiers = ['studyid', 'versionid', 'subjectid','visitid','sessionid']
        for subset_idx in range(len(object_list) - 1, -1, -1): # iterate backwards so we can drop items but dont bugger the indexes
            candidate_subset = object_list[subset_idx]
            for superset_idx in range(len(object_list) - 1, -1, -1):
                candidate_superset = object_list[superset_idx]
                if subset_idx == superset_idx: # compare int faster than compare dict
                    continue
                if all((k not in candidate_subset) or (candidate_subset[k] is None or candidate_subset[k] == candidate_superset[k])
                       for k in hierarchical_specifiers):
                    del object_list[subset_idx]
                    break
        return object_list

    def login(self, username, password):
        """
        Login to the server. Saves the login token.
        :param username: username to login with (generally an email)
        :param password: password
        :return: a tuple of (success, usertype)
        """
        # TODO
        # self.username = username
        # base_str = self.server_address + '/Login?' + 'Username']=username + '&Password']=password
        # ret = _json_loads(self.s.post(base_str))
        # return ret['token'], ret['usertype']
        # self.login_token = True
        return True, 'admin'

    # File related functions
    def upload_file(self, fileobject, fileformat, filetype, fileversion=None, studyid=None, versionid=None, subjectid=None,
                    visitid=None, sessionid=None):
        """
        Upload a file data to the filestore in the specified location.
        If this is a brand new file, then add, if it exists, then overwrite (i.e. set previous version as inactive).
        Returns file info object
        :param fileobject: The file object, i.e. the return from open('filename.csv','r+')
        :param fileformat: Format of the file, this dictates how the file will be parsed by microservices (pyparse).
            Known parse-able fileformats are currently (others will be ignored by parsing microservices):
            - "sleep_scoring" - sleep scoring files. Currently supports edf, mat (hume), xml (NSRR), and various tabular types
            - "tabular" - any tabular-like data, with column headers and cols for specific subjectid, visitid, etc
            - "eeg" - edf's or other eeg like files. Basically anything the python package MNE can open
            TODO:
            - "actigraphy"
            - "sleep_diaries"
        :param filetype: The "datatype" contained in the file, e.g. "demographics" for demographics related file, etc.
            Can be anything, but preferred filetypes are:
             - "sleep_eeg" for all edf, eeg, timeseries containing sleep eeg
             - "sleep_scoring" for all sleep scoring files (vrmk, mat, csv)
             - "demographics" for all demographics information (age, sex, etc)
             - "counterbalance" for a counterbalance assigning subjects/visits/sessions to conditions
             - "sleep_diary" for all sleep diary information
             - "sleep_features" for any files with spindle, REM, SO events
             - "sleep_stats" for all traditional sleep stats (minutes in REM, latency, etc)
             - Task Names ("WPA", etc)

        :param fileversion: Upload with a specific fileversion. This is usualy managed by the backend. Does this even work? FIXME
        :param studyid: specifies location in database to upload to
        :param versionid: specifies location in database to upload to
        :param subjectid: specifies location in database to upload to
        :param visitid: specifies location in database to upload to
        :param sessionid: specifies location in database to upload to
        :return:
        """
        data_packet = _parse_locals_to_data_packet(locals())
        files = {'fileobject': data_packet.pop('fileobject')}
        ret = self.s.post(url=self.server_address + '/files/upload', data={'data':json.dumps(data_packet, cls=MyEncoder)}, files=files)
        return _json_loads(ret)['ops'][0]

    def update_file_info(self, fid, **kwargs):
        """
        Change the location of a file on the datastore and update its info.
        :param fid: fid of file to update
        :param kwargs: a list of keys and values to update with,
            e.g. update_file_info(fid, studyid='TEST') or update_file_info(fid, {'studyid':'TEST'})
        :return: Unknown, should return updated file info. TODO
        """
        data_packet = _parse_locals_to_data_packet(locals())
        ret = self.s.put(url=self.server_address + '/files/update', data=data_packet)
        return _json_loads(ret) #TODO should return file info

    def update_parsed_status(self, fid, status: bool):
        """
        Change the parsed status of a file. Status is True when parsed or False otherwise
        :param fid: the fid of the file to change
        :param status: The status (True | false) to change to  FIXME as of 1.2.2 this function does not take status, and can only go from false->true
        :return:
        """

        ret = self.s.put(url=self.server_address + '/files/updateParsedStatus', data={'id':fid, 'status':status})
        return _json_loads(ret)

    def delete_file(self, fid, delete_all_versions=False,
                    reactivate_previous=False,
                    remove_associated_data=False):
        """
        Delete a file from the filestore.

        :param fid: the fid of the file to delete
        :param delete_all_versions: If true, delete all version of this file
        :param reactivate_previous: If true, set any old versions as the active version, and trigger a reparse of these files so there data is added to the datastore
        :param remove_associated_data: If true, purge datastore of all data associated with this file
        :return:
        """
        locals_vars = locals().copy()
        name_map = {
            'reactivate_previous': 'previous',
            'delete_all_versions': 'all',
            'remove_associated_data': 'data',
            'fid': 'id'
        }
        locals_vars.pop('self')
        data = {name_map[k]: v for k, v in locals_vars.items()}
        return _json_loads(self.s.post(self.server_address + '/files/expire', data=data))

    def get_files(self, query: str=None, previous_versions: bool=False, format: str='nested_dict', **kwargs):
        """
        Retrieves a list of file info from files in the file store that match the above specifiers.
           When querying, any keys in the file profile may be included, and only matching files for all will be returned.
           Return file info's are sorted by datemodified
        :param query: str version of a query, supports the following operators, see unittest for more examples

            ' and '  - and together operations ('subjectid==1 and studyid==TEST'), i.e. both operands must eval to true
            ' or '  - or together operations ('subjectid==1 or studyid==TEST'), i.e. one operands must eval to true
            ' >= '  - greater than equal to, e.g. 'subjectid >= 1'
            ' > '  - greater than, e.g. 'subjectid > 1'
            ' <= '  - less than equal to, e.g. 'subjectid <= 20'
            ' < ' - less than, e.g. 'subjectid < 20'
            ' not in ' - key not in list, e.g. 'subjectid not in [20,21,22]'
            ' in ' - key in list, e.g. 'subjectid in [20,21,22]'
            ' not ' - key not equal to, e.g. 'subjectid not 20'
            ' != ' same as not
            ' = ' key is value, e.g. subjectid == 20
            ' == ' same as above
            ' & ' and together operations ('subjectid==1 & studyid==TEST') -> ('subjectid==1 and studyid==TEST')
            ' | ' or together operations ('subjectid==1 | studyid==TEST') -> ('subjectid==1 or studyid==TEST')

            All operands can be with or without a single whitespace either side

        :param previous_versions: Whether to return previous, non-active versions of the file also
        :param format: Format to return as, see format_as for possibilities
        :param kwargs: alternative way to query params, e.g. get_files(studyid='TEST') or get_files(kwargs={'studyid':'TEST'})
        :return: a list/dataframe of file_info objects that match
        """
        if query:
            for k, v in query_kwmap.items():
                query = query.replace(k, v)
            print(query)
            if previous_versions:
                ret = _json_loads(self.s.get(self.server_address + '/files?'+query, params={'versions': '1'}))
            else:
                ret = _json_loads(self.s.get(self.server_address + '/files?'+query))
        else:
            params = _parse_locals_to_data_packet(kwargs)
            if previous_versions:
                params.update({'versions': '1'})
            ret = _json_loads(self.s.get(self.server_address + '/files', params=params))

        ret = self.sortby(ret, 'datemodified')

        if len(ret) > 0:
            ret = self.format_as(ret, format)

        return ret

    def get_file_by_fid(self, fid):
        """
        Get the file_info associated with a file id (i.e. the data associated with this id in the filestore)
        :param fid: the fid to get for
        :return: file_info associated with fid
        """
        data_packet = _parse_locals_to_data_packet(locals())
        return _json_loads(self.s.get(url=self.server_address + '/files/info', params={'id': fid}))

    def download_file(self, fid):
        """
        Downloads the binary data for that fid, can be saved to disk as ```open("filename.txt", "wb").write(download_file(fid))```
        :param fid: the fid of the file to download
        :return: binary data of file
        """
        return _json_loads(self.s.get(url=self.server_address + '/files/download', params={'id': fid}), file=True)

    def download_files(self, fids):
        """
        Downloads a number of files from a list of file id's
        :param fids: list of fids to download for
        :return: list of file binaries
        """
        fids_param = '*AND*'.join(fids)
        return _json_loads(self.s.get(url=self.server_address + '/files/downloadmultiple', params={'id': fids_param}))

    def delete_multiple(self, fids):
        """
        Deletes a list of files corresponding to the given fids.
        :param fids: list of fids to delete
        :return: ? TODO
        """
        fids_param = '*AND*'.join(fids)
        return _json_loads(self.s.delete(url=self.server_address + '/files/expiremultiple', data={'id': fids_param}))

    def get_deleted_files(self):
        """
        Retrieves a list of fids for deleted files from the file store, no querying to file these files is done (TODO)
        :return: A huge list of all the file_infos of the files that have been deleted
        """
        return _json_loads(self.s.get(url=self.server_address + '/files/expired'))

    def get_unparsed_files(self, previous_versions=False):
        """
        Return a list of fid's for unparsed files
        :param previous_versions: if true include previous versions. FIXME this would be better as an actual backend option
        :return: file_infos of unparsed files
        """
        files = _json_loads(self.s.get(self.server_address + '/files/unparsed'))
        if not previous_versions:
            files = [file for file in files if file['active']]
        return files

    def get_parsed_files(self, previous_versions=False):
        """
        Return a list of fid's for parsed files
        :param previous_versions: if true include previous versions. FIXME this would be better as an actual backend option
        :return: file_infos of parsed files
        """
        files = _json_loads(self.s.get(self.server_address + '/files/parsed'))
        if not previous_versions:
            files = [file for file in files if file['active']]
        return files

    def get_unique_var_values(self, var, store, **kwargs):
        """
        Get possible values of a hierarchical specifier variable from either data or files store.
        For example, get all filetypes for studyid=TEST from file store:
            get_unique_var_values('filetype', store='files', studyid='TEST') = [demographics, sleep_scoring, memtesta]
        :param var: variable to get unique values for, e.g.
        :param store: store to get data from (data or files)
        :param kwargs: specific place to search at, i.e. subjectid=1, studyid='TEST'
        :return: unique values of that variable, or empty if that variable does not exist
        """
        if store == 'data':
            ret = self.get_data(**kwargs, format='nested_dict')
        elif store == 'files':
            ret = self.get_files(**kwargs, format='nested_dict')
        else:
            raise ValueError('Store Unknown')

        if store == 'data' and var == 'filetype':
            values = []
            for row in ret:
                values += list(row['data'].keys())
        else:
            values = []
            for row in ret:
                try:
                    values.append(row[var])
                except KeyError:
                    values.append(None)
        return list(numpy.unique(values))

    # def get_studyids(self, store="data"):
    #     """Get a list of studies stored in either the data or file store"""
    #     return _json_loads(self.s.get(self.server_address + '/' + store + '/studies'))
    #
    # def get_versionids(self, store, studyid=None, versionid=None, subjectid=None):
    #     """Get the visitids associated with a particular studyid,versionid.
    #     Either from data store (default) or file store"""
    #     params = _parse_locals_to_data_packet(locals())
    #     return _json_loads(self.s.get(self.server_address + '/' + store + '/versions', params=params))
    #
    # def get_subjectids(self, studyid=None, versionid=None):
    #     """Get a list of studies stored in either the data store"""
    #     params = _parse_locals_to_data_packet(locals())
    #     return _json_loads(self.s.get(self.server_address + '/data/subjects', params=params))
    #
    # def get_visitids(self, store, studyid=None, versionid=None, subjectid=None):
    #     """Get the visitids associated with a particular studyid,versionid.
    #     Either from data store (default) or file store"""
    #     params = _parse_locals_to_data_packet(locals())
    #     return _json_loads(self.s.get(self.server_address + '/' + store + '/visits', params=params))
    #
    # def get_sessionids(self, store, studyid=None, versionid=None, subjectid=None, visitid=None):
    #     """Get the sessionids associated with a particular studyid,versionid,visitid.
    #     Either from data store (default) or file store"""
    #     params = _parse_locals_to_data_packet(locals())
    #     return _json_loads(self.s.get(self.server_address + '/' + store + '/sessions', params=params))

    # def get_filetypes(self, store, studyid, versionid=None, subjectid=None, visitid=None, sessionid=None):
    #     """Get the filetypes associated with that level of the hierarchy from the data or file store"""
    #     _locals = locals()
    #     _locals.pop('store')
    #     params = _parse_locals_to_data_packet(_locals)
    #     if store == 'data':
    #         rows = self.get_data(format='nested_dict', **_locals)
    #         file_types = []
    #         for row in rows:
    #             file_types.append(list(row['data'].keys()))
    #         return list(numpy.unique(file_types))
    #
    #     return _json_loads(self.s.get(self.server_address + '/' + store + '/types', params=params))

    # Data Functions
    def upload_data(self, data: dict, studyid, versionid, filetype, fid, subjectid, visitid=None, sessionid=None):
        """
        Upload a data to the datastore in the specified location.
            Specifiers like studyid etc contained in the data object will be extracted and used before any in the function arguments.
            If this is a new location (no data exists), then add, if it exists, merge or overwrite.

        :param data: Single level object of key:values and convertable to json.
        :param studyid: where to put on the data store
        :param versionid: where to put on the data store
        :param filetype: The type of data, see upload_file for standard values
        :param fid: If this data came from a particular file in the server, then add a file id to link back to that file
        :param subjectid: where to put on the data store
        :param visitid: where to put on the data store
        :param sessionid: where to put on the data store
        :return: file_info of uploaded data? TODO change to return the whole data object
        """
        data_packet = _parse_locals_to_data_packet(locals())
        data_packet['sourceid'] = data_packet.pop('id')
        return _json_loads(self.s.post(self.server_address + '/data/upload', data={'data': json.dumps(data_packet, cls=MyEncoder)}))

    def get_data(self, query=None, discard_subsets=True, format='dataframe_single_index', **kwargs):
        """Get all the data in the datastore at the specified location. Return format as specified in args"""
        if query:
            for k, v in query_kwmap.items():
                query = query.replace(k, v)
            rows = _json_loads(self.s.get(self.server_address + '/data?'+query))
        else:
            params = _parse_locals_to_data_packet(kwargs)
            rows = _json_loads(self.s.get(self.server_address + '/data', params=params))

        if discard_subsets:
            rows = self.discard_subsets(rows)

        if len(rows) > 0:
            rows = self.format_as(rows, format=format)

        return rows

    def delete_data(self, **kwargs):
        """
        Delete all data at a particular level of the hierarchy or using a specific dataid given
        the data id of the data object (returned from get_data as "_id")

        :param kwargs: Where to delete data, e.g. delete_data(studyid=TEST), delete all data with SubjectID=TEST,
            if id in kwargs, then delete that specific profile with mongo id==id
        """
        delete_param_name = 'id'
        if delete_param_name in kwargs:
            _json_loads(self.s.delete(self.server_address + '/data/expire', data={delete_param_name: kwargs[delete_param_name]}))
        else:
            rows = self.get_data(**kwargs, format='nested_dict', discard_subsets=False)
            for row in rows:
                self.delete_data(id=row['_id'])

    def get_data_from_single_file(self, filetype, fid, format='dataframe_single_index'):
        """
        Get the data in the datastore associated with a file (i.e. get the data that was extracted from that file on upload)

        :param filetype: The filetype of the data to get #FIXME this could be pulled from the file itself after a query to the filestore?
        :param fid: The file which generated the data you want to get back
        :param format: Return format. See format_as
        :return: the data profiles where the parsing of that file added data
        """
        # TODO filter the returned object by just data that came from fid
        return self.get_data('data.'+filetype+'.sourceid='+fid, format=format)

    def delete_data_from_single_file(self, fid):
        """
        Deletes the data in the datastore associated with a file
        (i.e. get the data that was extracted from that file on upload)
        :param fid:
        :return: TODO ?
        """
        ret = self.s.delete(self.server_address + '/data/expireByFile', data={'id':fid})
        return _json_loads(ret)

    def _delete_all_files(self, password):
        """
        Delete all files on the DB, use with extreme caution. Do you really need to use this?
        :param password: the password to use this program
        :return:
        """
        if password == 'i_am_deleting_everything':
            files = self.get_files()
            print(len(files), 'found, beginning delete of ALL FILES on the server...')
            for file in files:
                print(self.delete_file(file['_id']))
        else:
            print('Cannot delete all files on the server without correct password!')

    def __del__(self):
        """
        Triggers logout. Do we need this? FIXME NotImplemented
        :return:
        """
        # TODO, this should trigger logout.
        pass


if __name__ == '__main__':

    med_api = MednickAPI('http://saclab.ss.uci.edu:8000', 'bdyetton@hotmail.com', 'Pass1234')
    #med_api = MednickAPI('https://postb.in/odTme5YI', 'bdyetton@hotmail.com', 'Pass1234')
    # med_api.delete_all_files(password='nap4life')
    # sys.exit()
    # med_api.delete_data(studyid='TEST')
    # med_api.delete_file(fid='5bb2788f5e52330010f10727')

    # with open('testfiles/scorefile1.mat', 'rb') as uploaded_version:
    #     fid = med_api.upload_file(fileobject=uploaded_version,
    #                               fileformat='scorefile',
    #                               filetype='Yo',
    #                               studyid='TEST',
    #                               subjectid=1,
    #                               versionid=1)
    #
    # sys.exit()


    med_api.upload_data(data={'acc': 0.2, 'std':0.1},
                        studyid='TEST',
                        subjectid=2,
                        versionid=1,
                        visitid=1,
                        filetype='WPA',
                        fid='as5123412345')

    # med_api.upload_data(data={'acc': 0.1, 'std': 0.1},
    #                     studyid='TEST',
    #                     subjectid=2,
    #                     versionid=1,
    #                     visitid=2,
    #                     filetype='WPA',
    #                     fid=fid)
    #
    # med_api.upload_data(data={'age': 22, 'sex': 'M'},
    #                     studyid='TEST',
    #                     subjectid=2,
    #                     versionid=1,
    #                     filetype='demo',
    #                     fid=fid)


    #med_api.delete_data(studyid='TEST')
    #med_api.get_unique_var_values('subjectid', 'files', studyid='TEST')
    #b = med_api.get_data(query='studyid=TEST&data.demo.age>0', format='flat_dict')
    #a = med_api.get_data(studyid='TEST', format='flat_dict')



    some_files = med_api.get_files()
    print('There are', len(some_files), 'files on the server before upload')
    print('There are', len(med_api.get_unparsed_files()), 'unparsed files before upload')
    some_files = med_api.get_deleted_files()
    # print('There are', len(some_files), 'deleted files on the server')
    with open('testfiles/scorefile1.mat', 'rb') as uploaded_version:
        fid = med_api.upload_file(fileobject=uploaded_version,
                                  fileformat='scorefile',
                                  filetype='Yo',
                                  studyid='TEST',
                                  versionid=1)
    print('We uploaded', len(fid), 'files')
    #print(fid)
    some_files = med_api.get_files()
    print('There are', len(some_files), 'files on the server after upload')
    print('There are', len(med_api.get_unparsed_files()), 'unparsed files after upload')
    # print('There are', len(med_api.get_parsed_files()), 'parsed files')
    # print('There are', med_api.get_studyids('files'), 'studies')
    # print('There are', med_api.get_visitids('files', studyid='TEST'), 'visits in TEST')
    print(fid[0])
    print(med_api.get_file_by_fid(fid[0]))
    downloaded_version = med_api.download_file(fid[0])
    with open('testfiles/scorefile1.mat', 'rb') as uploaded_version:
        assert(downloaded_version == uploaded_version.read())
