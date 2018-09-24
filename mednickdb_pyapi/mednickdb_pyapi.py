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

# TODO use kwargs instead of a long list of specifiers

param_map = {
    'fid':'id',
}

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
    def default(self, obj):
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return time.mktime(obj.timetuple())*1000  # Convert to ms
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
        json.JSONDecoder.__init__(self, object_hook=self.parser,
                                  *args, **kargs)

    def parser(self, dct):
        for k, v in dct.items():
            if isinstance(v, str) and v == '':
                dct[k] = None
            if k in ['datemodified','dateexpired']:
                dct[k] = datetime.datetime.fromtimestamp(v/1000)
            # Parse datestrings back to python datetimes
            if isinstance(v, str) and re.search('[0-9]*-[0-9]*-[0-9]*T[0-9]*:[0-9]*:', v):
                try:
                    dct[k] = dateutil.parser.parse(v)
                except:
                    pass
        return dct


def _json_loads(ret, file=False):
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
    if 'self' in locals_dict:
        locals_dict.pop('self')
    if 'kwargs' in locals_dict:
        kwargs = locals_dict.pop('kwargs')
        locals_dict.update(kwargs)
    return {(param_map[k] if k in param_map else k): v for k, v in locals_dict.items()}


class MednickAPI:
    def __init__(self, server_address, username, password):
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
        if format == 'nested_dict':
            return ret_data

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
        if raise_on_missing:
            return [d[var] for d in list_of_dicts]
        else:
            return [d[var] for d in list_of_dicts if var in d]

    @staticmethod
    def sortby(sort_x, by_key, reverse=True):
        return sorted(sort_x, key=itemgetter(by_key), reverse=reverse)

    def login(self, username, password):
        """Login to the server. Returns login token and usertype (privilages)"""
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
        """Upload a file data to the filestore in the specified location. File_data should be convertable to json.
        If this is a brand new file, then add, if it exists, then overwrite. This shoudl return file id"""
        data_packet = _parse_locals_to_data_packet(locals())
        files = {'fileobject': data_packet.pop('fileobject')}
        ret = self.s.post(url=self.server_address + '/files/upload', data={'data':json.dumps(data_packet, cls=MyEncoder)}, files=files)
        fids = _json_loads(ret)['insertedIds']
        inserted_fids = [fid for _, fid in sorted(fids.items())]
        if isinstance(fileobject, list):
            return inserted_fids
        else:
            return inserted_fids[0]

    def update_file_info(self, fid, fileformat, filetype, studyid=None):
        """Change the location of a file on the datastore and update its info"""
        data_packet = _parse_locals_to_data_packet(locals())
        ret = self.s.put(url=self.server_address + '/files/update', data=data_packet)
        return _json_loads(ret)

    def update_parsed_status(self, fid, status):
        """Change the parsed status of a file. Status is True when parsed or False otherwise"""
        data_packet = _parse_locals_to_data_packet(locals())
        data_packet.pop('status')  # FIXME as of 1.2.2 this function does not take status
        ret = self.s.put(url=self.server_address + '/files/update', data=data_packet)
        return _json_loads(ret)

    def delete_file(self, fid, delete_all_versions=False,
                    reactivate_previous=False,
                    remove_associated_data=False):
        """Delete a file from the filestore.
            Args:
                delete_all_versions: If true, delete all version of this file
                reactivate_previous: If true, set any old versions as the active version, and trigger a reparse of these files so there data is added to the datastore
                remove_associated_data: If true, purge datastore of all data associated with this file
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
        return _json_loads(self.s.delete(self.server_address + '/files/expire', data=data))

    def get_files(self, query=None, previous_versions=False, format='dict_list', **kwargs):
        """Retrieves a list of files ids for files in the file store that match the above specifiers.
            Any keys in the file profile may be included, and only matching files for all will be returned.
            Return files are sorted by datemodified.
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

        if 'dataframe' in format:
            ret = self.format_as(ret)

        return ret

    def get_file_by_fid(self, fid):
        """Get the meta data associated with a file id (i.e. the data associated with this id in the filestore)"""
        data_packet = _parse_locals_to_data_packet(locals())
        return _json_loads(self.s.get(url=self.server_address + '/files/info', params={'id': fid}))

    def download_file(self, fid):
        """Downloads a file that matches the file id as binary data"""
        return _json_loads(self.s.get(url=self.server_address + '/files/download', params={'id': fid}), file=True)

    def download_files(self, fids):
        """Downloads a number of files from a list of file id's"""
        fids_param = '*AND*'.join(fids)
        return _json_loads(self.s.get(url=self.server_address + '/files/downloadmultiple', params={'id': fids_param}))

    def delete_multiple(self, fids):
        """Deletes a list of files coresponding to the given fileids. Not Tested TODO"""
        fids_param = '*AND*'.join(fids)
        return _json_loads(self.s.delete(url=self.server_address + '/files/expiremultiple', data={'id': fids_param}))

    def get_deleted_files(self):
        """Retrieves a list of fids for deleted files from the file store that match the above specifiers"""
        return _json_loads(self.s.get(url=self.server_address + '/files/expired'))

    def get_unparsed_files(self, previous_versions=False):
        """Return a list of fid's for unparsed files"""
        files = _json_loads(self.s.get(self.server_address + '/files/unparsed'))
        if not previous_versions:
            files = [file for file in files if file['active']]
        return files

    def get_parsed_files(self):
        """Return a list of fid's for unparsed files"""
        return _json_loads(self.s.get(self.server_address + '/files/parsed'))

    def get_unique_var_values(self, var, store, **kwargs):
        """Get possible values of a variable from either data or files store.
        For example, get all filetypes for studyid=TEST from file store:
            get_unique_var_values('filetype', store='files', studyid='TEST')
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
                values.append(list(row['data'].keys()))
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
    def upload_data(self, data, studyid, versionid, filetype, fid, subjectid, visitid=None, sessionid=None):
        """Upload a data to the datastore in the specified location. data should be a single object of key:values and convertable to json.
        Specifiers like studyid etc contained in the data object will be extracted and used before any in the function arguments.
        If this is a new location (no data exists), then add, if it exists, merge or overwrite.
        If this data came from a particular file in the server, then please add a file id to link back to that file"""
        data_packet = _parse_locals_to_data_packet(locals())
        data_packet['sourceid'] = data_packet.pop('id')
        return _json_loads(self.s.post(self.server_address + '/data/upload', data={'data': json.dumps(data_packet, cls=MyEncoder)}))

    def get_data(self, query=None, discard_subsets=True, format='dataframe_single_index', **kwargs):
        """Get all the data in the datastore at the specified location. Return is python dictionary"""
        if query:
            for k, v in query_kwmap.items():
                query = query.replace(k, v)
            rows = _json_loads(self.s.get(self.server_address + '/data?'+query))
        else:
            params = _parse_locals_to_data_packet(kwargs)
            rows = _json_loads(self.s.get(self.server_address + '/data', params=params))

        if discard_subsets:
            rows = self.discard_subsets(rows)

        rows = self.format_as(rows, format=format)

        return rows

    def delete_data(self, **kwargs):
        """Delete all data at a particular level of the hierarchy or using a specific dataid given
        the data id of the data object (returned from get_data as "_id")"""
        if 'dataid' in kwargs:
            return _json_loads(self.s.delete(self.server_address + '/data/expire', data={'sourceid':kwargs['dataid']}))
        else:
            rows = self.get_data(**kwargs, format='nested_dict')
            for row in rows:
                self.delete_data(dataid=row['_id'])

    def get_data_from_single_file(self, filetype, fid, format='dataframe_single_index'):
        """ Get the data in the datastore associated with a file
        (i.e. get the data that was extracted from that file on upload)"""
        return self.get_data('data.'+filetype+'.sourceid='+fid, format=format)

    def delete_data_from_single_file(self, fid):
        """ Deletes the data in the datastore associated with a file
        (i.e. get the data that was extracted from that file on upload)"""
        ret = self.s.delete(self.server_address + '/data/expireByFile', data={'id':fid})
        return _json_loads(ret)

    def delete_all_files(self, password):
        if password == 'nap4life':
            files = self.get_files()
            print(len(files), 'found, beginning delete...')
            for file in files:
                print(self.delete_file(file['_id']))
        else:
            print('Cannot delete all files on the server without correct password!')

    def discard_subsets(self, ret_data):
        hierarchical_specifiers = ['studyid','versionid', 'subjectid','visitid','sessionid']
        for subset_idx in range(len(ret_data)-1, -1, -1): # iterate backwards so we can drop items but dont bugger the indexes
            candidate_subset = ret_data[subset_idx]
            for superset_idx in range(len(ret_data)-1, -1, -1):
                candidate_superset = ret_data[superset_idx]
                if subset_idx == superset_idx: # compare int faster than compare dict
                    continue
                if all(candidate_subset[k] == candidate_superset[k] or candidate_subset[k] is None
                       for k in hierarchical_specifiers):
                    del ret_data[subset_idx]
                    break
        return ret_data

    def __del__(self):
        # TODO, this should trigger logout.
        pass


if __name__ == '__main__':
    med_api = MednickAPI('http://saclab.ss.uci.edu:8000', 'bdyetton@hotmail.com', 'Pass1234')
    #med_api.delete_all_files(password='kiwi')

    # med_api.upload_data(data={'acc': 0.2, 'std':0.1},
    #                     studyid='TEST',
    #                     subjectid=2,
    #                     versionid=1,
    #                     visitid=1,
    #                     filetype='WPA',
    #                     fid='dasdasd')
    #
    # med_api.upload_data(data={'acc': 0.1, 'std': 0.1},
    #                     studyid='TEST',
    #                     subjectid=2,
    #                     versionid=1,
    #                     visitid=2,
    #                     filetype='WPA',
    #                     fid='dasdasd')
    #
    # med_api.upload_data(data={'age': 22, 'sex': 'M'},
    #                     studyid='TEST',
    #                     subjectid=2,
    #                     versionid=1,
    #                     filetype='demo',
    #                     fid='dasdasd')
    med_api.delete_data(studyid='TEST')
    med_api.get_unique_var_values('subjectid', 'files', studyid='TEST')
    b = med_api.get_data(query='studyid=TEST&data.demo.age>0', format='flat_dict')
    a = med_api.get_data(studyid='TEST', format='flat_dict')


    sys.exit()
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
