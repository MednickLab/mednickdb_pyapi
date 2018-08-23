import requests
import json
import datetime
import numpy
import re
import dateutil.parser
import _io
import base64

# TODO use kwargs instead of a long list of specifiers

param_map = {
    'fid':'id'
}


class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        if isinstance(obj, numpy.integer):  # TODO test
            return int(obj)
        elif isinstance(obj, numpy.floating):
            return float(obj)
        elif isinstance(obj, numpy.ndarray):
            return obj.tolist()
        else:
            return super(MyEncoder, self).default(obj)


class MyDecoder(json.JSONDecoder):  # TODO. test this!
    def __init__(self, *args, **kargs):
        json.JSONDecoder.__init__(self, object_hook=self.parser,
                                  *args, **kargs)

    def parser(self, dct):
        for k, v in dct.items():
            if isinstance(v, str) and v == '':
                dct[k] = None
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
        raise Exception('Server Replied "' + ret.content.decode("utf-8") + '"') from e
    if file:
        return ret.content
    else:
        return json.loads(ret.content, cls=MyDecoder)


def _parse_locals_to_data_packet(locals_dict):
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
        print('Successfully connected to server at', self.server_address, 'with', self.usertype, 'privileges')

    @staticmethod
    def extract_var(list_of_dicts, var):
        return [d[var] for d in list_of_dicts if var in d]

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
    def upload_file(self, fileobject, fileformat, filetype, studyid=None, versionid=None, subjectid=None,
                    visitid=None, sessionid=None):
        """Upload a file data to the filestore in the specified location. File_data should be convertable to json.
        If this is a brand new file, then add, if it exists, then overwrite. This shoudl return file id"""
        data_packet = _parse_locals_to_data_packet(locals())
        files = {'fileobject': data_packet.pop('fileobject')}
        ret = self.s.post(url=self.server_address + '/files/upload', data=data_packet, files=files)
        fids = _json_loads(ret)['insertedIds']
        return [fid for _, fid in sorted(fids.items())]

    def update_file_info(self, fid, fileformat, filetype, studyid=None):
        """Unsure why this is useful. TODO ask Juan"""
        data_packet = _parse_locals_to_data_packet(locals())
        ret = self.s.put(url=self.server_address + '/files/update', data=data_packet)
        return _json_loads(ret)

    def delete_file(self, fid):
        """Delete a file from the filestore"""
        return _json_loads(self.s.delete(self.server_address + '/files/expire', data={'id': fid}))

    def get_files(self, studyid=None, versionid=None, subjectid=None, visitid=None, sessionid=None, filetype=None, active_only=True):
        """Retrieves a list of files ids for files in the file store that match the above specifiers"""
        data_packet = _parse_locals_to_data_packet(locals())
        active_only = data_packet.pop('active_only')
        files = _json_loads(self.s.get(url=self.server_address + '/files', params=data_packet))
        if active_only:
            files = [file for file in files if file['active']]
        return files

    def get_single_file(self, fid):
        """Get the meta data associated with a file id (i.e. the data associated with this id in the filestore)"""
        data_packet = _parse_locals_to_data_packet(locals())
        return _json_loads(self.s.get(url=self.server_address + '/files/info', params={'id': fid}))

    def download_file(self, fid):
        """Downloads a file that matches the file id as binary data"""
        # TODO, may need to convert this
        return _json_loads(self.s.get(url=self.server_address + '/files/download', params={'id': fid}), file=True)

    def download_files(self, fids):
        """Downloads a number of files from a list of file id's"""
        fids_param = '*AND*'.join(fids)
        return _json_loads(self.s.get(url=self.server_address + '/files/downloadmultiple', params={'id': fids_param}))

    def delete_multiple(self, fids):
        """Deletes a list of files coresponding to the given fileids"""
        fids_param = '*AND*'.join(fids)
        return _json_loads(self.s.get(url=self.server_address + '/files/expiremultiple', params={'id': fids_param}))

    def get_deleted_files(self):
        """Retrieves a list of fids for deleted files from the file store that match the above specifiers"""
        return _json_loads(self.s.get(url=self.server_address + '/files/expired'))

    def get_unparsed_files(self, active_only=True):
        """Return a list of fid's for unparsed files"""
        files = _json_loads(self.s.get(self.server_address + '/files/unparsed'))
        if active_only:
            files = [file for file in files if file['active']]
        return files

    def get_parsed_files(self):
        """Return a list of fid's for unparsed files"""
        return _json_loads(self.s.get(self.server_address + '/files/parsed'))

    def get_studyids(self, store="data"):
        """Get a list of studies stored in either the data or file store"""
        return _json_loads(self.s.get(self.server_address + '/' + store + '/studies'))

    def get_versionids(self, store, studyid=None, versionid=None, subjectid=None):
        """Get the visitids associated with a particular studyid,versionid.
        Either from data store (default) or file store"""
        params = _parse_locals_to_data_packet(locals())
        return _json_loads(self.s.get(self.server_address + '/' + store + '/versions', params=params))

    def get_subjectids(self, studyid=None, versionid=None):
        """Get a list of studies stored in either the data store"""
        params = _parse_locals_to_data_packet(locals())
        return _json_loads(self.s.get(self.server_address + '/data/subjects', params=params))

    def get_visitids(self, store, studyid=None, versionid=None, subjectid=None):
        """Get the visitids associated with a particular studyid,versionid.
        Either from data store (default) or file store"""
        params = _parse_locals_to_data_packet(locals())
        return _json_loads(self.s.get(self.server_address + '/' + store + '/visits', params=params))

    def get_sessionids(self, store, studyid=None, versionid=None, subjectid=None, visitid=None):
        """Get the sessionids associated with a particular studyid,versionid,visitid.
        Either from data store (default) or file store"""
        params = _parse_locals_to_data_packet(locals())
        return _json_loads(self.s.get(self.server_address + '/' + store + '/sessions', params=params))

    def get_filetypes(self, store, studyid, versionid=None, subjectid=None, visitid=None, sessionid=None):
        """Get the filetypes associated with that level of the hierarchy from the data or file store"""
        # TODO implement switch for file or data store
        params = _parse_locals_to_data_packet(locals())
        return _json_loads(self.s.get(self.server_address + '/' + store + '/types', params=params))

    # Data Functions
    def upload_data(self, data, studyid, versionid, filetype, fid, subjectid, visitid=None, sessionid=None):
        """Upload a data to the datastore in the specified location. data should be a single object of key:values and convertable to json.
        Specifiers like studyid etc contained in the data object will be extracted and used before any in the function arguments.
        If this is a new location (no data exists), then add, if it exists, merge or overwrite.
        If this data came from a particular file in the server, then please add a file id to link back to that file"""
        data_packet = _parse_locals_to_data_packet(locals())
        data_packet['data'] = json.dumps(data_packet['data'], cls=MyEncoder)
        data_packet['sourceid'] = data_packet.pop('id')
        return _json_loads(self.s.post(self.server_address + '/data/upload', data=data_packet))

    def get_data(self, studyid=None, versionid=None, subjectid=None, visitid=None, sessionid=None, filetype=None):
        """Get all the data in the datastore at the specified location. Return is python dictionary"""
        params = _parse_locals_to_data_packet(locals())
        return _json_loads(self.s.get(self.server_address + '/data', params=params))

    def delete_data(self, studyid, versionid=None, subjectid=None, visitid=None, sessionid=None, filetype=None):
        """Delete all data at a particular level of the hierarchy"""
        params = _parse_locals_to_data_packet(locals())
        return _json_loads(self.s.get(self.server_address + '/data/expired', params=params))

    def get_data_from_single_file(self, fid):
        """ Get the data in the datastore associated with a file
        (i.e. get the data that was extracted from that file on upload)"""
        raise NotImplementedError()
        # ret = self.s.post(self.server_address + '/FileData?id='+fid)
        # return _json_loads(ret, cls=MyDecoder)

    def delete_data_from_single_file(self, fid):
        """ Deletes the data in the datastore associated with a file
        (i.e. get the data that was extracted from that file on upload)"""
        raise NotImplementedError()
        # ret = self.s.post(self.server_address + '/FileData?id='+fid)
        # return _json_loads(ret, cls=MyDecoder)

    def update_parsed_status(self, fid, status):
        """Change the parsed status of a file. Status is True when parsed or False otherwise"""
        raise NotImplementedError()
        # self.s.get(self.server_address + '/updateParsedStatus?id']=id + '&Status='+status)

    def search_filestore(self, query_string):
        """Return a list of file ids that match the query"""
        raise NotImplementedError()
        # ret = self.s.get(self.server_address + '/getFiles?query']=query_string)
        # return _json_loads(ret)

    def search_datastore(self, query_string):
        """Return a data rows (as python objects) that match the query"""
        raise NotImplementedError()
        # ret = self.s.get(self.server_address + '/getProfiles?query']=query_string)
        # return _json_loads(ret, cls=MyDecoder)

    def delete_all_files(self, password):
        if password == 'kiwi':
            files = self.get_files()
            print(len(files),'found, beginning delete...')
            for file in files:
                print(self.delete_file(file['_id']))
        else:
            print('Cannot delete all files on the server without correct password!')

    def __del__(self):
        # TODO, this should trigger logout??
        pass




if __name__ == '__main__':
    med_api = MednickAPI('http://saclab.ss.uci.edu:8000', 'bdyetton@hotmail.com', 'Pass1234')
    #med_api.delete_all_files(password='kiwi')
    print(med_api.get_data(studyid='TEST'))

    some_files = med_api.get_files()
    med_api.upload_data(data={'test': 'value1'}, subjectid=1, studyid='TEST', versionid=1, filetype='test', fid=some_files[0]['_id'])

    print('There are', len(some_files), 'files on the server before upload')
    print('There are', len(med_api.get_unparsed_files()), 'unparsed files before upload')
    some_files = med_api.get_deleted_files()
    # print('There are', len(some_files), 'deleted files on the server')
    with open('testfiles/scorefile1.mat', 'rb') as uploaded_version:
        fid = med_api.upload_file(fileobject=uploaded_version,
                                  fileformat='scorefile',
                                  filetype='Yo',
                                  studyid='TEST4',
                                  versionid=str(1))
    print('We uploaded', len(fid), 'files')
    #print(fid)
    some_files = med_api.get_files()
    print('There are', len(some_files), 'files on the server after upload')
    print('There are', len(med_api.get_unparsed_files()), 'unparsed files after upload')
    # print('There are', len(med_api.get_parsed_files()), 'parsed files')
    # print('There are', med_api.get_studyids('files'), 'studies')
    # print('There are', med_api.get_visitids('files', studyid='TEST'), 'visits in TEST')
    print(fid[0])
    print(med_api.get_single_file(fid[0]))
    downloaded_version = med_api.download_file(fid[0])
    with open('testfiles/scorefile1.mat', 'rb') as uploaded_version:
        assert(downloaded_version == uploaded_version.read())
