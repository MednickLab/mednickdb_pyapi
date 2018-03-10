import requests
import json
import datetime
import numpy
import re
import dateutil.parser


class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        if isinstance(obj, numpy.integer):
            return int(obj)
        elif isinstance(obj, numpy.floating):
            return float(obj)
        elif isinstance(obj, numpy.ndarray):
            return obj.tolist()
        else:
            return super(MyEncoder, self).default(obj)


class MyDecoder(json.JSONDecoder): #TODO. test this!
    def __init__(self, *args, **kargs):
        json.JSONDecoder.__init__(self, object_hook=self.datetime_parser,
                             *args, **kargs)

    def datetime_parser(self, dct):
        for k, v in dct.items():
            if isinstance(v, str) and re.search('[0-9]*-[0-9]*-[0-9]*T[0-9]*:[0-9]*:',v):
                try:
                    dct[k] = dateutil.parser.parse(v)
                except:
                    pass
        return dct


def append_hierarchical_specifiers(studyid=None, versionid=None, subjectid=None, visitid=None, sessionid=None, filetype=None):
    base_str = ''
    if studyid:
        base_str = base_str + '&studyid=' + studyid
    if versionid:
        base_str = base_str + '&versionid=' + versionid
    if subjectid:
        base_str = base_str + '&subjectid=' + subjectid
    if visitid:
        base_str = base_str + '&visitid=' + visitid
    if sessionid:
        base_str = base_str + '&sessionid=' + sessionid
    if filetype:
        base_str = base_str + '&filetype=' + filetype
    return base_str


def extract_hierarchical_specifiers(obj):
    specifier_keys = ['studyid', 'versionid', 'subjectid', 'visitid', 'sessionid', 'filetype']
    return {key: value for key, value in obj if key in specifier_keys}


class MednickAPI:
    def __init__(self, server_address, username, password):
        '''server_address address constructor'''
        self.server_address = server_address
        self.s = requests.session()
        self.username = username
        self.token, self.usertype = self.login(username, password)
        print('Successfully connected to server at', self.server_address, 'with', self.usertype, 'privileges')

    def login(self, username, password):
        """Login to the server. Returns login token and usertype (privilages)"""
        # FIXME i dont really understand how login works, so this will probably change
        # self.username = username
        # base_str = self.server_address + '/Login?' + 'Username=' + username + '&Password=' + password
        # ret = json.loads(self.s.post(base_str))
        # return ret['token'], ret['usertype']
        return True, 'admin'

    def get_file_ids(self, studyid=None, versionid=None, subjectid=None, visitid=None, sessionid=None, filetype=None):
        """Retrieves a list of files ids for files in the file store that match the above specifiers"""
        base_str = self.server_address + '/Files?' + \
                   append_hierarchical_specifiers(studyid, versionid, subjectid, visitid, sessionid, filetype)
        return json.loads(self.s.get(base_str).text)

    def get_deleted_file_ids(self, studyid=None, versionid=None, subjectid=None, visitid=None, sessionid=None, filetype=None):
        """Retrieves a list of fids for deleted files from the file store that match the above specifiers"""
        base_str = self.server_address + '/DeletedFiles?' + \
                   append_hierarchical_specifiers(studyid, versionid, subjectid, visitid, sessionid, filetype)
        return json.loads(self.s.get(base_str).text)

    def download_file(self, fid):
        """Downloads a file that matches the file id as binary data"""
        # TODO, may need to convert this
        return json.loads(self.s.get(self.server_address + '/DownloadFile?' + 'id=' + fid).text)

    def get_file_info(self, fid):
        """Get the meta data associated with a file id (i.e. the data associated with this id in the filestore)"""
        return json.loads(self.s.get(self.server_address + '/FileInfo?' + 'id=' + fid).text, cls=MyDecoder)

    def upload_file(self, file_data, fileformat, filename=None, studyid=None, versionid=None, subjectid=None, visitid=None, sessionid=None, filetype=None):
        """Upload a file data to the filestore in the specified location. File_data should be convertable to json.
        If this is a brand new file, then add, if it exists, then overwrite. This shoudl return file id"""
        # TODO test this. Do we need a filename?
        ret = self.s.post(self.server_address + '/FileUpload?' +
                    append_hierarchical_specifiers(studyid, versionid, subjectid, visitid, sessionid, filetype) +
                    '&FileName=' + filename +
                    '&FileFormat'+ fileformat +
                    '&FileData=' + json.dumps(file_data, cls=MyEncoder))
        return json.loads(ret)['id']

    def update_file_info(self, id, file_info):
        """Add meta data to a file. file_info should be key:value pairs. already existing keys will be overwritten"""
        self.s.post(self.server_address + '/UpdateFileInfo?' +
                    'id=' + id +
                    '&FileInfo=' + json.dumps(file_info, cls=MyEncoder))

    def upload_data(self, data, fid=None, studyid=None, versionid=None, subjectid=None, visitid=None, sessionid=None, filetype=None):
        """Upload a data to the datastore in the specified location. data should be a single object of key:values and convertable to json.
        Specifiers like studyid etc contained in the data object will be extracted and used before any in the function arguments.
        If this is a new location (no data exists), then add, if it exists, merge or overwrite.
        If this data came from a particular file in the server, then please add a file id to link back to that file"""
        specifiers = {'studyid':studyid,
                      'versionid':versionid,
                      'subjectid':subjectid,
                      'visitid':visitid,
                      'sessionid':sessionid,
                      'filetype':filetype}
        specifiers.update(extract_hierarchical_specifiers(data))
        self.s.post(self.server_address + '/DataUpload?' +
                    append_hierarchical_specifiers(**specifiers) +
                    '&Data=' + json.dumps(data, cls=MyEncoder) +
                    '&FileId=' + fid)

    def get_data(self, studyid=None, versionid=None, subjectid=None, visitid=None, sessionid=None, filetype=None):
        """Get all the data in the datastore at the specified location. Return is python dictionary"""
        ret = self.s.post(
            self.server_address + '/Data?' + append_hierarchical_specifiers(studyid, versionid, subjectid, visitid, sessionid, filetype))
        return json.loads(ret, cls=MyDecoder)

    def get_deleted_data(self, studyid=None, versionid=None, subjectid=None, visitid=None, sessionid=None, filetype=None):
        """Get all the data in the datastore at the specified location. Return is python dictionary"""
        ret = self.s.post(
            self.server_address + '/DeletedData?' + append_hierarchical_specifiers(studyid, versionid, subjectid, visitid, sessionid,
                                                                                   filetype))
        return json.loads(ret, cls=MyDecoder)

    def get_data_associated_with_file(self, id):
        """Get the data in the datastore associated with a file (i.e. get the data that was extracted from that file on upload)"""
        ret = self.s.post(self.server_address + '/FileData?id=' + id)
        return json.loads(ret, cls=MyDecoder)

    def get_filetypes(self, studyid, versionid=None, subjectid=None, visitid=None, sessionid=None, store='File'):
        """Get the filetypes associated with that level of the hierarchy from the data or file store"""
        # TODO implement switch for file or data store
        return json.loads(self.s.get(self.server_address + '/FileTypes' +
                                     append_hierarchical_specifiers(studyid, versionid, subjectid, visitid, sessionid)).text)

    def get_sessionids(self, studyid, versionid, subjectid, visitid, store='Data'):
        """Get the sessionids associated with a particular studyid,versionid,visitid.
        Either from data store (default) or file store"""
        # TODO implement switch for file or data store
        return json.loads(self.s.get(self.server_address + '/Sessionids?' +
                                     append_hierarchical_specifiers(studyid, versionid, subjectid, visitid)).text)

    def get_studyids(self, store="Data"):
        """Get a list of studies stored in either the data or file store"""
        # TODO implement switch for file or data store
        return json.loads(self.s.get(self.server_address + '/Studyids?').text)

    def get_visitids(self, studyid, versionid, subjectid, store='Data'):
        """Get the visitids associated with a particular studyid,versionid.
        Either from data store (default) or file store"""
        # TODO implement switch for file or data store
        return json.loads(self.s.get(self.server_address + '/Visitids?' +
                                     append_hierarchical_specifiers(studyid, versionid, subjectid)).text)

    def get_versionids(self, studyid, versionid, subjectid, store='Data'):
        """Get the visitids associated with a particular studyid,versionid.
        Either from data store (default) or file store"""
        # TODO implement switch for file or data store
        return json.loads(self.s.get(self.server_address + '/Versionids?' +
                                     append_hierarchical_specifiers(studyid, versionid, subjectid)).text)

    def update_parsed_status(self, id, status):
        """Change the parsed status of a file. Status is True when parsed or False otherwise"""
        self.s.post(self.server_address + '/UpdateParsedStatus?id=' + id + '&Status=' + status)

    def get_unparsed_files(self):
        """Return a list of fid's for unparsed files"""
        return json.loads(self.s.post(self.server_address + '/UnparsedFiles?'))

    def search_filestore(self, query_string):
        """Return a list of file ids that match the query"""
        ret = self.s.post(self.server_address + '/QueryFile?query=' + query_string)
        return json.loads(ret)

    def search_datastore(self, query_string):
        """Return a data rows (as python objects) that match the query"""
        ret = self.s.post(self.server_address + '/QueryData?query=' + query_string)
        return json.loads(ret, cls=MyDecoder)

    def __del__(self):
        # TODO, this should trigger logout??
        pass

