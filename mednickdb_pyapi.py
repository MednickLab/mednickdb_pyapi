import requests
import json


def append_hierarchical_specifiers(study=None, version=None, visit=None, session=None, filetype=None):
    base_str = ''
    if study:
        base_str = base_str + '&study=' + study
    if version:
        base_str = base_str + '&version=' + version
    if visit:
        base_str = base_str + '&visit=' + visit
    if session:
        base_str = base_str + '&session=' + session
    if filetype:
        base_str = base_str + '&filetype=' + filetype
    return base_str


class MednickAPI:
    def __init__(self, server_address, username, password):
        '''server_address address constructor'''
        self.server_address = server_address
        self.s = requests.Session()
        self.username = username
        self.token, self.usertype = self.login(username, password)
        print('Successfully connected to server at', self.server_address, 'with', self.usertype, 'privileges')

    def login(self, username, password):
        """Login to the server. Returns login token and usertype (privilages)"""
        # FIXME i dont really understand how login works, so this will probably change
        self.username = username
        base_str = self.server_address + '/Login?' + 'Username=' + username + '&Password=' + password
        ret = json.loads(self.s.post(base_str))
        return ret['token'], ret['usertype']

    def get_files(self, study=None, version=None, visit=None, session=None, filetype=None):
        """Retrieves a list of files and their metadata from the file store that match the above specifiers"""
        base_str = self.server_address + '/Files?' + \
                   append_hierarchical_specifiers(study, version, visit, session, filetype)
        return json.loads(self.s.get(base_str).text)

    def get_deleted_files(self, study=None, version=None, visit=None, session=None, filetype=None):
        """Retrieves a list of deleted files and their metadata from the file store that match the above specifiers"""
        base_str = self.server_address + '/DeletedFiles?' + \
                   append_hierarchical_specifiers(study, version, visit, session, filetype)
        return json.loads(self.s.get(base_str).text)

    def download_file(self, id):
        """Downloads a file that matches the id as binary data"""
        # TODO, may need to convert this
        return json.loads(self.s.get(self.server_address + '/DownloadFile?' + 'id=' + id).text)

    def get_file_info(self, id):
        """Get the meta data associated with a file id (i.e. the data associated with this id in the filestore)"""
        return json.loads(self.s.get(self.server_address + '/FileInfo?' + 'id=' + id).text)

    def upload_file(self, file_data, file_name=None, study=None, version=None, visit=None, session=None, filetype=None):
        """Upload a file data to the filestore in the specified location. File_data should be convertable to json.
        If this is a brand new file, then add, if it exists, then overwrite. This shoudl return file id"""
        # TODO test this. Do we need a filename?
        ret = self.s.post(self.server_address + '/FileUpload?' +
                    append_hierarchical_specifiers(study, version, visit, session, filetype) +
                    '&FileName=' + file_name +
                    '&FileData=' + json.dumps(file_data))
        return json.loads(ret)['id']

    def update_file_info(self, id, file_info):
        """Add meta data to a file. file_info should be key:value pairs. already existing keys will be overwritten"""
        self.s.post(self.server_address + '/UpdateFileInfo?' +
                    'id=' + id +
                    '&FileInfo=' + json.dumps(file_info))

    def upload_data(self, data, file_id=None, study=None, version=None, visit=None, session=None, filetype=None):
        """Upload a data to the datastore in the specified location. data should be a single object of key:values and convertable to json.
        If this is a new location (no data exists), then add, if it exists, merge or overwrite.
        If this data came from a particular file in the server, then please add a file id to link back to that file"""
        self.s.post(self.server_address + '/FileUpload?' +
                    append_hierarchical_specifiers(study, version, visit, session, filetype) +
                    '&Data=' + json.dumps(data) +
                    '&FileId=' + id)

    def get_data(self, study=None, version=None, visit=None, session=None, filetype=None):
        """Get all the data in the datastore at the specified location. Return is python dictionary"""
        ret = self.s.post(
            self.server_address + '/Data?' + append_hierarchical_specifiers(study, version, visit, session, filetype))
        return json.loads(ret)

    def get_deleted_data(self, study=None, version=None, visit=None, session=None, filetype=None):
        """Get all the data in the datastore at the specified location. Return is python dictionary"""
        ret = self.s.post(
            self.server_address + '/DeletedData?' + append_hierarchical_specifiers(study, version, visit, session,
                                                                                   filetype))
        return json.loads(ret)

    def get_data_associated_with_file(self, id):
        """Get the data in the datastore associated with a file (i.e. get the data that was extracted from that file on upload)"""
        ret = self.s.post(self.server_address + '/FileData?id=' + id)
        return json.loads(ret)

    def get_filetypes(self, study, version=None, visit=None, session=None, store='File'):
        """Get the filetypes associated with that level of the hierarchy from the data or file store"""
        # TODO implement switch for file or data store
        return json.loads(self.s.get(self.server_address + '/FileTypes' +
                                     append_hierarchical_specifiers(study, version, visit, session)).text)

    def get_sessions(self, study, version, visit, store='Data'):
        """Get the sessions associated with a particular study,version,visit.
        Either from data store (default) or file store"""
        # TODO implement switch for file or data store
        return json.loads(self.s.get(self.server_address + '/Sessions?' +
                                     append_hierarchical_specifiers(study, version, visit)).text)

    def get_studies(self, store="Data"):
        """Get a list of studies stored in either the data or file store"""
        # TODO implement switch for file or data store
        return json.loads(self.s.get(self.server_address + '/Studies').text)

    def get_visits(self, study, version, store='Data'):
        """Get the visits associated with a particular study,version.
        Either from data store (default) or file store"""
        # TODO implement switch for file or data store
        return json.loads(self.s.get(self.server_address + '/Visits?' +
                                     append_hierarchical_specifiers(study, version)).text)

    def update_parsed_status(self, id, status):
        """Change the parsed status of a file. Status is True when parsed or False otherwise"""
        self.s.post(self.server_address + '/UpdateParsedStatus?id=' + id + '&Status=' + status)

    def search_filestore(self, query_string):
        """Return a list of file ids that match the query"""
        ret = self.s.post(self.server_address + '/QueryFile?query=' + query_string)
        return json.loads(ret)

    def search_datastore(self, query_string):
        """Return a data rows (as python objects) that match the query"""
        ret = self.s.post(self.server_address + '/QueryData?query=' + query_string)
        return json.loads(ret)

    def __del__(self):
        # TODO, this should trigger logout??


if __name__ == "__main__":
    Mednick = MednickAPI('http://server_address:8001')

# s = requests.Session()
# print(s.get('http://server_address:8001/Files?study=study4&visit=visit1&session=session1&doctype=screening').text))
