import requests, time
from requests import Session

class MednickAPI:
	

	def __init__(self,localhost):
		'''localhost address constructor'''
		self.localhost = localhost
	
	'''Below are all the functions, which could call different endpoints'''
	def files(self, study,visit,session,doctype):
	
		s = requests.Session()
		return s.get(self.localhost+'/Files?'+'study='+study+'&visit='+visit+'&session='+session+'&doctype='+doctype).text

	def DeletedFiles(self):

		s = requests.Session()
		return s.get(self.localhost+'/DeletedFiles').text

	def file(self,id):

		s = requests.Session()
		return s.get(self.localhost+'/File?'+'id='+id).text

	def DownloadFile(self,id):

		s = requests.Session()
		return s.get(self.localhost+'/DownloadFile?'+'id='+id).text
	
	def TempFiles(self):
		s = requests.Session()
		return s.get(self.localhost+'/DeletedFiles').text

	def DocumentTypes(self):
		s = requests.Session()
		return s.get(self.localhost+'/DocumentTypes').text	

	def Sessions(self,study,visit):
		s = requests.Session()
		return s.get(self.localhost+'/Sessions?'+'study='+study+'&visit='+visit).text

	def Studies(self):
		s = requests.Session()
		return s.get(self.localhost+'/Studies').text	

	def Visits(self,study):
		s = requests.Session()
		return s.get(self.localhost+'/Visits?'+'study='+study).text

	def updateFile(self,id):
		s = requests.Session()
		s.post(self.localhost+'/UpdateFile/'+id)

	def UploadFile(self):
		s = requests.Session()
		s.post(self.localhost+'/FileUpload')

	def NewFileRecord(self):

		s = requests.Session()
		s.post(self.localhost+'/NewFileRecord')

	def TaskData(self):

		s = requests.Session()
		s.post(self.localhost+'/TaskData')

	def Screenings(self):

		s = requests.Session()
		s.post(self.localhost+'/Screenings')

	def UpdateParsedStatus(self):

		s = requests.Session()
		s.post(self.localhost+'/UpdateParsedStatus')
  
if __name__ == "__main__":
	Mednick = MednickAPI('http://localhost:8001')
	print(Mednick.files('study4','visit1','session1','screening'))
	print ('\n')
	print(Mednick.DeletedFiles())
	print ('\n')
	print(Mednick.file('5977072b60950c2778cd2d33'))
	print ('\n')
	print(Mednick.Sessions('study1','visit1'))
	print ('\n')
	#s = requests.Session()
	#print(s.get('http://localhost:8001/Files?study=study4&visit=visit1&session=session1&doctype=screening').text))
