import requests, time
from requests import Session

class MednickAPI:
	

	def __init__(self,server_address):
		'''server_address address constructor'''
		self.server_address = server_address
		self.s = requests.Session()
	
	'''Below are all the functions, which could call different endpoints,for more details please read the readme'''
	def files(self, study,visit,session,doctype):
		
		return self.s.get(self.server_address+'/Files?'+'study='+study+'&visit='+visit+'&session='+session+'&doctype='+doctype).text

	def DeletedFiles(self):

		return self.s.get(self.server_address+'/DeletedFiles').text

	def file(self,id):

		return self.s.get(self.server_address+'/File?'+'id='+id).text

	def DownloadFile(self,id):

		return self.s.get(self.server_address+'/DownloadFile?'+'id='+id).text
	
	def TempFiles(self):

		return self.s.get(self.server_address+'/DeletedFiles').text

	def DocumentTypes(self):
		
		return self.s.get(self.server_address+'/DocumentTypes').text	

	def Sessions(self,study,visit):
		
		return self.s.get(self.server_address+'/Sessions?'+'study='+study+'&visit='+visit).text

	def Studies(self):
		
		return self.s.get(self.server_address+'/Studies').text	

	def Visits(self,study):
		
		return self.s.get(self.server_address+'/Visits?'+'study='+study).text

	def updateFile(self,id):
		
		self.s.post(self.server_address+'/UpdateFile/'+id)

	def UploadFile(self):
		
		self.s.post(self.server_address+'/FileUpload')

	def NewFileRecord(self):

		self.s.post(self.server_address+'/NewFileRecord')

	def TaskData(self):

		self.s.post(self.server_address+'/TaskData')

	def Screenings(self):

		self.s.post(self.server_address+'/Screenings')

	def UpdateParsedStatus(self):
		
		self.s.post(self.server_address+'/UpdateParsedStatus')
  
if __name__ == "__main__":
	Mednick = MednickAPI('http://server_address:8001')
	print(Mednick.files('study4','visit1','session1','screening'))
	print ('\n')
	print(Mednick.DeletedFiles())
	print ('\n')
	print(Mednick.file('5977072b60950c2778cd2d33'))
	print ('\n')
	print(Mednick.Sessions('study1','visit1'))
	print ('\n')
	#s = requests.Session()
	#print(s.get('http://server_address:8001/Files?study=study4&visit=visit1&session=session1&doctype=screening').text))
