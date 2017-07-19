import requests, time
r = requests.post('https://requestb.in/1lpgxkz1', data={"ts":time.time()})
print (r.status_code)
print (r.content)

s = requests.Session()
print (s.get('http://httpbin.org/ip').text)
print (s.get('http://httpbin.org/get').json)
print (s.post('http://httpbin.org/post',{'key':'value'},headers={'user-agent':'Nelly'}).text)
print (s.get('http://httpbin.org/status/404').status_code)
print (s.get('http://httpbin.org/html').text)
print (s.get('http://httpbin.org/deny').text)
