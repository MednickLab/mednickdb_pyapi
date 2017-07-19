import requests, time
from requests import Session
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



session = Session()

# HEAD requests ask for *just* the headers, which is all you need to grab the
# session cookie
session.head('http://sportsbeta.ladbrokes.com/football')

response = session.post(
                        url='http://sportsbeta.ladbrokes.com/view/EventDetailPageComponentController',
                        data={
                        'N': '4294966750',
                        'form-trigger': 'moreId',
                        'moreId': '156#327',
                        'pageType': 'EventClass'
                        },
                        headers={
                        'Referer': 'http://sportsbeta.ladbrokes.com/football'
                        }
                        )

print (response.text)
