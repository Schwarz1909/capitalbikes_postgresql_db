#Extract the Data

#Import Libraries
import requests
from bs4 import BeautifulSoup
import os
import zipfile

#folder path
download_path = '/home/zips/'
csv_path = '/home/csv/'

#connection to the website
url = "https://s3.amazonaws.com/capitalbikeshare-data/"
response = requests.get(url)

#get the Filenames of the zipFiles
soup = BeautifulSoup(response.text, 'xml')
file_names = soup.find_all('Key')
zipfiles = []
for i in file_names:
  i = str(i)
  i = i.replace('<Key>', '')
  i = i.replace('</Key>', '')
  if 'zip' in i:
    zipfiles.append(i)

#Download only new Files
download_log = []
download_log = os.listdir(download_path)
for i in zipfiles:
  if i not in download_log:
    source = url + i
    destination = download_path + i
    download_file = requests.get(source)
    with open(destination, 'wb') as f:
      f.write(download_file.content)
    download_log.append(i)

#Unzip the zipFiles
csv_log = []
csv_log = os.listdir(csv_path)
for i in download_log:
  if i not in csv_log:
    source = download_path + i
    destination = csv_path
    with zipfile.ZipFile(source, 'r') as zip_ref:
      inhalt = zip_ref.namelist() 
      for j in inhalt:
        if "/" not in j and ".csv" in j:
          zip_ref.extract(j, destination)
    csv_log.append(i)