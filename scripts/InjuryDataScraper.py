from BeautifulSoup import BeautifulSoup
from urllib2 import urlopen
import csv

soup = BeautifulSoup(urlopen('http://www.cbssports.com/nfl/injuries'))

outputFile = open("injuries.csv", "w")
csvwriter = csv.writer(outputFile)

for table in soup.findAll("table", attrs = {"class" : "data"}):
    rows = table.findAll("tr")
    if len(rows) > 0:
        team_name = rows.pop(0).find("a")
        print rows.pop(0)
        #print rows
        for row in rows:
            cells = [cell.text for cell in row.findAll('td')]
            if len(cells) == 6:
                cells += team_name
                csvwriter.writerow(cells)

outputFile.close()