from BeautifulSoup import BeautifulSoup
from urllib2 import urlopen
import csv

soup = BeautifulSoup(urlopen('http://www.cbssports.com/nfl/injuries'))

outputFile = open("injuries.csv", "w")
csvwriter = csv.writer(outputFile)

for table in soup.findAll("table", attrs = {"class" : "data"}):
    team_name = table.find("a").contents
    rows = table.findAll("tr")
    if len(rows) > 0:
        rows.pop(0)
        for row in rows:
            cells = [cell.text for cell in row.findAll('td')]
            print cells
            if len(cells) == 6:
                cells += team_name
                csvwriter.writerow(cells)

outputFile.close()