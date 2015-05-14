#!/usr/bin/env python

import os
import sys
import argparse
import unicodecsv as csv
import datetime
from BeautifulSoup import BeautifulStoneSoup


outputFields = [
    "Source",
    "Unique ID",
    "namespace",
    "Title",
    "Description",
    "Date",
    "Duration",
    "Keywords",
    "Price category (1= stock, 2=news, 3=brand)",
    "Sounds",
    "Color",
    "Location",
    "Country",
    "State",
    "City",
    "Region",
    "Alternative ID" ]
outputRows = []

def main(arguments):
    parser = argparse.ArgumentParser(description='This is a script which takes a path to a top-level directory of XML files of MovieTone metadata from KnowledgeMotion and outputs a .csv file which can be ingested by the Owl Atlas Processing ingest task by uploading it to the appropriate S3 bucket.')
    parser.add_argument('input_directory', help="Input directory", type=str)
    parser.add_argument('-o', '--outfile', help="Output CSV",
                        default=sys.stdout, type=argparse.FileType('wb'))
    args = parser.parse_args(arguments)

    csvWriter = csv.DictWriter(args.outfile, fieldnames=outputFields)
    csvWriter.writeheader()

    for root, subdirs, files in os.walk(args.input_directory):
        for xmlFile in files:
            parser = BeautifulStoneSoup(open(os.path.join(root, xmlFile), 'r').read())

            row = {}
            # tag names are parsed into lower case, so select them with lower case here
            row["Source"] = "Movietone"

            id = parser.find("id").string

            row["Unique ID"] = id

            row["namespace"] = "Movietone:" + id

            if parser.find("apcm:headline"):
                row["Title"] = parser.find("apcm:headline").string
            else:
                row["Title"] = parser.find("title").string

            description = ""
            bodyContentTag = parser.find("body.content")
            if not bodyContentTag is None:
                for pTag in bodyContentTag.findAll('p'):
                    if not pTag.string.startswith("Disclaimer:"):
                        description += pTag.string + "\n"
            row["Description"] = description

            row["Date"] = parser.find("apcm:firstcreated").string[0:10]

            characteristicsTag = parser.find("apcm:characteristics", {"totalduration" : True})
            if characteristicsTag is None:
                continue
            durationMs = int(characteristicsTag["totalduration"])
            durationSeconds = (durationMs/1000)%60
            durationMinutes = (durationMs/(1000*60))%60
            durationHours = (durationMs/(1000*60*60))%24
            row["Duration"] = str(durationHours).zfill(2) + ":" + str(durationMinutes).zfill(2) + ":" + str(durationSeconds).zfill(2)

            keywords = ""
            for entityClassificationTag in parser.findAll("apcm:entityclassification", {"value" : True}):
                keywords += entityClassificationTag["value"] + ", "
            for subjectClassificationTag in parser.findAll("apcm:subjectclassification", {"value" : True}):
                keywords += subjectClassificationTag["value"] + ", "
            row["Keywords"] = keywords[0:-2]

            row["Price category (1= stock, 2=news, 3=brand)"] = ""
            row["Sounds"] = ""
            row["Color"] = ""
            row["Location"] = ""
            row["Country"] = ""
            row["State"] = ""
            row["City"] = ""
            row["Region"] = ""
            row["Alternative ID"] = ""

            csvWriter.writerow(row)

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
