# -*- coding: utf-8 -*-
import urllib
import json
import os.path
import sys

# CONSTANTS
startday = '2016-01-01T00:00:00.000Z'
finishday = 'NOW'

# FUNCTIONS
# Check if the server is avaible or they are doing Maintenance.
def create_folder():
    # Create folder if not exist
    if not os.path.exists('json_files'):
        os.makedirs('json_files')
    if not os.path.exists('csv_files'):
        os.makedirs('csv_files')
    if not os.path.exists('solution'):
        os.makedirs('solution')

def check_dataHub():
     url = urllib.urlopen('https://scihub.copernicus.eu/dhus/search?q=Sentinel-1%20and%20beginposition:[2016-01-01T00:00:00.000Z%20TO%20NOW]&start=0&rows=100')
     code = url.getcode()
     if code == 503:
         print 'Maintenance of the server'
         return False
     else:
         print 'Server is avaible'
         return True
         
# Delete content of the folders
def delete_folder_content():
    # BORRAMOS EL CONTENIDO DE LAS CARPETAS
    path_folder = os.path.dirname(os.path.abspath(__file__))
    json_folder = path_folder + '/json_files'
    csv_folder = path_folder + '/csv_files'

    for the_file in os.listdir(json_folder):
        file_path = os.path.join(json_folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            print(e)

    for the_file in os.listdir(csv_folder):
        file_path = os.path.join(csv_folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            print(e)
            
# DOWNLOAD JSON FILES FOR EACH QUERY
def donwload_json_files():
    # OBTAIN THE NUMBER OF TOTAL REGISTRES
    # Download the first file
    filetotal = urllib.urlretrieve("https://scihub.copernicus.eu/dhus/search?q=Sentinel-1%20and%20beginposition:["+startday+" TO "+finishday+"]&format=json&start=0&rows=100", 'json_files/initialfile.json')

    # Read in file the number of registres
    with open('json_files/initialfile.json') as json_data:
        try:
            d = json.load(json_data)
            #totalregistres = int(d["feed"]["opensearch:totalResults"])
            totalregistres = 1000
            # Create de different files  
            for startrow in range(0,totalregistres,100):
                    f = urllib.urlretrieve("https://scihub.copernicus.eu/dhus/search?q=Sentinel-1%20and%20beginposition:["+startday+" TO "+finishday+"]&format=json&start="+str(startrow)+"&rows=100", "json_files/file_name"+str(startrow)+".json") 

        except:
            print 'The file is not a JSON file'
            # Check server
            if check_dataHub() == False:
                print 'Data Hub will be back soon. Run the script later'
                sys.exit()
            else:
                delete_folder_content()
                donwload_json_files()
                
# PARSER JSON TO CSV FILE
def json_to_csv_file():

    # Read total of files in folder
    path = os.path.dirname(os.path.abspath(__file__))
    num_files = len([f for f in os.listdir(path + '/json_files')if os.path.isfile(os.path.join(path + '/json_files', f))])
    hasta = (num_files - 1)*100

    for filenumber in range(0,hasta, 100):
        try:
            with open('json_files/file_name'+str(filenumber)+'.json') as json_data:
                d = json.load(json_data)
                parser_json(filenumber, d)     

        except:
            print 'The file is not a JSON file'
            # We need to dowload another time the file.
            f = urllib.urlretrieve("https://scihub.copernicus.eu/dhus/search?q=Sentinel-1%20and%20beginposition:["+startday+" TO "+finishday+"]&format=json&start="+str(filenumber)+"&rows=100", "json_files/file_name"+str(filenumber)+".json")
            # Parser the download file
            with open('json_files/file_name'+str(filenumber)+'.json') as json_data:
                d = json.load(json_data)
                parser_json(filenumber, d)


def parser_json(filenumber, d):
    # Initial parameters
    id_ = ""
    summary = ""
    title = ""
    uuid = ""
    acquisitiontype = ""
    filename = ""
    gmlfootprint = ""
    format_ = ""
    identifier = ""
    instrumentshortname = ""
    sensoroperationalmode = ""
    instrumentname = ""
    swathidentifier = ""
    footprint = ""
    platformidentifier = ""
    orbitdirection = ""
    polarisationmode = ""
    productclass = ""
    producttype = ""
    platformname = ""
    size = ""
    status = ""
    productconsolidation = ""
    missiondatatakeid = ""
    orbitnumber = ""
    lastorbitnumber = ""
    relativeorbitnumber = ""
    lastrelativeorbitnumber = ""
    slicenumber = ""
    link = ""
    processed_bool = ""
    ingestiondate= ""
    beginposition = ""
    endposition = ""
    
    # File Creation
    copernicus_data = open('csv_files/copernicus_data'+str(filenumber)+'.csv', 'wt')

    num_entry = len(d["feed"]["entry"])
    for entry_ in range(num_entry):

        # ID, SUMMARY AND TITLE
        id_ = d["feed"]["entry"][entry_]["id"].strip()
        summary = d["feed"]["entry"][entry_]["summary"].strip()
        title = d["feed"]["entry"][entry_]["title"].strip()
        
        # STR PARAMETERS
        len_str = len(d["feed"]["entry"][entry_]["str"])
        for str_obj in range(len_str):
            
            if  d["feed"]["entry"][entry_]["str"][str_obj]["name"] == 'uuid':
                uuid = d["feed"]["entry"][entry_]["str"][str_obj]["content"].strip()
            elif d["feed"]["entry"][entry_]["str"][str_obj]["name"] == 'acquisitiontype':
                acquisitiontype = d["feed"]["entry"][entry_]["str"][str_obj]["content"].strip()
            elif  d["feed"]["entry"][entry_]["str"][str_obj]["name"] == 'filename':
                filename = d["feed"]["entry"][entry_]["str"][str_obj]["content"].strip()
            elif  d["feed"]["entry"][entry_]["str"][str_obj]["name"] == 'gmlfootprint':
                gmlfootprint = d["feed"]["entry"][entry_]["str"][str_obj]["content"].strip().replace("\n", "")
            elif  d["feed"]["entry"][entry_]["str"][str_obj]["name"] == 'format':
                format_ = d["feed"]["entry"][entry_]["str"][str_obj]["content"].strip()
            elif  d["feed"]["entry"][entry_]["str"][str_obj]["name"] == 'identifier':
                identifier = d["feed"]["entry"][entry_]["str"][str_obj]["content"].strip()
            elif  d["feed"]["entry"][entry_]["str"][str_obj]["name"] == 'instrumentshortname':
                instrumentshortname = d["feed"]["entry"][entry_]["str"][str_obj]["content"].strip()
            elif  d["feed"]["entry"][entry_]["str"][str_obj]["name"] == 'sensoroperationalmode':
                sensoroperationalmode = d["feed"]["entry"][entry_]["str"][str_obj]["content"].strip()
            elif  d["feed"]["entry"][entry_]["str"][str_obj]["name"] == 'instrumentname':
                instrumentname = d["feed"]["entry"][entry_]["str"][str_obj]["content"].strip()
            elif  d["feed"]["entry"][entry_]["str"][str_obj]["name"] == 'swathidentifier':
                swathidentifier = d["feed"]["entry"][entry_]["str"][str_obj]["content"].strip()
            elif  d["feed"]["entry"][entry_]["str"][str_obj]["name"] == 'footprint':
                footprint = d["feed"]["entry"][entry_]["str"][str_obj]["content"].strip()
            elif  d["feed"]["entry"][entry_]["str"][str_obj]["name"] == 'platformidentifier':
                platformidentifier = d["feed"]["entry"][entry_]["str"][str_obj]["content"].strip()
            elif  d["feed"]["entry"][entry_]["str"][str_obj]["name"] == 'orbitdirection':
                orbitdirection = d["feed"]["entry"][entry_]["str"][str_obj]["content"].strip()
            elif  d["feed"]["entry"][entry_]["str"][str_obj]["name"] == 'polarisationmode':
                polarisationmode = d["feed"]["entry"][entry_]["str"][str_obj]["content"].strip()
            elif  d["feed"]["entry"][entry_]["str"][str_obj]["name"] == 'productclass':
                productclass = d["feed"]["entry"][entry_]["str"][str_obj]["content"].strip()
            elif  d["feed"]["entry"][entry_]["str"][str_obj]["name"] == 'producttype':
                producttype = d["feed"]["entry"][entry_]["str"][str_obj]["content"].strip()
            elif  d["feed"]["entry"][entry_]["str"][str_obj]["name"] == 'platformname':
                platformname = d["feed"]["entry"][entry_]["str"][str_obj]["content"].strip()
            elif  d["feed"]["entry"][entry_]["str"][str_obj]["name"] == 'size':
                size = d["feed"]["entry"][entry_]["str"][str_obj]["content"].strip()
            elif d["feed"]["entry"][entry_]["str"][str_obj]["name"] == 'status':
                status = d["feed"]["entry"][entry_]["str"][str_obj]["content"].strip()
            elif d["feed"]["entry"][entry_]["str"][str_obj]["name"] == 'productconsolidation':
                productconsolidation = d["feed"]["entry"][entry_]["str"][str_obj]["content"].strip()

                
        
        # INT PARAMETERS
        len_int = len(d["feed"]["entry"][entry_]["int"])
        for int_obj in range(len_int):
            if d["feed"]["entry"][entry_]["int"][int_obj]["name"] == 'missiondatatakeid':
                missiondatatakeid = d["feed"]["entry"][entry_]["int"][int_obj]["content"].strip()
            elif d["feed"]["entry"][entry_]["int"][int_obj]["name"] == 'orbitnumber':
                orbitnumber = d["feed"]["entry"][entry_]["int"][int_obj]["content"].strip()
            elif d["feed"]["entry"][entry_]["int"][int_obj]["name"] == 'lastorbitnumber':
                lastorbitnumber = d["feed"]["entry"][entry_]["int"][int_obj]["content"].strip()
            elif d["feed"]["entry"][entry_]["int"][int_obj]["name"] == 'relativeorbitnumber':
                 relativeorbitnumber = d["feed"]["entry"][entry_]["int"][int_obj]["content"].strip()
            elif d["feed"]["entry"][entry_]["int"][int_obj]["name"] == 'lastrelativeorbitnumber':
                lastrelativeorbitnumber = d["feed"]["entry"][entry_]["int"][int_obj]["content"].strip()
            elif d["feed"]["entry"][entry_]["int"][int_obj]["name"] == 'slicenumber':
                slicenumber = d["feed"]["entry"][entry_]["int"][int_obj]["content"].strip()

        # LINK PARAMETER
        link = d["feed"]["entry"][entry_]["link"][0]["href"].strip()

        #PROCESSED PARAMETERS    
        #processed_bool = d["feed"]["entry"][entry_]["bool"]["content"].strip()

        # DATEPARAMETERS
        len_date = len(d["feed"]["entry"][entry_]["date"])
        for date_obj in range(len_date):
            if d["feed"]["entry"][entry_]["date"][date_obj]["name"] == 'ingestiondate':
                ingestiondate = d["feed"]["entry"][entry_]["date"][date_obj]["content"].strip()
            elif d["feed"]["entry"][entry_]["date"][date_obj]["name"] == 'beginposition':
                beginposition = d["feed"]["entry"][entry_]["date"][date_obj]["content"].strip()
            elif d["feed"]["entry"][entry_]["date"][date_obj]["name"] == 'endposition':
                endposition = d["feed"]["entry"][entry_]["date"][date_obj]["content"].strip()
            
        copernicus_data.write(id_ + ';' + summary + ';' + title + ';' + uuid + ';' + acquisitiontype + ';' + filename + ';' + gmlfootprint + ';' + \
                               format_  + ';' + identifier + ';' + instrumentshortname + ';' + sensoroperationalmode + ';' + instrumentname + ';' + \
                               swathidentifier + ';' + footprint + ';' + platformidentifier + ';' + orbitdirection + ';' + polarisationmode + ';' + \
                               productclass + ';' + producttype + ';' + platformname + ';' + size + ';' + status + ';' + productconsolidation + ';' +\
                               missiondatatakeid + ';' + orbitnumber + ';' + lastorbitnumber + ';' + relativeorbitnumber + ';' + \
                               lastrelativeorbitnumber + ';' + slicenumber + ';' + link + ';' + ingestiondate + ';' + \
                               beginposition + ';' + endposition + '\n')

        # Clean the variables
        id_ = ""
        summary = ""
        title = ""
        uuid = ""
        acquisitiontype = ""
        filename = ""
        gmlfootprint = ""
        format_ = ""
        identifier = ""
        instrumentshortname = ""
        sensoroperationalmode = ""
        instrumentname = ""
        swathidentifier = ""
        footprint = ""
        platformidentifier = ""
        orbitdirection = ""
        polarisationmode = ""
        productclass = ""
        producttype = ""
        platformname = ""
        size = ""
        status = ""
        productconsolidation = ""
        missiondatatakeid = ""
        orbitnumber = ""
        lastorbitnumber = ""
        relativeorbitnumber = ""
        lastrelativeorbitnumber = ""
        slicenumber = ""
        link = ""
        processed_bool = ""
        ingestiondate= ""
        beginposition = ""
        endposition = ""

            

# UNION CSV FILES IN ONE FILE
def union_csv_files():
    path_folder = os.path.dirname(os.path.abspath(__file__))
    csv_folder = path_folder + '/csv_files/'
    os.system('cat '+ csv_folder + '* > solution/union.csv')

            

# EXECUTION PROCESS
if check_dataHub() == True:
    create_folder()
    delete_folder_content()
    donwload_json_files()
    json_to_csv_file()
    union_csv_files()
    

else:
    print 'Data Hub will be back soon. Run the script later'
    
    




