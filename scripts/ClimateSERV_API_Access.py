# Title:        ClimateSERV Api Automation Script
# Description:  This is a python 2.7 script that provides automated access to the data that is accessible via the SERVIR ClimateSERV API
#
# Note:         In this a somewhat early version, API access is limited.
#
# Current Supported Operations:
#               Geography Selections: List of Coordinate pairs 
#               Operation: Average, Min, Max, Download (Seasonal_Forecast Only)
#               DataSets: CHIRPS, eMODIS, Seasonal_Forecast
#               Only supports single ensemble + variable combinations per script run for Seasonal_Forecasts
#
#
# Version 0.71 - PreRelease (rough draft - Not ready for official release or distribution)
#
#
# Author: Kris Stanton (SERVIR)
# Date: Dec 2015
from __builtin__ import False



# Imports
import time
import urllib  # simple url encoding formatting
import urllib2
import json
#import cserv_config
import ConfigParser
import os
import sys
import logging
import csv


# Do not override, # These are managed by command line inputs.
#g_isLogFile = False
#g_logFileLocation = None
g_logger = None

# Print function
def printMe(msgStr):
    #global g_isLogFile, g_logFileLocation, g_logger
    global g_logger
    print(msgStr)
    if not (g_logger == None):
        try:
            # attempt to log the msgStr
            logging.info(msgStr)
        except:
            g_logger = None
        
    #if(g_isLogFile == True):
    #    if not (g_logFileLocation == None):
    #        # Logg the msgStr statement.
    #        pass
    

# As a result of early access to this script, note that if the dataset ID's change this function will have to change as well to match.) 
# This function is a hardcoded look up table, (bad practice!)
def get_datasetID__HC_datasetLookup(datasetType, seasonal_Ensemble, seasonal_Variable):
    if(datasetType == "CHIRPS"):
        return 0
    if(datasetType == "eMODIS"):
        return 1
    if(datasetType == "Seasonal_Forecast"):
        if(seasonal_Ensemble == "ens01"):
            if(seasonal_Variable == "Temperature"):
                return 6
            if(seasonal_Variable == "Precipitation"):
                return 7
        if(seasonal_Ensemble == "ens02"):
            if(seasonal_Variable == "Temperature"):
                return 8
            if(seasonal_Variable == "Precipitation"):
                return 9
        if(seasonal_Ensemble == "ens03"):
            if(seasonal_Variable == "Temperature"):
                return 10
            if(seasonal_Variable == "Precipitation"):
                return 11
        if(seasonal_Ensemble == "ens04"):
            if(seasonal_Variable == "Temperature"):
                return 12
            if(seasonal_Variable == "Precipitation"):
                return 13
        if(seasonal_Ensemble == "ens05"):
            if(seasonal_Variable == "Temperature"):
                return 14
            if(seasonal_Variable == "Precipitation"):
                return 15
        if(seasonal_Ensemble == "ens06"):
            if(seasonal_Variable == "Temperature"):
                return 16
            if(seasonal_Variable == "Precipitation"):
                return 17
        if(seasonal_Ensemble == "ens07"):
            if(seasonal_Variable == "Temperature"):
                return 18
            if(seasonal_Variable == "Precipitation"):
                return 19
        if(seasonal_Ensemble == "ens08"):
            if(seasonal_Variable == "Temperature"):
                return 20
            if(seasonal_Variable == "Precipitation"):
                return 21
        if(seasonal_Ensemble == "ens09"):
            if(seasonal_Variable == "Temperature"):
                return 22
            if(seasonal_Variable == "Precipitation"):
                return 23
        if(seasonal_Ensemble == "ens10"):
            if(seasonal_Variable == "Temperature"):
                return 24
            if(seasonal_Variable == "Precipitation"):
                return 25
    # if we got this far, there was an issue looking up the dataset id
    return -1
def get_OperationID__HC_operationLookup(operationType):
    if(operationType == "Average"):
        return 5
    if(operationType == "Max"):
        return 0
    if(operationType == "Min"):
        return 1
    if(operationType == "Download"):
        return 6
    # if we got this far, there was an issue looking up the operation id
    return -1


def get_URLParam_NoCacheOption():
    retString = "&_=" + str(int(time.time()))
    return retString

def get_URLParam_Key(theKey):
    retString = "&t=" + theKey
    return retString

def get_URLParam_Callback(callBackFuncName):
    retString = "&callback=" + callBackFuncName
    return retString



def get_Request_URL_submitDataRequest(baseURL, datatype, intervaltype, operationtype, begintime, endtime, geometry_CoordsList):
    retURL = baseURL + "?a=1"
    retURL += "&cmd=submitDataRequest"
    retURL += "&datatype="+str(datatype)
    retURL += "&intervaltype="+str(intervaltype)
    retURL += "&operationtype="+str(operationtype)
    retURL += "&begintime="+str(begintime)
    retURL += "&endtime="+str(endtime)

    # Process Geometry CoordsList and encode it.
    # geometry_CoordsList : Expects [ [lng,lat],[lng,lat]...[lng,lat] ]
    # Server expects, geometry:{"type":"Polygon","coordinates":[ [ [lng,lat],[lng,lat],....[lng,lat] ]   ] }
    
    # Convert [[],[]] string into an obj 
    # Nope, that conversion should be handled before this function..
    
    geometry_String = ""
    try:
        gObj = {"type":"Polygon","coordinates":[]}
        gObj['coordinates'].append(geometry_CoordsList)
        geometry_JSON = json.dumps(gObj)
        geometry_JSON_NoSpaces = geometry_JSON.replace(" ","") # Remove Spaces
        geometry_JSON_Encoded = urllib.quote(geometry_JSON_NoSpaces)
        geometry_String = geometry_JSON_Encoded
    except:
        printMe("Error Creating and encoding geometry_String parameter")
        pass

    retURL += "&geometry="+str(geometry_JSON_Encoded)
    
    return retURL
    

def get_Request_URL_getDataRequestProgress(baseURL, jobID):
    retURL = baseURL + "?a=2"
    retURL += "&cmd=getDataRequestProgress"
    retURL += "&id=" + str(jobID)
    return retURL

def get_Request_URL_getDataFromRequest(baseURL, jobID):
    retURL = baseURL + "?a=3"
    retURL += "&cmd=getDataFromRequest"
    retURL += "&id=" + str(jobID)
    return retURL

def get_Request_URL_getFileForJobID(baseURL, jobID):
    retURL = baseURL + "?a=4"
    retURL += "&cmd=getFileForJobID"
    retURL += "&id=" + str(jobID)
    return retURL

# Generic Communication with the server
def get_ServerResponse(theURL):
    urlOpen_Timeout = 30 # 30 Second timeout
    time.sleep(1)   # Please don't remove'time.sleep(1)', We don't want to hammer the server with too many concurrent requests at this time....
    #response = urllib2.urlopen(theURL)
    response = urllib2.urlopen(theURL, timeout=urlOpen_Timeout)
    theJSON = json.load(response)
    return theJSON


# Get the data from the server
def get_ServerReturn_DataFromJob(response):
    try:
        returnData = response[0]
        return returnData
    except:
        printMe("get_ServerReturn_DataFromJob: Something went wrong..Generic Catch All Error.")
        return -1

# Check for Server Errors
def check_And_Display_ServerErrorMessage(response):
    try:
        errorMessage = response['errorMsg']
        printMe("**** SERVER RESPONDED WITH AN ERROR ")
        printMe("**** The server responded to your request with an error message.")
        printMe("**** Error Message: " + errorMessage)
        #printMe("**** For Debugging purposes, here is the raw request that was made: ")
        #printMe("**** " + str(response))
        #printMe("**** END")
    except:
        # Probably no errors to report!
        pass

# Response from submitting a new job # When it is expected that the server will return a JobID
def get_ServerReturn_JobID_FromResponse(response):
    try:
        newJobID = response[0]
        return newJobID
    except:
        printMe("get_ServerReturn_JobID_FromResponse: Something went wrong..Generic Catch All Error.")
        return -1

# Job Progress and Status Loop
def get_JobProgressValue_FromResponse(response):
    try:
        jobProgressItem = response[0]
        #printMe(jobProgressItem)
        return jobProgressItem
    except:
        # Something went wrong, Catch all
        printMe("get_JobProgressValue_FromResponse: Something went wrong..Generic Catch All Error.")
        return -1.0 # Default, 'error' code for jobstatus
    
def check_JobProgress(jobID, APIAccessKey, BaseURL):
    url_For_getDataRequestProgress = get_Request_URL_getDataRequestProgress(BaseURL, jobID) 
    url_For_getDataRequestProgress += get_URLParam_Key(APIAccessKey)
    serverResponse = get_ServerResponse(url_For_getDataRequestProgress)
    currentJobStatus = int(get_JobProgressValue_FromResponse(serverResponse))
    return currentJobStatus

def getJobResult_wait_ForJobProgressCycle(jobID, APIAccessKey, BaseURL):
    
    isInCycle = True
    cycleCompleteCount = 0
    jobStatus = "unset" # "inprogress", "complete", "error_generic", "error_timeout"
    numOfCyclesToTry = 1800
    
    while(isInCycle == True):
        
         
        # get Job Progress value
        currentJobProgress = check_JobProgress(jobID, APIAccessKey, BaseURL)
        printMe("Current Job Progress: " + str(currentJobProgress) + ".  JobID: " + str(jobID))
        time.sleep(1) 
        
        # Process Job Status
        if(currentJobProgress == 100):
            jobStatus = "complete"
            isInCycle = False
        elif(currentJobProgress == -1):
            jobStatus = "error_generic"
            isInCycle = False
        else:
            jobStatus = "inprogress"
            isInCycle = True
            
        # Should we bail out of this loop?
        if(cycleCompleteCount > numOfCyclesToTry):
            jobStatus = "error_timeout"
            isInCycle = False
        
        cycleCompleteCount += 1
        
        # For long wait times, echo the cycle
        if(cycleCompleteCount % 50 == 0):
            printMe("Still working.... Cycle: " + str(cycleCompleteCount))
        
        
        
        
    # Process return (did the job fail or succeed..)
    printMe("Result of Job Status Cycle: " + str(jobStatus))
    if(jobStatus == "complete"):
        return True
    else:
        return False
    

# attempt to Convert epochTime to ints and then sort
def get_jobData_As_Sorted_EpochTimeInts(jobData):
    
    try:
        convertedEpochTimesList = jobData['data']
        #printMe(convertedEpochTimesList)
        for x in range(0, len(convertedEpochTimesList)):
            #printMe("x: " + str(x))
            #printMe("convertedEpochTimesList[x]['epochTime']: " + str(convertedEpochTimesList[x]['epochTime']))
            #printMe("int(convertedEpochTimesList[x]['epochTime']): " + str(int(convertedEpochTimesList[x]['epochTime'])))
            convertedEpochTimesList[x]['epochTime'] = int(convertedEpochTimesList[x]['epochTime'])
        sortedJobData = sorted(convertedEpochTimesList, key=lambda k: k['epochTime'])
        retObj = { 'data':sortedJobData}
        return retObj
    except:
        # Failed, return original
        #printMe("get_jobData_As_Sorted_EpochTimeInts Failed")
        return jobData
    # Got to the end, return original
    return jobData
    
# Convert the server response to a CSV ready object
def get_CSV_Ready_Processed_Dataset(jobData, operationType):
    #printMe(str(jobData))
    #printMe("Converting and Sorting jobData")
    jobData = get_jobData_As_Sorted_EpochTimeInts(jobData)
    #printMe(str(jobData))
    retList = []
    fileFailedList = []
    csvHeaderStringList = []
    try:
        # Set the Key from the operation Type
        dateKey = "date"
        valueKey = "value"
        if(operationType == 0):
            valueKey = "max"
        if(operationType == 1):
            valueKey = "min"
        if(operationType == 5):
            valueKey = "avg"
        if(operationType == 6):
            valueKey = "FileGenerationSuccess"
        
        csvHeaderStringList.append(dateKey) 
        csvHeaderStringList.append(valueKey) 
        
        for currentGranule in jobData['data']:
            currentDate = "NULL"
            currentValue = "NULL"
            if not (operationType == 6):
                # For non download types
                currentDate = str(currentGranule[dateKey])
                currentValue = str(currentGranule['value'][valueKey])
            else:
                # For download types
                currentDate = str(currentGranule[dateKey])
                currentValue = str(currentGranule['value'])
                if (currentValue == '0'):
                    fileFailedList.append(currentDate)
            listObj = {
                       dateKey:currentDate,
                       valueKey:currentValue
                       }
            retList.append(listObj)    
            
    except:
        printMe("get_CSV_Ready_Processed_Dataset: Something went wrong..Generic Catch All Error.")
        
    return retList, csvHeaderStringList, fileFailedList
    


# Main Pipe for managing the submitting of a job, waiting for it to complete and receiving the data/download links 
def process_Job_Controller(APIAccessKey, BaseURL, DatasetType, OperationType, Earliest_Date, Latest_Date, GeometryCoords, SeasonalEnsemble, SeasonalVariable):
    
    #retList = []
    
    job_OperationID = get_OperationID__HC_operationLookup(OperationType) 
    job_DatasetID = get_datasetID__HC_datasetLookup(DatasetType, SeasonalEnsemble, SeasonalVariable) 
    
    # Validation
    if(job_DatasetID == -1):
        printMe("ERROR.  DatasetID not found.  Check your input params to ensure the DatasetType value is correct.  (Case Sensitive)")
        printMe(" To help you debug, Some of the parameters used for this job were: ")
        printMe("  DatasetType : " + str(DatasetType))
        printMe("  SeasonalEnsemble : " + str(SeasonalEnsemble))
        printMe("  SeasonalVariable : " + str(SeasonalVariable))
        return -1
    if(job_OperationID == -1):
        printMe("ERROR.  OperationID not found.  Check your input params to ensure the OperationType value is correct.  (Case Sensitive)")
        printMe(" To help you debug, Some of the parameters used for this job were: ")
        printMe("  OperationType : " + str(OperationType))
        return -1
    
    # Submit the new job request
    submitData_ReqURL = get_Request_URL_submitDataRequest(BaseURL, job_DatasetID, 0, job_OperationID, Earliest_Date, Latest_Date, GeometryCoords)  
    submitData_ReqURL += get_URLParam_Key(APIAccessKey)
    newJob_Response = get_ServerResponse(submitData_ReqURL)
    check_And_Display_ServerErrorMessage(newJob_Response)
    theJobID = get_ServerReturn_JobID_FromResponse(newJob_Response)
    
    # Validate the JobID
    if(theJobID == -1):
        printMe("Something went wrong submitting the job.  Waiting for a few seconds and trying one more time")
        time.sleep(3)
        newJob_Response = get_ServerResponse(submitData_ReqURL)
        check_And_Display_ServerErrorMessage(newJob_Response)
        theJobID_SecondTry = get_ServerReturn_JobID_FromResponse(newJob_Response)
        if(theJobID_SecondTry == -1):
            printMe("Job Submission second failed attempt.  Bailing Out.")
            retObjFail = {}
            return retObjFail
        else:
            theJobID = theJobID_SecondTry
            
    printMe("New Job Submitted to the Server: New JobID: " + str(theJobID))
    
    # Enter the loop waiting on the progress.
    isJobSuccess = getJobResult_wait_ForJobProgressCycle(theJobID, APIAccessKey, BaseURL) 
    
    # Report Status to the user (console)
    printMe("Job, " + str(theJobID) + " is done, did it succeed? : " + str(isJobSuccess))
    
    # If it succeeded, get data
    if(isJobSuccess == True):
        getJobData_ReqURL = get_Request_URL_getDataFromRequest(BaseURL, theJobID) 
        getJobData_ReqURL += get_URLParam_Key(APIAccessKey)
        getJobData_Response = get_ServerResponse(getJobData_ReqURL)
        
        #printMe("Job Data (getJobData_Response): " + str(getJobData_Response))
        
        # Parse out to an object that is ready for csv writing
        csvReady_DataObj, csvHeaderList, failedFileList = get_CSV_Ready_Processed_Dataset(getJobData_Response, job_OperationID)
        
        
        
        # If file download job, generate the file download link.
        downloadLink = "NA"
        if(job_OperationID == 6):
            downloadLink = get_Request_URL_getFileForJobID(BaseURL, theJobID) 
            downloadLink += get_URLParam_Key(APIAccessKey)
        
        # Output formats
        # JSON Formatted response object: getJobData_Response
        # CSV Ready Formatted response object: 
        #    The Header Row (titles): csvHeaderList
        #    Object reader for csvFile.writerow inside for loop: csvReady_DataObj
        # Download Link: downloadLink
        
        # Debug and Test
        # Printing the outputs to the console
        #printMe("Job " + str(theJobID) + " Outputs:")
        #printMe("- getJobData_Response: " + str(getJobData_Response))
        #printMe("- csvHeaderList: " + str(csvHeaderList))
        #printMe("- csvReady_DataObj: " + str(csvReady_DataObj))
        #printMe("- downloadLink: " + str(downloadLink))
        #printMe("- Dates for files that failed to generate: " + str(failedFileList))
        
        retObj = {
                  "ServerJobID":theJobID,
                  "JobData_ServerResponse_JSON":getJobData_Response,
                  "csvHeaderList":csvHeaderList,
                  "csvWriteReady_DataObj":csvReady_DataObj, 
                  "downloadLink":downloadLink,
                  "rawData_FailedDatesList":failedFileList
                  }
        
        #retList.append(retObj)
        return retObj
        
    else:
        printMe("ERROR.  There was an error with this job.")
        printMe("(The error may have been caused by an error on the server.)")
        printMe("Double check the parameters listed below and try again.  If the error persists, please contact the ClimateSERV Staff and be sure to send the a copy of this error message along with the parameters listed below.  Thank you!")
        printMe(" To help you debug, Some of the parameters used for this job were: ")
        printMe("  APIAccessKey : " + str(APIAccessKey))
        printMe("  BaseURL : " + str(BaseURL))
        printMe("  DatasetType : " + str(DatasetType))
        printMe("  OperationType : " + str(OperationType))
        printMe("  Earliest_Date : " + str(Earliest_Date))
        printMe("  Latest_Date : " + str(Latest_Date))
        printMe("  SeasonalEnsemble : " + str(SeasonalEnsemble))
        printMe("  SeasonalVariable : " + str(SeasonalVariable))
        printMe("  GeometryCoords : " + str(GeometryCoords))

        retObjFailError = {}
        return retObjFailError
    
    retObjReachedTheEnd = {}
    return retObjReachedTheEnd
    

    
# Process config list of objects, do each job one at a time, return results.
def main_ProcessRequests(configObj_List):   
    
    scriptJobCount = 0
    jobsDataList = []
    for configObj in configObj_List:
        scriptJobCount += 1
        printMe("=======================================================")
        #printMe("About to process Scripted Job Item: " + str(scriptJobCount))
        printMe("About to process scripted job item now.")
        
        # Unpack current Config Item
        APIAccessKey = configObj['APIAccessKey']
        BaseURL = configObj['BaseURL']
        DatasetType = configObj['DatasetType']
        OperationType = configObj['OperationType']
        Earliest_Date = configObj['EarliestDate']
        Latest_Date = configObj['LatestDate']
        GeometryCoords = configObj['GeometryCoords']
        SeasonalEnsemble = configObj['SeasonalEnsemble']
        SeasonalVariable = configObj['SeasonalVariable']
        
        try:
            # Execute Job
            currentJob_ReturnData = process_Job_Controller(APIAccessKey, BaseURL, DatasetType, OperationType, Earliest_Date, Latest_Date, GeometryCoords, SeasonalEnsemble, SeasonalVariable)
        
            # Store Job Return Data along with original Config Item
            jobDetails = {
                      "JobReturnData":currentJob_ReturnData,
                      "JobConfigData":configObj
                      }
            jobsDataList.append(jobDetails)
        except:
            printMe("ERROR: Something went wrong!!       There and can mean that there is currently an issue with the server.  Please try again later.  If the error persists, please contact the ClimateSERV staff.")
            printMe("  This is a generic catch all error that could have multiple possible causes.")
            printMe("     Possible causes may include:")
            printMe("       Issues with your connection to the ClimateSERV server")
            printMe("       Issues with your connection to the Internet")
            printMe("       Invalid input parameters from the configuration file or command line")
            printMe("       Interruptions of service with the ClimateSERV Service")
            
            
    printMe("=======================================================")
    #printMe("  END")
    return jobsDataList

def print_UsageCommandLineArgumentList_ToConsole():
    printMe("")
    printMe("======================================================")
    printMe("")
    printMe("ClimateSERV Script API Access Arguments Usage:")
    printMe("")
    printMe(" Required Argument Parameters")
    printMe("")
    printMe("  -config YourConfigFileName.ini")
    printMe("    # Specifies the configuration file read by the script")
    printMe("")
    printMe("  -outfile YourOutFileName")
    printMe("    # Specifies the filename for output data")
    printMe("")
    printMe("")
    printMe(" Optional Argument Parameters")
    printMe("")
    printMe("  -logfileout LogFileName")
    printMe("    # (optional) if specified, console output is also logged to a file")
    printMe("    # File will attempt to be created it if it does not exist")
    printMe("")
    printMe("  -dataset DatasetTypeValue")
    printMe("    # (optional) overrides the DatasetType configuration value.")
    printMe("    # Supported values: CHIRPS, eMODIS, Seasonal_Forecast")
    printMe("")
    printMe("  -operation OperationTypeValue")
    printMe("    # (optional) overrides the OperationType configuration value.")
    printMe("    # Supported values: Average, Min, Max, Download")
    printMe("")
    printMe("  -earlydate MM/DD/YYYY")
    printMe("    # (optional) overrides the EarliestDate configuration value.")
    printMe("    # Date values must be 10 characters including forward slashes")
    printMe("")
    printMe("  -latedate MM/DD/YYYY")
    printMe("    # (optional) overrides the LatestDate configuration value.")
    printMe("    # Date values must be 10 characters including forward slashes")
    printMe("")
    printMe("  -sens SeasonalEnsembleValue")
    printMe("    # (optional) overrides the Seasonal Ensemble value.")
    printMe("    # Supported values: ens01, ens02, ens03, ens04, ens05, ens06, ens07, ens08, ens09, ens10")
    printMe("")
    printMe("  -svar SeasonalVariableValue")
    printMe("    # (optional) overrides the Seasonal Variable value.")
    printMe("    # Supported values: Precipitation, Temperature ")
    printMe("")
    printMe("=====================================================")
    printMe("")
    

# find the value of the command line argument from a given key.
def get_Arg_Value(argKey, theArgs):
    retValue = None
    try:
        for i, val in enumerate(theArgs):
            if(argKey == str(val)):
                retValue = theArgs[(i+1)]
                
            #printMe("i : " + str(i))
            #printMe("val : " + str(val))
    except:
        pass
    return retValue



def get_ConfigObj_From_File(configFile):
    doesFileExist = os.path.isfile(configFile) 
    printMe("")
    printMe("Config File: " + configFile + ", was found? : " + str(doesFileExist))
    
    # Config Parser object
    config = ConfigParser.ConfigParser()
    
    # Open the config file
    config.read(configFile)
    
    cserv_config = {
                    'APIAccessKey':str(config.get('DEFAULT', 'APIAccessKey', 1)),
                    'DatasetType': str(config.get('DEFAULT', 'DatasetType', 1)),
                    'OperationType': str(config.get('DEFAULT', 'OperationType', 1)),
                    'SeasonalEnsemble': str(config.get('DEFAULT', 'SeasonalEnsemble', 1)),
                    'SeasonalVariable': str(config.get('DEFAULT', 'SeasonalVariable', 1)),
                    'EarliestDate': str(config.get('DEFAULT', 'EarliestDate', 1)),
                    'LatestDate': str(config.get('DEFAULT', 'LatestDate', 1)),
                    'GeometryCoords': json.loads(str(config.get('DEFAULT', 'GeometryCoords', 1))),
                    'BaseURL': str(config.get('DEFAULT', 'BaseURL', 1))
                    }
    
    return cserv_config
    
def download_File(urlToFile, localFileName_ForSaving):
    f = urllib2.urlopen(urlToFile)
    printMe("Downloading file.  This may take a few minutes..")
    #with open(os.path.basename(localFileName_ForSaving), "wb") as local_file:
    with open(localFileName_ForSaving, "wb") as local_file:
        local_file.write(f.read())
    
# Entry point of command line interface
def main_ProcessCommandLine_Request():
    
    # For detailed outputs
    debug_VerboseMsg = True
    
    printMe("")
    printMe("New Script Run")
            
    #global g_isLogFile, g_logFileLocation, g_logger
    global g_logger
        
    # Check for Required arguments
    theArgs = sys.argv
    hasRequiredParams = False
    if( ('-config' in theArgs) and ('-outfile' in theArgs) ):
        hasRequiredParams = True
    else:
        hasRequiredParams = False
    if(hasRequiredParams == False):
        printMe("ERROR: Parameters -config and -outfile are required.  See Usage below.  Exiting...")
        printMe("")
        print_UsageCommandLineArgumentList_ToConsole()
        return
    
    
    # Toggle Verbose Debug Messages from the command line.    
    try:
        if ('-verboseoff' in theArgs):
            debug_VerboseMsg = False
            printMe("Verbose Debug Messages turned off.")
        else:
            debug_VerboseMsg = True
    except:
        debug_VerboseMsg = True
            
    # Attempt to create log file
    #g_isLogFile = False
    logFileLocation = None
    g_logger = None
    try:
        if ('-logfileout' in theArgs):
            g_logFileLocation = get_Arg_Value("-logfileout", theArgs)
            logging.basicConfig(filename=g_logFileLocation,level=logging.INFO,format='%(asctime)s %(message)s')
            g_logger = "Placeholder__NOT_NONE"
            printMe("")
            printMe("========= New Script Run")
            printMe("Logging has been Enabled!")
    except:
        g_logger = None


    # Scoping
    configObj = None
    
    # Attempt to read and store the config file into an object used by the rest of the script.
    try:
        # Get file location from the args
        configFile = get_Arg_Value("-config", theArgs)
        #printMe("Config File: " + str(configFile))
        
        # Check for required config file
        does_Config_FileExist = os.path.isfile(configFile) 
        if(does_Config_FileExist == False):
            printMe("ERROR. Config file: " + str(configFile) + " not found or was unable to be opened.")
            printMe("Exiting...")
            printMe("")
            return
        
        # Get the Config object by reading the file
        configObj = get_ConfigObj_From_File(configFile)
        
        if(debug_VerboseMsg == True):
            printMe("configObj (From File Only): " + str(configObj))
        
        # Also in here is the overwriting of config settings by commandline arguments.
        try:
            # Override Config with command line args
            configObj['logfileout'] = logFileLocation
            if ('-outfile' in theArgs):
                configObj['outfile'] = get_Arg_Value("-outfile", theArgs)
            if ('-dataset' in theArgs):
                configObj['DatasetType'] = get_Arg_Value("-dataset", theArgs)
            if ('-operation' in theArgs):
                configObj['OperationType'] = get_Arg_Value("-operation", theArgs)
            if ('-earlydate' in theArgs):
                configObj['EarliestDate'] = get_Arg_Value("-earlydate", theArgs)
            if ('-latedate' in theArgs):
                configObj['LatestDate'] = get_Arg_Value("-latedate", theArgs)
            if ('-sens' in theArgs):
                configObj['SeasonalEnsemble'] = get_Arg_Value("-sens", theArgs)
            if ('-svar' in theArgs):
                configObj['SeasonalVariable'] = get_Arg_Value("-svar", theArgs)
            if ('-baseurl' in theArgs):
                configObj['BaseURL'] = get_Arg_Value("-baseurl", theArgs)
                
            if(debug_VerboseMsg == True):
                printMe("configObj (After Command Line Overrides applied): " + str(configObj))
                
        except:
            printMe("ERROR: Error applying command line overrides to configuration.  See Usage below.")
            printMe("Exiting...")
            printMe("")
            print_UsageCommandLineArgumentList_ToConsole()
            return
        
    except:
        printMe("ERROR: Error reading config file.")
        printMe("Exiting...")
        printMe("")
        return
        
    # Make the request, get the data!
    configList = []
    configList.append(configObj)
    result_Python_Data_List = main_ProcessRequests(configList)
    
    # Print the raw output
    #printMe("Raw Output Object: " + str(result_Python_Data_List))
     
    # Manage the output
    
    # Check Type (Is this a download job or a script job?)
    if (configObj['OperationType'] == 'Download'):
        # Do the download stuff
        try:
            local_FileName = configObj['outfile'] 
            
            theURL = result_Python_Data_List[0]['JobReturnData']['downloadLink'] 
            theJobID = result_Python_Data_List[0]['JobReturnData']['ServerJobID']
            does_DownloadLocalFile_AlreadyExist = os.path.isfile(local_FileName)
            
            # Download the file (and create it)
            download_File(theURL, local_FileName)
            
            printMe("Data for JobID: " + str(theJobID) + " was downloaded to file: " + str(local_FileName))
            
            
            if(does_DownloadLocalFile_AlreadyExist == True):
                printMe("WARNING: -outfile param: " + str(local_FileName) + " already exists.  Download may fail or file may be overwritten.")
                printMe("VERBOSE: If there is an issue with your file, try the download link below.")
                printMe("   Download URL for JobID: " + str(theJobID))
                printMe("     " + str(theURL))
                printMe("Note, download links are only valid for a short time (a few days)")
            printMe("Exiting...")
            return
        except:
            printMe("Failed to download the file, Attempting to write the download URL to the console.")
            try:
                theURL = result_Python_Data_List[0]['JobReturnData']['downloadLink'] 
                theJobID = result_Python_Data_List[0]['JobReturnData']['ServerJobID']
                printMe("Download URL for JobID: " + str(theJobID))
                printMe(str(theURL))
                printMe("Copy and paste this URL into your web browser to manually download the file.  It will be only be available for a few days!")
                printMe("Exiting...")
                return
            except:
                printMe("Could not get download link to write to the console... Exiting...")
                return
            
            
    else:
        try:
            # Do the csv stuff
            printMe("Attempting to write CSV Data to: " + str(configObj['outfile']))
            jobHeaderInfo = ['JobID', result_Python_Data_List[0]['JobReturnData']['ServerJobID']]
            rowHeadings = result_Python_Data_List[0]['JobReturnData']['csvHeaderList']
            singleDataSet = result_Python_Data_List[0]['JobReturnData']['csvWriteReady_DataObj']
            
            myCSVFileName = configObj['outfile'] #"someFile.csv"
            
            theFile = open(myCSVFileName, 'a') #'wb+')
            
            
            #f = csv.writer(open(myCSVFileName, "wb+"))
            f = csv.writer(theFile)
            f.writerow(jobHeaderInfo)
            f.writerow(rowHeadings)  # To overwrite or add rows, the format for 'rowHeadings' is an array of strings
            for row in singleDataSet:
                f.writerow([
                            row[rowHeadings[0]],    #row['Date'],    # row[rowHeadings[0]],  (To get this dynamically...)
                            row[rowHeadings[1]]     #row['value']    # row[rowHeadings[1]]   (To get this dynamically... usually becomes 'avg' or 'min' or 'max')
                            ])

            theFile.close()
            printMe("CSV Data Written to: " + str(myCSVFileName))
            printMe("Exiting...")
            printMe("")
            return
        except:
            printMe("Failed to create the CSV file output.  Attempting to write the CSV data to the console: ")
            try:
                rowHeadings = result_Python_Data_List[0]['JobReturnData']['csvHeaderList']
                singleDataSet = result_Python_Data_List[0]['JobReturnData']['csvWriteReady_DataObj']
                printMe("_CSV_DATA_START")
                printMe("rowHeadings: " + str(rowHeadings))
                printMe("singleDataSet: " + str(singleDataSet))
                printMe("_CSV_DATA_END")
                printMe("Exiting...")
                return
            except:
                printMe("Could not write CSV data to the console...")
                printMe("Exiting...")
                printMe("")
                return
        
    
        


# Entry Point
main_ProcessCommandLine_Request()


# Script Usage:

# You should not make edits to this .py file at all.
# All edits should be done in the config file


# TODO.  Make a better comprehensive documentation file and move the below notes into that file.

# Edit the config file to set your selection parameters.
# Run the script with options that load the config file and specify an output file
# ex: python ClimateSERV_API_Access.py -config /path/to/cserv_config.ini -outfile /path/to/myOutputFile
# If you wish to add a log file output, do so with the param, -logfileout /path/to/myScriptLogFile
# Some configuration options from the config file can be overwritten via command line using various command line options
# ex: using, -dataset eMODIS will set the "DatasetType" option to eMODIS even if the configuration file has a different type in it.
# See the comments in the cserv_config.ini file to see what other parameters can be over written
# This option is particularly useful if you wish to set up a cron job, task scheduler or programatically call script execution with slightly different parameters.
# Also, passing in the pointer to different config files lets you set up multiple configurations.
#
# Inputs:  See the comments in the cserv_config.ini file to view the specific selection parameters that are supported
#  Note: Remember, all string types are case sensitive, so passing 'emodis' instead of 'eMODIS' may not work.
# Outputs
#  For all download jobs, the -outfile parameter is the file that will be downloaded.  So if calling multiple datasets from different command line parameters be sure to have different file names ready for each download data script execution
#  If a downloaded file cannot be created, the server generated link will appear in the console for you to manually download.
#  For all statistics jobs, the -outfile parameter is the csv output file that will be created.  If the file already exists, it will be appended at the bottom.
#  If a csv file cannot be created or written to, the raw arrays that make the csv will be written to the console.






    