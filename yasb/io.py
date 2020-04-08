"""
module containg io functions to deal with yasb native data as well as other data sources
"""


# deprecated file format format for Data coming from Boxes with old firmware -> depends on number of columns
columns = [
    "time",
    "latitude",
    "longitude",
    "elevation",
    "rot_x",
    "rot_y",
    "rot_z",
    "acc_x",
    "acc_y",
    "acc_z",
    "mag_x",
    "mag_y",
    "mag_z",
    "roll",
    "pitch",
    "yaw",
]

columns2 = [
    "time",
    "runtime",
    "gpstime",
    "latitude",
    "longitude",
    "elevation",
    "rot_x",
    "rot_y",
    "rot_z",
    "acc_x",
    "acc_y",
    "acc_z",
    "mag_x",
    "mag_y",
    "mag_z",
    "roll",
    "pitch",
    "yaw",
]

### Data aggregation and cleaning

def readLogFile(
    logFilePath,
    columns=columns,
    skipheader=3,
    verbose=False,
    lowMemory=True,
    errorOnBadLine=False,
    engine="python",
):
    """
    readLogFile(logFilePath, columns=columns, skipheader=2, skipfooter=1):

    opens the given path, tries to read in the data, convert it to a dataframe
    and append it.

    returns a dataframe containing the data from a given csv file
    """

    if verbose: print("processing file: {}".format(logFilePath))

    if not isfile(logFilePath):
        print("no such file: {} -> skipping".format(logFile))
        return None

    try:
        tempDataFrame = pd.read_csv(
            logFilePath,
            skiprows=skipheader,
            names=columns,
            low_memory=lowMemory,
            error_bad_lines=errorOnBadLine,
            skipfooter=1,
            engine=engine,
            )
        if verbose: print(tempDataFrame.info())

    except:
        print("could not process file: {}, skipping".format(logFilePath))
        return None

    return tempDataFrame

def cleanDataFrame(
    df,
    roundTimeStamp=False,
    toDateTime=True,
    dateTimeIndex = True,
    replaceNan=True,
    verbose=False,
    correctTimeByGPS=True,
    timeZone="Europe/Berlin",
    dropDuplicateIndices=True,
):

    if df.empty:
        print("empty dataframe, skipping!")
        return pd.DataFrame()

    # convert relevant columns to strings

    if replaceNan:
        if verbose: print("cleaning NaNs")
        df.fillna(method="ffill", inplace=True)

    if roundTimeStamp:
        if verbose: print("rounding time")
        df["time"].round(roundTimeStamp)

    if toDateTime:
        if verbose: print("converting timestamps")
        df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)

    if dateTimeIndex:
        if verbose: print("converting timestamps to index")
        df.set_index("time", inplace=True)

    if correctTimeByGPS:
        if verbose: print("correcting time stamp via GPS")
        if len(df.columns) == 17: # only log file version to is egligible to gps time correction
            if not GPSDateTimeCorrection(df, verbose=False):
                return pd.DataFrame()

    if timeZone:
        try:
            if verbose: print("converting time zone to: {}".format(timeZone))
            df.index = df.index.tz_convert(timeZone)
        except:
            print("could not convert time zone to {}".format(timeZone))

    if dropDuplicateIndices:
        if verbose: print("dropping duplicate indices")
        df = df.loc[~df.index.duplicated(keep='first')]   

    return df

def processDataFile(dataFile, cols=columns2, verbose=False):
    
    tempData = pd.DataFrame()
    
    if not isfile(dataFile):
        print("not a file: {}, skipping".format(dataFile))
        return pd.DataFrame()
    
    tempData = readLogFile(dataFile, verbose=verbose, columns=cols)
    
    if tempData.empty:
        print("skipping corrupt file: {}".format(dataFile))
        return pd.DataFrame()
    
    tempData = cleanDataFrame(tempData, verbose=verbose)        # clean it -> generate index, etc.

    if not tempData.empty:                     # append the dataframes to the global dataframe
        return tempData
    
def processDataSet_parallel(dataSet, pickleName=None, pattern = "log_0???.txt", nProcs = 32, verbose=False, substractMean=True):

    if not isdir(dataSet):
        print("*! not a directory, skipping")
        return pd.DataFrame()
    
    if verbose: print("* processing: {}".format(dataSet))
   
    cols = checkLogFileVersion(dataSet, [columns, columns2])

    if verbose: print("* file version checked: {}".format(cols))
    
    pool = Pool(nProcs)
    frames = list()
   
    if verbose: print("* iterating over files")
    for dfile in sorted(glob(path.join(dataSet, pattern))):
          frameData = pool.apply_async(processDataFile,(dfile, cols, verbose))
          frames.append(frameData)

    pool.close()
    pool.join()
    
    if not len(frames) > 0:
        print("*! no files found")
        return pd.DataFrame()

    data = pd.concat([d.get() for d in frames])
  
    if substractMean:
        if verbose: print("* substracting mean")
        for comp in ("acc_x", "acc_y", "acc_z"):
            try:
                data[comp] -= np.mean(data[comp])
            except:
                print("*! could not calculate mean, data cleaning needed!")
                continue

    if pickleName:
        if verbose: print("* exporting pickle {}".format(pickleName))
        try:
            data.to_pickle(path.join(dataSet, "{}".format(pickleName)))
        except:
            print("*! failed to export pickle!")
    
    return data


def correctTime(df, runTime, gpsTimeStamp, verbose=False):

    powerOnTimeUnix = gpsTimeStamp - runTime
    powerOnTime = pd.to_datetime(powerOnTimeUnix, unit="s", utc=True)

    if verbose: print("power on time: {}".format(powerOnTime))

    correctedTime = (df.index - df.index[0]) + powerOnTime

    if verbose: print("corrected power on time series: {}".format(correctedTime))
    if verbose: print("inserting as new index.. ")

    df.reset_index()
    df.insert(loc=0, column="truetime", value=correctedTime)
    df.set_index("truetime", inplace=True)

    if verbose: print(df.head())

    if verbose: print("done")

def GPSDateTimeCorrection(df, verbose=False):

    """
    this function extracts the last valid time stamp and the corresponding run time of the box
    and corrects the time index of the given data frame
    """

    try:

        """
        this method has a know edge case: if the last available time stamp has a time lock, 
        but no date lock, the time stamp might look something like this: 
        
        2000-00-00-12-13-14

        which fails later in the programm when trying to generate a valid datetime object from
        the time stamp (line 482). This is currently caught via an exception, however, this is far from ideal.
        As there is currently no easy fix, the whole concept should be re-evaluated
        """

        lastUniqueGPSTimeStamp = pd.unique(
                df.loc[(df.gpstime != "0000-00-00-00-00-00") & 
                       (df.gpstime != "2000-00-00-00-00-00")
                      ].gpstime)[-1]
    except:
        print("no GPS time stamp available, skipping")
        return False

    runTime = df.loc[df.gpstime == lastUniqueGPSTimeStamp].runtime[0] / 1000.0  # convert to seconds!
    runTimeZero = df.runtime[0]/1000.0

    deltaRunTime = runTime - runTimeZero

    gpsTime = df.loc[df.gpstime == lastUniqueGPSTimeStamp].gpstime[0]
    if verbose: print("found time stamp: {} runtime: {}, run time since beginning: {}".format(gpsTime, runTime, (runTime - runTimeZero)))
    date = gpsTime.split("-")[:3]
    time = gpsTime.split("-")[3:]
    try:
        gpsDateTime = pd.to_datetime("{} {}".format("-".join(date), ":".join(time)), utc=True).value / 10**9
    except Exception as e:
        print("failed to generate gpsDateTime for {} : {}".format(date, time))
        print("skipping dataframe")
        return False
    if verbose: print("correcting time")
    correctTime(df, runTime=deltaRunTime, gpsTimeStamp=gpsDateTime)
    return True


def checkLogFileVersion(logFileDir, cols, verbose=False):
    """

    checks the row length of log_0000.txt in a given directory to parse the log file version
    Two log file versions are available:
    - Version 1: normal log file format
    - Version 2: log including GPS timestamp

    return: the correct columns to use

    """

    # find a suitable log file
    logFilePath = glob(path.join(logFileDir, "log_????.txt"))[0]

    if path.isfile(logFilePath):
        with open(logFilePath) as logFile:
            for i, line in enumerate(logFile):
                if i == 3:  # first line = header, second line = overflow from last file -> hence third line used to check for file version
                    if len(line.split(",")) == 18:
                        if verbose: print("file version 2")
                        return cols[1]
                    elif len(line.split(",")) == 16:
                        if verbose: print("file version 1")
                        return cols[0]
                    else:
                        print("wrong number of columns in file {}".format(logFilePath))
                    break
    else:
        raise Exception("no such file or directory: {}".format(logFilePath))
