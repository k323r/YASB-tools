"""
module containing serveral numerical methods for 
working with yasb data:

- filters
- integration
- local mean/max identification
- conversion of coordinate systems
- calculation of directions 

"""

import pandas as pd

def _butter_bandpass(lowcut, highcut, fs, order=5):
    """
    generates a butter bandpass filter object
    """
    nyq = 0.5 * fs
    low = lowcut / nyq
    high = highcut / nyq
    b, a = butter(order, [low, high], btype='band')
    return b, a


def butter_bandpass_filter(data, lowcut, highcut, fs, order=5):
    """
    appliess a butter bandpass filter to the given dataDir
    """
    b, a = _butter_bandpass(lowcut, highcut, fs, order=order)
    y = lfilter(b, a, data)
    return y

def integrateVelocityAcceleration(df,
                                  verbose=False,
                                  resampleInterval="30ms",
                                  filterLowCut=0.1,
                                  filterHighCut=1,
                                  filterFrequency=33.333,
                                  filterOrder=3,
                                  calculateDeflection=True,
                                  components = ("x", "y", "z"),
                                  applyG=True,
                                 ):
    g = 9.80665
    
    data = pd.DataFrame()

    """
    1. resample
    2. filter
    3. integrate

    if applyG:
        if verbose: print("> applying g")
        for comp in components:
            df["acc_{}".format(comp)] = df["acc_{}".format(comp)]*g
    
    # add raw components to data frame
    for comp in components:
        data.insert(column="acc_{}".format(comp),
                    value=df["acc_{}".format(comp)],
                    loc=len(data.columns)
                   )
    """
    # resample data
    if verbose: print("*    resampling data to {}. Start time: {}".format(resampleInterval, df.index[0]))
    # resample data and multiply with g!
    for comp in components:
        data.insert(column="acc_{}r".format(comp),
                    value=df["acc_{}".format(comp)].resample(resampleInterval).bfill()*g,
                    loc=len(data.columns)
                   )

    # time ins seconds, resampled -> used for integration
    t = data.index.astype(np.int64)/10**9

     
    if verbose: print("*    applying filter with order = {} frequency = {} lowcut = {} highcut = {}".format(filterOrder,
                                                                                                           filterFrequency,
                                                                                                           filterLowCut,
                                                                                                           filterHighCut,
                                                                                                          ))
    for comp in components:
        data.insert(column="acc_{}rf".format(comp),
                    value=butter_bandpass_filter(data["acc_{}r".format(comp)],
                                                 filterLowCut,
                                                 filterHighCut,
                                                 filterFrequency,
                                                 order=filterOrder),
                    loc=len(data.columns)
                   )


    if verbose: print("*    integrating acceleration")
    for comp in components:
        if verbose: print("*        acceleration {}".format(comp.upper()))
        # integrate filtered acceleration
        data.insert(column="vel_{}".format(comp),
                    value=integrate.cumtrapz(data["acc_{}rf".format(comp)], t, initial=0),
                    loc=len(data.columns)
                   )
    if verbose: print("*    integrating velocity")
    for comp in components:
        if verbose: print("*        velocity {}".format(comp.upper()))
        # integrate velocity to yield position
        data.insert(column="pos_{}".format(comp),
                    value=integrate.cumtrapz(data["vel_{}".format(comp)], t, initial=0),
                    loc=len(data.columns)
                   )
        
    if calculateDeflection:
        if verbose: print("*    calculating deflection")
        data.insert(column = "deflection",
                    value = np.sqrt(np.power(data.pos_z, 2) + np.power(data.pos_x, 2)),
                    loc = len(data.columns),
                   )

    return data

def applyIntegration_parallel(dataset, 
                              verbose=False,
                              nProcs=32,
                              integrationInterval="10min",
                              resampleInterval="30ms",
                              filterLowCut=0.1,
                              filterHighCut=1,
                              filterFrequency=30,
                              filterOrder=3,
                              calculateDeflection=True, 
                              components = ("x", "y", "z"),
                              applyG=True,
                             ):

    # create a pool of workers
    pool = Pool(nProcs)
    frames = list()

    if verbose: print("* integration interval set to {}. Starting integration with {} threads".format(integrationInterval, nProcs))
    ## iterate over the sample intervalls and enable parallel integration
    for t, dataSample in dataset.resample(integrationInterval):
        if verbose: print("* integration start: {}".format(t))
        
        frames.append(
            pool.apply_async(
                integrateVelocityAcceleration, (dataSample,
                                                verbose,
                                                resampleInterval,
                                                filterLowCut,
                                                filterHighCut,
                                                filterFrequency,
                                                filterOrder,
                                                calculateDeflection,
                                                components
                                                )))

    pool.close()
    pool.join()

    frames = pd.concat([d.get() for d in frames])
    
    return frames

def applyIntegration(dataset, 
                     verbose=False,
                     integrationInterval="10min",
                     resampleInterval="30ms",
                     filterLowCut=0.1,
                     filterHighCut=1,
                     filterFrequency=30,
                     filterOrder=3,
                     calculateDeflection=True, components = ("x", "y", "z"),
                     applyG=True,
                    ):
   
    frames = list()

    if verbose: print("* integration interval set to {}".format(integrationInterval))
    ## iterate over the sample intervalls and enable parallel integration
    for t, dataSample in dataset.resample(integrationInterval):
        if verbose: print("* integration start: {}".format(t))
        
        frames.append(integrateVelocityAcceleration(dataSample,
                                                    verbose,
                                                    resampleInterval,
                                                    filterLowCut,
                                                    filterHighCut,
                                                    filterFrequency,
                                                    filterOrder,
                                                    calculateDeflection,
                                                    components
                                                   ))

    frames = pd.concat(frames)
    
    return frames



