

def fftTimeSeries(data, newFigure=True, label=None):
    """
    performs a fft on the given data and plots it.

    returns peak frequency
    """

    if newFigure:
        plt.figure()
    deltaT = data.index.to_series().diff()
    deltaTMean = np.mean(deltaT) / np.timedelta64(1, 's')
    print(np.mean(deltaT) / np.timedelta64(1, 's'))
    FFT = scipy.fftpack.fft(data)
    PSD = np.abs(FFT) ** 2
    Frequency = scipy.fftpack.fftfreq(len(data), deltaTMean)
    Frequency_i = Frequency > 0

    if label:
        plt.plot(Frequency[Frequency_i], PSD[Frequency_i], label=label)
    else:
        plt.plot(Frequency[Frequency_i], PSD[Frequency_i])

    plt.xlabel("Frequency"); plt.ylabel("Power Spectrum Density")
    return Frequency[np.argmax(PSD)]


