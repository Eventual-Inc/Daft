from daft.dataframe import dataframe

from daft.io.stream import stream

def test_video():

    # ImageSource <- VideoSource <- StreamSource
    source = stream("/Users/rch/Desktop/zoo.mp4").as_video().to_frames(fps=24)

    df = dataframe(source)



