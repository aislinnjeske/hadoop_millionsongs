~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
            HW-3 README
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


The cs455.hadoop package contains the following files and packages:

MillionSongJob.java                 - Hadoop Job 
MillionSongMapperAnalysis.java      - Hadoop Mapper that reads and maps analysis files
MillionSongMapperMetadata.java      - Hadoop Mapper that reads and maps metadata files
MillionSongReducer.java             - Hadoop Reducer
Artist.java                         - Contains information about an artist (artist name, artist id, attribute totals, etc)
Song.java                           - Contains information about a song (song name, song id, duration, tempo, etc)


cs455.hadoop.Utilities

AttributePredictor.java             - Contains all information for Q9 and calculates correlation coefficients for all attributes for prediction
Coefficient.java                    - Contains information for about a single attribute and calculates the correlation coefficient for that attribute
DurationComparer.java               - Compares the durations of songs to find the longest, shortest, and median
GenericAverages.java                - Contains information about average tempos, duration, and hotness to find the most generic and unique artists
ReadAttributes.java                 - Parses data from the mapper for both the analysis and metadata files
SegmentData.java                    - Contains information about all the segment data and finds the average for each segment
Util.java                           - Contains util methods including string to double and string to integer methods


cs455.hadoop.Map

WorldMap.java                       - Sorts songs into the correct region of the world based on their longitude and latitude
Region.java                         - Represents a region of the world and contains information about that region (average hotness, hotest song, most frequent terms)


To start Hadoop and Yarn: $HADOOP_HOME/sbin/start-all.sh

Point conf directory to shared dataset: export HADOOP_CONF_DIR=<path-to-config>

Start the job: $HADOOP_HOME/bin/hadoop jar build/libs/HW-3.jar cs455.hadoop.MillionSongJob /data/analysis/*.csv /data/metadata/*.csv <output-file-directory>
