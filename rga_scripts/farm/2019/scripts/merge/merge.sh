echo "listing the files";
#ls -l

if [ $1 == 1 ]
then
	echo "Running hipo-utils"
	hipo-utils -filter -b "RUN::config,RUN::rf,RUN::scaler,RAW::epics,RAW::scaler,HEL::flip,REC*"  -merge -o output.hipo skim_*.hipo
	echo "Finished running hipo-utils -merge"
elif [ $1 == 2 ]
then
	echo "Running hipo-utils FTOF Skim"
        hipo-utils -filter -b "RUN::config,RUN::rf,RUN::scaler,RAW::epics,RAW::scaler,HEL::flip,TimeBasedTrkg::TBTracks,REC*,FTOF*"  -merge -o output1.hipo skim_*.hipo
        echo "Finished running hipo-utils -merge"
	echo "Running hipo-utils HTCC Skim"
        hipo-utils -filter -b "RUN::config,RUN::rf,RUN::scaler,RAW::epics,RAW::scaler,HEL::flip,REC*,HTCC*"  -merge -o output2.hipo skim_*.hipo
        echo "Finished running hipo-utils -merge"
elif [ $1 == 3 ] 
then
	echo "Running hipo-utils CTOF Skim"
        hipo-utils -filter -b "RUN::config,RUN::rf,RUN::scaler,RAW::epics,RAW::scaler,HEL::flip,REC*,CTOF*,CVTRec::Tracks"  -merge -o output1.hipo skim_*.hipo
        echo "Finished running hipo-utils -merge"
	echo "Running hipo-utils CND Skim"
        hipo-utils -filter -b "RUN::config,RUN::rf,RUN::scaler,RAW::epics,RAW::scaler,HEL::flip,REC*,CND*,CVTRec::Tracks"  -merge -o output2.hipo skim_*.hipo
        echo "Finished running hipo-utils -merge"
elif [ $1 == 4 ]
then
        echo "Running hipo-utils FT Skim"
        hipo-utils -filter -b "RUN::config,RUN::rf,RUN::scaler,RAW::epics,RAW::scaler,HEL::flip,REC*,FTCAL*,FTHODO*"  -merge -o output.hipo skim_*.hipo
        echo "Finished running hipo-utils -merge"
fi
