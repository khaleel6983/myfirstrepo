. /home/gopalkrishna/userstory/retail_userstory_all_scripts/parameter

if [ -f $Inputfile ]
then

	echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
	echo "             Count Validation of Lines in Input file            "
	echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
	localCount=$(cat $Inputfile | wc -l)
	echo "COUNT Before copying to HDFS = "$localCount 
	echo ""
	hdfs dfs -test -d /${Hdfsdir}
		if [ $? -eq 0 ]
	then
			echo "Directory already present, DELETING & COPYING Updated Inputfile From LFS to HDFS"
			echo ""
			hdfs dfs -rm -r /${Hdfsdir}/*
			hdfs dfs -put  $Inputfile  /${Hdfsdir}/
			hdfsCount=$(hdfs dfs -cat /${Hdfsdir}/$Inputfile|wc -l)
			echo ""
			echo "COUNT AFTER copied to HDFS = "$hdfsCount 
			echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
			if [$($localCount) -eq $($hdfsCount)]
			then
					echo ""
					echo " Input file transfer VALIDATED CORRECTLY. "
					echo ""
				else
					echo ""
					echo " Input file transfer NOT VALIDATED "
					echo ""
			fi
	else
			echo "Creating the Directory and copying file From LFS to HDFS"
			hdfs dfs -mkdir /${Hdfsdir}	
			hdfs dfs -put  $Inputfile  /${Hdfsdir}
			hdfsCount=$(hdfs dfs -cat /${Hdfsdir}/$Inputfile|wc -l)
			echo ""
			echo "COUNT AFTER copied to HDFS = "$hdfsCount 
			echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
			if [$($localCount) -eq $($hdfsCount)]
				then
					echo ""
					echo " Input file transfer VALIDATED CORRECTLY. "
					echo ""
				else
					echo ""
					echo " Input file transfer NOT VALIDATED "
					echo ""
			fi
	fi
else
echo "++++++++++++++++++++++++++++++++++++++++++++++++++"
echo "Input file is not there in given location"
echo "++++++++++++++++++++++++++++++++++++++++++++++++++"
fi	
