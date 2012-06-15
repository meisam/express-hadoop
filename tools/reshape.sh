#!/bin/bash

TOOLDIR=$(dirname $0);

while getopts "f:i:" OPTION
do
    case $OPTION in
        f)
            FORMAT=$OPTARG;
			echo "Format File: $FORMAT";
            ;;
        i)
            INPUT=$OPTARG;
            ;;
        ?)
            echo "unknown parameter!"
			exit;
			;;
    esac
done

if [ -z $INPUT ]; then
	INPUT=$(mktemp);
	RINPUT=$INPUT;
	while read DATA; 
	do
		echo $DATA >> $INPUT;
	done
fi

OUTPUTS="";
IFS="$(echo '\t')";
cat $FORMAT| while read LINE;
do
	KEYWORD="";
	COLS="";
	for WORD in $LINE;
	do
		if [ -z "$KEYWORD" ];
		then
			KEYWORD="$WORD";
			echo "$WORD";
		else
			COLS=$COLS"\$${WORD},"
		fi
	done
	COLS=${COLS}end;

	CMD="cat $INPUT|grep $KEYWORD|awk '{print "${COLS}"}'";
	TMP=$(mktemp);
	eval $CMD > $TMP;
	OUTPUTS=$OUTPUTS" $TMP";
done

paste $OUTPUTS;
rm $OUTPUTS;
rm $RINPUT -f;
