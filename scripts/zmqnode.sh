#!/bin/bash

osx=false
case "`uname`" in
Darwin*) osx=true;;
esac

if $osx; then
    READLINK="stat"    
else
    READLINK="readlink"
fi

#---------------------------------------------
# USAGE and read arguments
#---------------------------------------------

BASE_DIR=`dirname $($READLINK -f $0)`
LIB_HOME=`$READLINK -f ${BASE_DIR}/../lib`
while getopts ":h:" opt;
do  case "$opt" in
    h) HIGH_WATER_MARK=$OPTARG;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
     :)
      echo "Option -$OPTARG requires an argument." >&2
      exit 1
      ;;
    esac
done
shift $(($OPTIND-1))

OPTS=""
if [ "x$HIGH_WATER_MARK" != "x" ] ; then
    OPTS="-h ${HIGH_WATER_MARK}"
fi

CP_SEP=":"

JAVA_LOC=""
if [ "x$JAVA_HOME" != "x" ] ; then
  JAVA_LOC=${JAVA_HOME}"/bin/"
fi

echo "java location is ${JAVA_LOC}"
echo -n "JAVA VERSION="
echo `${JAVA_LOC}java -version`

CLASSPATH=`find $LIB_HOME -name "*.jar" | awk '{p=$0"'$CP_SEP'"p;} END {print p}'`

CMD="${JAVA_LOC}java -Djava.library.path=/usr/local/lib -classpath $CLASSPATH zmqtest.Node $OPTS $1 $2"
echo "RUNNING $CMD"

exec ${CMD}
