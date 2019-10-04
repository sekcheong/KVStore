SCRIPT=`basename ${BASH_SOURCE[0]}`
SERVER_DIR=$(cd `dirname $0` && pwd)
SERVER_PORTS=""
CONF_FILE="${SERVER_DIR}/KVStore.conf"
LOG_DIR="${SERVER_DIR}/logs"
RESTART=0

# Set fonts for Help.
NORM=`tput sgr0`
BOLD=`tput bold`
REV=`tput smso`

# Help function
function HELP {
  echo -e \\n"Help documentation for ${BOLD}${SCRIPT}.${NORM}"\\n
  echo -e "${REV}Basic usage:${NORM} ${BOLD}$SCRIPT -p <comma separated list of ports>${NORM}"\\n
  echo "${REV}-p${NORM}  --Sets the ports on which the servers ${BOLD}p${NORM} on which the servers of KVStore_should be created. This should should be comma separated."
  echo -e "Example: ${BOLD}$SCRIPT -p 8004,8005,8006${NORM}"\\n
  exit 1
}

# Check the number of arguments. If none are passed, print help and exit.
NUMARGS=$#
echo -e \\n"Number of arguments: $NUMARGS"
if [ $NUMARGS -eq 0 ]; then
  HELP
fi

### Start getopts code ###

#Parse command line flags
#If an option should be followed by an argument, it should be followed by a ":".
#Notice there is no ":" after "h". The leading ":" suppresses error messages from
#getopts. This is required to get my unrecognized option code to work.

while getopts :p:rh FLAG; do
  case $FLAG in
    p)
      SERVER_PORTS=$OPTARG
      echo "-s used: $OPTARG"
      ;;
    r)
      RESTART=1
      ;;
    h)  #show help
      HELP
      ;;
    \?) #unrecognized option - show help
      echo -e \\n"Option -${BOLD}$OPTARG${NORM} not allowed."
      HELP
      ;;
  esac
done

shift $((OPTIND-1))  #This tells getopts to move on to the next argument.

# Exit if server ports weren't specified
if [ -z ${SERVER_PORTS} ]
then
  HELP
fi

pushd $SERVER_DIR
IFS=','; read -ra server_ports <<< "$SERVER_PORTS"

if [ $RESTART -eq 0 ]
then
  # Create the configuration file
  if [ -e ${CONF_FILE} ]
  then
    rm -vf ${CONF_FILE}
  fi
  # Assign internal ports used for multicast communication
  MULTICAST_PORT=4447
  INTERNAL_PORT=9001
  for server_port in "${server_ports[@]}"; do
    echo "${server_port},${MULTICAST_PORT},${INTERNAL_PORT}" >> ${CONF_FILE}
    MULTICAST_PORT=$((MULTICAST_PORT+1))
    INTERNAL_PORT=$((INTERNAL_PORT+1))
  done
fi

if [ ! -d "${LOG_DIR}" ]
then
  mkdir ${LOG_DIR}
fi

# Compile the package
mvn compile
# Create the servers
for server_port in "${server_ports[@]}"; do
  LOG_FILE="${LOG_DIR}/KVStore_${server_port}.log"
  echo "Creating server on port ${server_port}, please check it's log at ${LOG_FILE}..."
  mvn exec:java -Dexec.mainClass="com.cs739.kvstore.KeyValueServer" -Dexec.args="${server_port} ${CONF_FILE}" > ${LOG_FILE} 2>&1 &
  sleep 2
done

popd

exit 0
