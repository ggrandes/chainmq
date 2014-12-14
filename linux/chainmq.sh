#!/bin/bash
JAVA_HOME=${JAVA_HOME:-/opt/java/java-current}
CHAINMQ_HOME=${CHAINMQ_HOME:-/opt/chainmq}
CHAINMQ_ADDR=${CHAINMQ_ADDR:-0.0.0.0}
CHAINMQ_PORT=${CHAINMQ_PORT:-11300}
CHAINMQ_LOG_TYPE=${CHAINMQ_LOG_TYPE:-DAY} # DAY or SIZE
CHAINMQ_CLASSPATH=$(echo $CHAINMQ_HOME/lib/*.jar | tr ' ' ':')
#
do_start () {
  cd ${CHAINMQ_HOME}
  nohup ${JAVA_HOME}/bin/java -Dprogram.name=chainmq -Xmx512m \
    -Dchainmq.home=${CHAINMQ_HOME} -Dchainmq.log.type=${CHAINMQ_LOG_TYPE} \
    -cp "${CHAINMQ_HOME}/conf/:${CHAINMQ_CLASSPATH}" \
    org.javastack.chainmq.Server -l $CHAINMQ_ADDR -p $CHAINMQ_PORT 1>${CHAINMQ_HOME}/log/chainmq.bootstrap 2>&1 &
  PID="$!"
  echo "ChainMQ: STARTED [${PID}]"
}
do_stop () {
  PID="$(ps axwww | grep "program.name=chainmq" | grep -v grep | while read _pid _r; do echo ${_pid}; done)"
  if [ "${PID}" = "" ]; then
    echo "ChainMQ: NOT RUNNING"
  else
    echo -n "ChainMQ: KILLING [${PID}]"
    kill -TERM ${PID}
    echo -n "["
    while [ -f "/proc/${PID}/status" ]; do
      echo -n "."
      sleep 1
    done
    echo "]"
  fi
}
do_status () {
  PID="$(ps axwww | grep "program.name=chainmq" | grep -v grep | while read _pid _r; do echo ${_pid}; done)"
  if [ "${PID}" = "" ]; then
    echo "ChainMQ: NOT RUNNING"
  else
    echo "ChainMQ: RUNNING [${PID}]"
  fi
}
case "$1" in
  start)
    do_stop
    do_start
  ;;
  stop)
    do_stop
  ;;
  restart)
    do_stop
    do_start
  ;;
  status)
    do_status
  ;;
  *)
    echo "$0 <start|stop|restart|status>"
  ;;
esac
