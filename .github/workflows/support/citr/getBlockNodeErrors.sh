NAMESPACE=$1
BN_LOG=/opt/hiero/block-node/logs
TOOLDIR=`dirname $0`

mkdir BNlog_${NAMESPACE}

for i in `sh ${TOOLDIR}/kubectlt -n ${NAMESPACE} get pods | grep 'block-node-' | awk '{print $1}'`
do
  sh ${TOOLDIR}/kubectlt -n ${NAMESPACE} exec -it ${i} -- \
  bash -c "grep -h -i -E 'error|exception|warn' $BN_LOG/*.log" > BNlog_${NAMESPACE}/${i}-errors.log
done

cat BNlog_${NAMESPACE}/*-errors.log | grep -v -E 'exception[\=]null' |\
perl -ne 'if (/^\d{4}[\-]\d{2}[\-]\d{2}\s+\d{2}[\:]\d{2}[\:]\d{2}[\.]\d+\s+\d+\s+(.*)$/) {print "$1\n";} else {print;}' | perl -pne '~s/\d+/N/g' | sort | uniq -c | sort -n -k 1 -r |\
grep -v 'Report[\[]' | grep -v 'contracts.evm.chargeGasOnEvmHandleException [\=] true' > BNlog_${NAMESPACE}/error_summary.txt


for i in `sh ${TOOLDIR}/kubectlt -n ${NAMESPACE} get pods | grep 'block-node-' | awk '{print $1}'`
do
  mkdir BNlog_${NAMESPACE}/${i}_logs
  sh ${TOOLDIR}/kubectlt -n ${NAMESPACE} cp ${i}:$BN_LOG BNlog_${NAMESPACE}/${i}_logs
done
