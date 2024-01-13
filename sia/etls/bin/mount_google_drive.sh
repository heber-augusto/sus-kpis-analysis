add-apt-repository --yes ppa:alessandro-strada/ppa
apt-get update
apt-get install google-drive-ocamlfuse

mkdir "${1}/datalake"
mkdir "${1}/gdfuse"

google-drive-ocamlfuse -xdgbd -label monitor -serviceaccountpath "${1}/monitor-rosa-leitura.json" -serviceaccountuser "${2}"

python ${1}/sus-kpis-analysis/sia/etls/lib/gdfuse_config.py -c "${1}/gdfuse/monitor/config" -t "${3}"

google-drive-ocamlfuse -cc -label monitor ${1}/datalake
