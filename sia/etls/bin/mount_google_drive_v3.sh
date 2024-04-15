mkdir "${1}/${4}"
mkdir "${1}/gdfuse"

fusermount -u "${1}/${4}"

google-drive-ocamlfuse -xdgbd -label "${4}" -serviceaccountpath "${5}" -serviceaccountuser "${2}"

python ${6}/sia/etls/lib/gdfuse_config.py -c "${1}/gdfuse/${4}/config" -t "${3}"

google-drive-ocamlfuse -cc -label ${4} ${1}/${4}
