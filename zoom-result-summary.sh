FILE=$1

echo "FILE=$FILE"

echo -n "group pairs (ie. half cartesian): "
cat $FILE | wc -l

echo -n "group pairs, where number of collector peers is different: "
cat $FILE | grep -v '"set_diff": true' | wc -l

echo -n "group pairs, where path is same, but other attributes are different: "
cat $FILE | grep -v '"set_diff": true' | grep -v 'path' | wc -l
