package event

func BuildPartitionKey(dbName, dataType string, key []byte) []byte {
	if len(key) == 0 {
		out := make([]byte, 0, len(dbName)+1+len(dataType))
		out = append(out, dbName...)
		out = append(out, ':')
		out = append(out, dataType...)
		return out
	}
	out := make([]byte, 0, len(dbName)+1+len(dataType)+1+len(key))
	out = append(out, dbName...)
	out = append(out, ':')
	out = append(out, dataType...)
	out = append(out, ':')
	out = append(out, key...)
	return out
}
