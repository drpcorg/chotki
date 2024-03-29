package chotki

/*
func ShowObject(chotki *chotki.Chotki, id chotki.ID) error {
	i := chotki.ObjectIterator(id)
	for i.Valid() {
		id, rdt := chotki.OKeyIdRdt(i.Key())
		_, _ = fmt.Fprintf(os.Stderr, "%c#%d\t\n", rdt, id.Off())
	}
	return nil
}

var ErrBadObjectJson = errors.New("bad JSON object serialization")
var ErrUnsupportedType = errors.New("unsupported field type")


func CreateObjectFromList(chotki *chotki.Chotki, list []interface{}) (id chotki.ID, err error) {
	packet := toyqueue.Records{}
	// todo ref type json
	// todo add id, ref
	for _, f := range list {
		var rdt byte
		var body []byte
		switch f.(type) {
		case int64:
			rdt = 'C'
			body = chotki.CState(f.(int64))
		case float64:
			rdt = 'N'
		case string:
			str := f.(string)
			id := chotki.ParseBracketedID([]byte(str))
			if id != chotki.BadId { // check for id-ness
				rdt = 'L'
				body = chotki.LState(id, 0)
			} else {
				rdt = 'S'
				body = chotki.Stlv(str)
			}
		default:
			err = ErrUnsupportedType
			return
		}
		packet = append(packet, toytlv.Record(rdt, body))
	}
	return chotki.CommitPacket('O', chotki.ID0, packet)
}

// ["{10-4f8-0}", +1, "string", 1.0, ...]
func CreateObject(chotki *chotki.Chotki, jsn []byte) (id chotki.ID, err error) {
	var parsed interface{}
	err = json.Unmarshal(jsn, &parsed)
	if err != nil {
		return
	}
	switch parsed.(type) {
	case []interface{}:
		id, err = CreateObjectFromList(chotki, parsed.([]interface{}))
	default:
		err = ErrBadObjectJson
	}
	return
}
*/
