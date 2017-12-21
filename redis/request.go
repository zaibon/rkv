package redis

type request struct {
	args [][]byte
}

func (r *request) Command() string {
	return string(r.args[0])
}
