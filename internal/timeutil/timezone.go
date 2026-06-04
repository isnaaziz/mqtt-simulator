package timeutil

import "time"

var JakartaLoc *time.Location

func init() {
	var err error
	JakartaLoc, err = time.LoadLocation("Asia/Jakarta")
	if err != nil {
		JakartaLoc = time.FixedZone("WIB", 7*60*60)
	}
}
