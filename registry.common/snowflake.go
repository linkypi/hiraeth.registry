package common

import (
	sf "github.com/bwmarrin/snowflake"
	"time"
)

var snowFlake *sf.Node

func InitSnowFlake(startTime string, machineId int64) (err error) {
	var st time.Time
	if startTime == "" {
		st, err = time.Parse("2006-01-02", "2023-10-27")
	} else {
		st, err = time.Parse("2006-01-02", startTime)
	}
	if err != nil {
		return err
	}

	sf.Epoch = st.UnixNano() / 1000000
	snowFlake, err = sf.NewNode(machineId)
	return nil
}

func GenerateId() int64 {
	return snowFlake.Generate().Int64()
}
