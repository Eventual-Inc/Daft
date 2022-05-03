package timing

import (
	"time"

	"github.com/sirupsen/logrus"
)

func Timeit(name string, metadata string) func() {
	start := time.Now()
	return func() {
		logrus.Printf("Timeit: %s execution time %s:", name, time.Since(start), metadata)
	}
}
