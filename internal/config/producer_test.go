package config

import "testing"

func TestProducerDefaultsAndCoupledLimits(t *testing.T) {
	c := Kafka{}
	if err := c.ProducerDefaults(); err != nil || c.ProducerMaxRecords != 16384 || c.ProducerMaxBytes != 64<<20 || c.ProducerWatchdogMS <= c.ProducerDeliveryMS {
		t.Fatal(c, err)
	}
	for _, change := range []func(*Kafka){
		func(c *Kafka) { c.ProducerMaxRecords = -1 },
		func(c *Kafka) { c.ProducerMaxBytes = 32767 },
		func(c *Kafka) { c.ProducerBatchBytes = c.ProducerMaxBytes + 1 },
		func(c *Kafka) { c.ProducerDeliveryMS = 999 },
		func(c *Kafka) { c.ProducerWatchdogMS = c.ProducerDeliveryMS },
		func(c *Kafka) { c.ProducerLingerMS = -1 },
	} {
		bad := c
		change(&bad)
		if bad.ProducerDefaults() == nil {
			t.Fatal("invalid coupled producer settings accepted", bad)
		}
	}
	c = Kafka{ProducerMaxBytes: 32768, ProducerDeliveryMS: 20000}
	if err := c.ProducerDefaults(); err != nil || c.ProducerBatchBytes > c.ProducerMaxBytes || c.ProducerWatchdogMS != 22000 {
		t.Fatal("dependent defaults not derived", c, err)
	}
}
